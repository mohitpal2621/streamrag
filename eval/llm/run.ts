// Scores the two jobs streamrag gives a model, with promptfoo: answering with citations, and reading
// breaking changes out of release notes. The inputs are fixed (OSV advisories in a throwaway Postgres,
// changelogs at release tags), so two runs differ only by model.
import { readFile } from 'node:fs/promises';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { connectors } from '../../src/connectors/index.ts';
import { get } from '../../src/connectors/http.ts';
import { ask } from '../../src/core/answer.ts';
import { embedPending } from '../../src/core/embed.ts';
import { extractBreaking, type Breaking } from '../../src/core/extract.ts';
import { ingest } from '../../src/core/ingest.ts';
import { embed, rerank } from '../../src/core/models.ts';
import { connect, migrate } from '../../src/core/store.ts';
import { createGateway } from '../../src/llm/gateway.ts';
import { providersFromEnv } from '../../src/llm/providers.ts';

process.env.PROMPTFOO_DISABLE_TELEMETRY = '1';
process.env.PROMPTFOO_DISABLE_UPDATE = '1';
const { default: promptfoo } = await import('promptfoo');

type Context = { vars: Record<string, string>; providerResponse?: { metadata?: Record<string, unknown> } };
type Grade = { pass: boolean; score: number; reason: string };
const grade = (pass: boolean, reason: string, score = pass ? 1 : 0): Grade => ({ pass, score, reason });

// EVAL_MODELS lists Ollama models to compare; "gemini" and "groq" work too once their keys are set.
const models = (process.env.EVAL_MODELS ?? process.env.OLLAMA_MODEL ?? 'llama3.2:3b').split(',').map((m) => m.trim());
function gatewayFor(model: string) {
  const cloud = model === 'gemini' || model === 'groq';
  const providers = providersFromEnv({ ...process.env, LLM_PROVIDERS: cloud ? model : 'ollama', ...(cloud ? {} : { OLLAMA_MODEL: model }) });
  if (!providers.length) throw new Error(`${model} needs its API key in .env`);
  return { label: `${providers[0].name}:${providers[0].model}`, gateway: createGateway({ providers }) };
}

// A sample of the retrieval eval's questions, plus two that no npm advisory can answer.
const set: { packages: string[]; queries: { q: string; expect: string[] }[] } = JSON.parse(
  await readFile(new URL('../queries.json', import.meta.url), 'utf8'),
);
const answerable = [1, 5, 13, 17, 23, 32, 36, 45].map((i) => set.queries[i]);
const unanswerable = ['How do I turn on dark mode in Tailwind CSS?', 'What is new in PostgreSQL 18?'];

// Changelog sections at release tags. Each pattern stands for one breaking change the model should
// report, and `need` is how many of them count as a pass; no patterns means nothing breaks.
const releases = [
  { name: 'express 5.0.0', url: 'expressjs/express/v5.0.0/History.md', from: '5.0.0 / ', to: '\n5.0.0-beta', expect: ['res\\.status|status code', '\\bback\\b'], need: 2 },
  {
    name: 'eslint 9.0.0',
    url: 'eslint/eslint/v9.0.0/CHANGELOG.md',
    from: 'v9.0.0 - ',
    to: '\nv9.0.0-rc',
    expect: ['flat config', 'node\\.?js', 'formatter', 'valid-jsdoc|require-jsdoc', 'caughterrors', 'flateslint|flatruletester'],
    need: 4,
  },
  { name: 'express 5.0.1', url: 'expressjs/express/v5.0.1/History.md', from: '5.0.1 / ', to: '\n5.0.0 / ', expect: [], need: 0 },
  { name: 'express 4.21.1', url: 'expressjs/express/4.21.1/History.md', from: '4.21.1 / ', to: '\n4.21.0 / ', expect: [], need: 0 },
];

const container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
const db = connect(container.getConnectionUri());
try {
  await migrate(db);
  const synced = await ingest(db, set.packages.map((p) => ({ connector: 'osv', feed: `npm/${p}` })), connectors, 200);
  if (synced.some((r) => 'error' in r)) throw new Error('could not sync the advisories');
  console.log(`corpus: ${await embedPending(db, embed)} advisories`);

  const notes = new Map<string, string>();
  for (const r of releases) {
    const text = await (await get(`https://raw.githubusercontent.com/${r.url}`)).text();
    const start = text.indexOf(r.from);
    if (start < 0) throw new Error(`${r.name}: section not found`);
    notes.set(r.name, text.slice(start, text.indexOf(r.to, start + 1)));
  }

  const answerer = (model: string) => {
    const { label, gateway } = gatewayFor(model);
    return {
      id: () => label,
      label,
      callApi: async (question: string) => {
        await db.query('truncate answers'); // every model answers for itself, never from another's cache
        try {
          let sources: { url: string | null }[] = [];
          for await (const ev of ask(db, question, { gateway, embed, rerank })) {
            if (ev.type === 'sources') sources = ev.sources;
            if (ev.type === 'done') return { output: ev.answer, metadata: { cited: ev.cited.map((n) => sources[n - 1].url), invalid: ev.invalid } };
          }
          return { error: 'no answer' };
        } catch (err) {
          return { error: String(err) };
        }
      },
    };
  };
  const extractor = (model: string) => {
    const { label, gateway } = gatewayFor(model);
    return {
      id: () => label,
      label,
      callApi: async (name: string) => {
        try {
          const found = await extractBreaking(gateway, name, notes.get(name) ?? '');
          return { output: JSON.stringify(found, null, 2), metadata: { found } };
        } catch (err) {
          return { error: String(err) };
        }
      },
    };
  };

  const cites = (c: Context) => (c.providerResponse?.metadata?.cited ?? []) as (string | null)[];
  const noneInvented = (_: string, c: Context) => {
    const invalid = (c.providerResponse?.metadata?.invalid ?? []) as number[];
    return grade(!invalid.length, invalid.length ? `cites sources it was never given: ${invalid.join(', ')}` : 'every citation is to a real source');
  };
  const answers = await promptfoo.evaluate(
    {
      description: 'streamrag: answers with citations',
      prompts: ['{{question}}'],
      providers: models.map(answerer),
      tests: [
        ...answerable.map(({ q, expect }) => ({
          description: q,
          vars: { question: q },
          assert: [
            {
              type: 'javascript',
              value: (_: string, c: Context) => {
                const hit = cites(c).some((u) => expect.some((id) => u?.endsWith(`/${id}`)));
                return grade(hit, hit ? 'cites the advisory that answers it' : `cites ${cites(c).join(', ') || 'nothing'}`);
              },
            },
            { type: 'javascript', value: noneInvented },
          ],
        })),
        ...unanswerable.map((q) => ({
          description: q,
          vars: { question: q },
          assert: [
            { type: 'javascript', value: (_: string, c: Context) => grade(!cites(c).length, cites(c).length ? `cites ${cites(c).join(', ')} anyway` : 'says the sources do not cover it') },
            { type: 'javascript', value: noneInvented },
          ],
        })),
      ],
      writeLatestResults: true,
    },
    { maxConcurrency: 1, showProgressBar: Boolean(process.stdout.isTTY) },
  );

  const extraction = await promptfoo.evaluate(
    {
      description: 'streamrag: breaking changes from release notes',
      prompts: ['{{release}}'],
      providers: models.map(extractor),
      tests: releases.map((r) => ({
        description: r.name,
        vars: { release: r.name },
        assert: [
          {
            type: 'javascript',
            value: (_: string, c: Context) => {
              const found = (c.providerResponse?.metadata?.found ?? []) as Breaking[];
              if (!r.expect.length) return grade(!found.length, found.length ? `lists ${found.length} breaking changes for a release with none` : 'lists none');
              const text = found.map((b) => `${b.change} ${b.migration}`).join('\n');
              const missed = r.expect.filter((p) => !new RegExp(p, 'i').test(text));
              const reason = `${found.length} listed${missed.length ? `; missed /${missed.join('/, /')}/` : ', all expected ones among them'}`;
              return grade(r.expect.length - missed.length >= r.need, reason, (r.expect.length - missed.length) / r.expect.length);
            },
          },
        ],
      })),
      writeLatestResults: true,
    },
    { maxConcurrency: 1, showProgressBar: Boolean(process.stdout.isTTY) },
  );

  for (const [title, run] of [['answers with citations', answers], ['breaking changes from release notes', extraction]] as const) {
    const { results } = await run.toEvaluateSummary();
    console.log(`\n${title}\n\n| model | passed | score |\n|---|---|---|`);
    for (const label of new Set(results.map((r) => r.provider.label ?? r.provider.id))) {
      const mine = results.filter((r) => (r.provider.label ?? r.provider.id) === label);
      const score = mine.reduce((sum, r) => sum + r.score, 0) / mine.length;
      console.log(`| ${label} | ${mine.filter((r) => r.success).length}/${mine.length} | ${score.toFixed(2)} |`);
    }
    for (const r of results.filter((r) => !r.success)) {
      console.log(`  failed (${r.provider.label}): ${r.testCase.description}: ${r.error ?? r.gradingResult?.reason}`);
    }
  }
  console.log('\nsee every answer with: cd eval/llm && npx promptfoo view');
} finally {
  await db.end();
  await container.stop();
}
