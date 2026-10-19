import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { connectors } from './connectors/index.ts';
import { ask } from './core/answer.ts';
import { embedPending } from './core/embed.ts';
import { ingest, type Source } from './core/ingest.ts';
import { embed, rerank } from './core/models.ts';
import { MODES, search } from './core/search.ts';
import { connect, migrate } from './core/store.ts';
import { createGateway } from './llm/gateway.ts';
import { providersFromEnv } from './llm/providers.ts';
import { mergeSources, sourcesForPackageJson } from './track.ts';
import { breakingChanges } from './upgrade.ts';

const USAGE = `usage:
  npm run migrate                       create the tables
  npm run ingest                        sync every feed in sources.json, then embed what changed
  npm run search -- "<query>"           search everything synced
  npm run ask -- "<question>"           answer from the synced sources, with citations
  npm run upgrade -- <package> <from> [to]
                                        what breaks between two versions of an npm package (to defaults to latest)
  npm run usage                         model calls, errors, latency and tokens over the last 7 days
  npm run track -- <package.json>       follow the release notes and security advisories of its dependencies
  npm run eval                          score each search mode on the eval set
options:
  -n, --limit <count>                how many search results to show (default 10)
  -m, --mode <mode>                  keyword, vector, hybrid or rerank (default rerank)`;

const SOURCES_FILE = new URL('../sources.json', import.meta.url);

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    limit: { type: 'string', short: 'n', default: '10' },
    mode: { type: 'string', short: 'm', default: 'rerank' },
  },
});
const [command, arg, ...rest] = positionals;
const limit = Number(values.limit);
const mode = MODES.find((m) => m === values.mode);

const db = connect();
// Every model call is logged to llm_calls; the inserts are awaited before the pool closes.
const logged: Promise<unknown>[] = [];
const loggedGateway = () =>
  createGateway({
    providers: providersFromEnv(),
    onCall: (c) =>
      logged.push(
        db
          .query(
            `insert into llm_calls (provider, model, status, error, latency_ms, input_tokens, output_tokens)
             values ($1, $2, $3, $4, $5, $6, $7)`,
            [c.provider, c.model, c.status, c.error ?? null, c.latencyMs ?? null, c.inputTokens ?? null, c.outputTokens ?? null],
          )
          .catch((err) => console.error(`could not log a model call: ${err}`)),
      ),
  });

try {
  if (!Number.isInteger(limit) || limit < 1) throw new Error('--limit must be a positive whole number');
  if (!mode) throw new Error(`--mode must be one of: ${MODES.join(', ')}`);
  switch (command) {
    case 'migrate':
      await migrate(db);
      console.log('schema ready');
      break;

    case 'ingest': {
      const sources: Source[] = JSON.parse(await readFile(SOURCES_FILE, 'utf8'));
      for (const r of await ingest(db, sources, connectors)) {
        const name = `${r.connector} ${r.feed}`;
        if ('error' in r) {
          console.error(`FAILED ${name}: ${r.error}`);
          process.exitCode = 1;
        } else {
          console.log(`${r.inserted} new, ${r.updated} updated, ${r.unchanged} unchanged, ${r.closed} closed  ${name}`);
        }
      }
      const embedded = await embedPending(db, embed, (done) => console.log(`embedded ${done} documents`));
      console.log(embedded ? `done: ${embedded} documents embedded` : 'nothing new to embed');
      break;
    }

    case 'search': {
      if (!arg) throw new Error('give a search query');
      const hits = await search(db, arg, { mode, limit, embed, rerank });
      if (!hits.length) console.log('no matches');
      hits.forEach((h, i) => {
        const snippet = h.text.slice(h.text.indexOf('\n') + 1, 260).replace(/\s+/g, ' ');
        console.log(`${i + 1}. ${h.title}  [${h.source}]\n   ${h.url ?? ''}\n   ${snippet}…\n`);
      });
      break;
    }

    case 'ask': {
      if (!arg) throw new Error('give a question');
      let sources: { title: string; url: string | null }[] = [];
      for await (const ev of ask(db, arg, { gateway: loggedGateway(), embed, rerank })) {
        if (ev.type === 'sources') sources = ev.sources;
        if (ev.type === 'token') process.stdout.write(ev.text);
        if (ev.type === 'done') {
          console.log('\n');
          for (const n of ev.cited) console.log(`[${n}] ${sources[n - 1].title}  ${sources[n - 1].url ?? ''}`);
          if (ev.invalid.length) console.log(`(ignored citations to sources that don't exist: ${ev.invalid.join(', ')})`);
          if (ev.provider) console.log(ev.cached ? `\ncached answer, first written by ${ev.provider}` : `\nanswered by ${ev.provider}`);
        }
      }
      break;
    }

    case 'upgrade': {
      const [from, to] = rest;
      if (!arg || !from) throw new Error('give a package and the version you are on, e.g. npm run upgrade -- next 14.2.0 15.1.0');
      let read = false;
      const result = await breakingChanges(db, loggedGateway(), arg, from, to, (title) => {
        read = true;
        console.log(`reading the notes for ${title}…`);
      });
      const checked = result.releases.map((r) => r.version).join(', ');
      console.log(`${read ? '\n' : ''}${arg} ${from} → ${result.to} (github.com/${result.repo})`);
      console.log(checked ? `releases read: ${checked}; patch releases are skipped\n` : 'no major or minor release in that range; patch releases are skipped');
      for (const r of result.releases) {
        if (!r.changes) {
          console.log(`${r.version}  ${r.error ? `could not fetch the notes: ${r.error}` : 'no release notes on GitHub'}\n`);
          continue;
        }
        console.log(`${r.version}  ${r.url}`);
        if (!r.changes.length) console.log(r.short ? '  the notes are only a line or two; they may just link to a changelog' : '  nothing breaking listed');
        for (const b of r.changes) console.log(`  - ${b.change}${b.migration ? `\n    → ${b.migration}` : ''}`);
        console.log('');
      }
      break;
    }

    case 'usage': {
      const { rows } = await db.query(
        `select provider, model,
                count(*) filter (where status = 'ok')::int as ok,
                count(*) filter (where status = 'error')::int as errors,
                count(*) filter (where status = 'skipped')::int as skipped,
                round(avg(latency_ms) filter (where status = 'ok'))::int as avg_ms,
                coalesce(sum(input_tokens), 0)::int as input_tokens,
                coalesce(sum(output_tokens), 0)::int as output_tokens
         from llm_calls where created_at > now() - interval '7 days'
         group by provider, model order by provider`,
      );
      if (rows.length) console.table(rows);
      else console.log('no model calls in the last 7 days');
      break;
    }

    case 'track': {
      if (!arg) throw new Error('give the path to a package.json');
      const current: Source[] = JSON.parse(await readFile(SOURCES_FILE, 'utf8'));
      const { sources, noRepo } = await sourcesForPackageJson(arg);
      const added = mergeSources(current, sources);
      await writeFile(SOURCES_FILE, `${JSON.stringify([...current, ...added], null, 2)}\n`);
      console.log(`added ${added.length} feeds to sources.json; run npm run ingest to sync them`);
      if (noRepo.length) console.log(`no GitHub repo found, so advisories only: ${noRepo.join(', ')}`);
      break;
    }

    default:
      console.log(USAGE);
  }
} catch (err) {
  console.error(String(err));
  process.exitCode = 1;
} finally {
  await Promise.all(logged);
  await db.end();
}
