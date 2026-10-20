import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { askAgent, createAgent } from './agent/graph.ts';
import { connectors } from './connectors/index.ts';
import { ask } from './core/answer.ts';
import { embedPending } from './core/embed.ts';
import { ingest, type Source } from './core/ingest.ts';
import { embed, rerank } from './core/models.ts';
import { MODES, search } from './core/search.ts';
import { connect, migrate } from './core/store.ts';
import { clock, transcribeEpisode, untranscribed } from './media/transcribe.ts';
import { loggedGateway } from './llm/calls.ts';
import { deadLetters } from './pipeline/dlq.ts';
import { createProducer, TOPIC } from './pipeline/kafka.ts';
import { pipelineStatus } from './pipeline/status.ts';
import { mergeSources, sourcesForPackageJson } from './track.ts';
import { breakingChanges } from './upgrade.ts';

const USAGE = `usage:
  npm run migrate                       create the tables
  npm run ingest                        sync every feed in sources.json, then embed what changed
  npm run search -- "<query>"           search everything synced
  npm run ask -- "<question>"           answer from the synced sources, with citations
  npm run agent -- "<question>"         a tool-using agent: breaking changes, advisories and search
  npm run serve                         the GraphQL API at http://localhost:4000/graphql
  npm run mcp                           an MCP server over stdio, for coding assistants
  npm run upgrade -- <package> <from> [to]
                                        what breaks between two versions of an npm package (to defaults to latest)
  npm run usage                         model calls, errors, latency and tokens over the last 7 days
  npm run track -- <package.json>       follow the release notes and security advisories of its dependencies
  npm run eval                          score each search mode on the eval set
  npm run pipeline                      run the streaming pipeline (see README)
  npm run status                        pipeline backlog: outbox, Kafka lag, dead letters, sync jobs
  npm run alerts                        new security advisories the pipeline noticed
  npm run dlq [-- --replay]             list dead-lettered events, or send them back to be retried
  npm run transcribe [-- -n 2 --minutes 10]
                                        transcribe the newest podcast episodes without a transcript (FFmpeg + Whisper)
options:
  -n, --limit <count>                   how many results, alerts or episodes (default 10, or 1 to transcribe)
  -m, --mode <mode>                     keyword, vector, hybrid or rerank (default rerank)`;

const SOURCES_FILE = new URL('../sources.json', import.meta.url);

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    limit: { type: 'string', short: 'n' },
    mode: { type: 'string', short: 'm', default: 'rerank' },
    replay: { type: 'boolean', default: false },
    minutes: { type: 'string' },
  },
});
const [command, arg, ...rest] = positionals;
const limit = Number(values.limit ?? (command === 'transcribe' ? 1 : 10));
const mode = MODES.find((m) => m === values.mode);

const db = connect();
const { gateway, flush } = loggedGateway(db);

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
      for await (const ev of ask(db, arg, { gateway, embed, rerank })) {
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

    case 'agent': {
      if (!arg) throw new Error('give a question');
      let thinking = true;
      for await (const ev of askAgent(createAgent({ db, embed, rerank, gateway }), arg)) {
        if (ev.type === 'tool') console.log(`→ ${ev.name} ${ev.arguments}`);
        else {
          if (thinking) console.log('');
          thinking = false;
          process.stdout.write(ev.text);
        }
      }
      console.log('');
      break;
    }

    case 'transcribe': {
      const minutes = values.minutes === undefined ? undefined : Number(values.minutes);
      if (minutes !== undefined && !(minutes > 0)) throw new Error('--minutes must be a positive number');
      const episodes = await untranscribed(db, limit);
      if (!episodes.length) console.log('no podcast episodes without a transcript; add a podcast feed to sources.json and run npm run ingest');
      for (const ep of episodes) {
        const started = Date.now();
        console.log(`${ep.title}\n  ${ep.audio}`);
        const { seconds, words } = await transcribeEpisode(db, ep, {
          maxSeconds: minutes && minutes * 60,
          onProgress: (s) => process.stdout.write(`\r  ${clock(s)} transcribed`),
        });
        console.log(`\r  ${clock(seconds)} of audio, ${words} words, in ${clock((Date.now() - started) / 1000)}; run npm run ingest to make it searchable`);
      }
      break;
    }

    case 'upgrade': {
      const [from, to] = rest;
      if (!arg || !from) throw new Error('give a package and the version you are on, e.g. npm run upgrade -- next 14.2.0 15.1.0');
      let read = false;
      const result = await breakingChanges(db, gateway, arg, from, to, (title) => {
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

    case 'status': {
      const s = await pipelineStatus(db);
      console.log(`outbox      ${s.outbox} events waiting for the relay${s.oldestOutboxSeconds ? `, oldest ${s.oldestOutboxSeconds}s` : ''}`);
      console.log(`embeddings  ${s.pendingEmbeddings} documents waiting`);
      const k = s.kafka;
      console.log(
        'unreachable' in k
          ? `kafka       not reachable at ${k.unreachable}`
          : `kafka       behind by ${k.embedder} (embedder), ${k.alerts} (alerts), ${k.transcriber} (transcriber); ${k.deadLetters} dead letters not replayed`,
      );
      const q = s.syncs;
      console.log(
        'unreachable' in q
          ? `syncs       Redis not reachable (${q.unreachable})`
          : `syncs       ${q.scheduled} sources scheduled; ${q.waiting} waiting, ${q.active} running, ${q.delayed} delayed, ${q.failed} failed`,
      );
      console.log(`alerts      ${s.alertsThisWeek} new advisories in the last 7 days`);
      break;
    }

    case 'alerts': {
      const { rows } = await db.query<{ at: Date; feed: string; title: string; url: string | null }>(
        `select a.created_at as at, d.feed, d.title, d.url from alerts a join documents d on d.id = a.document_id
         order by a.id desc limit $1`,
        [limit],
      );
      if (!rows.length) console.log('no alerts yet; they appear when an advisory is published for a package you track');
      for (const r of rows) console.log(`${r.at.toISOString().slice(0, 16).replace('T', ' ')}  ${r.feed.replace(/^npm\//, '')}: ${r.title}  ${r.url ?? ''}`);
      break;
    }

    case 'dlq': {
      const producer = values.replay ? createProducer() : null;
      try {
        const found = await deadLetters(producer);
        for (const d of found) console.log(`${d.offset}  ${d.consumer}: ${d.error}\n  ${d.event}`);
        console.log(values.replay ? `sent ${found.length} events back to ${TOPIC}` : `${found.length} dead letters`);
      } finally {
        await producer?.close();
      }
      break;
    }

    default:
      console.log(USAGE);
  }
} catch (err) {
  console.error(String(err));
  process.exitCode = 1;
} finally {
  await flush();
  await db.end();
}
