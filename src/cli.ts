import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { connectors } from './connectors/index.ts';
import { embedPending } from './core/embed.ts';
import { ingest, type Source } from './core/ingest.ts';
import { embed, rerank } from './core/models.ts';
import { MODES, search } from './core/search.ts';
import { connect, migrate } from './core/store.ts';
import { mergeSources, sourcesForPackageJson } from './track.ts';

const USAGE = `usage:
  npm run migrate                    create the tables
  npm run ingest                     sync every feed in sources.json, then embed what changed
  npm run search -- "<query>"        search everything synced
  npm run track -- <package.json>    follow the release notes and security advisories of its dependencies
  npm run eval                       score each search mode on the eval set
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
const [command, arg] = positionals;
const limit = Number(values.limit);
const mode = MODES.find((m) => m === values.mode);

const db = connect();
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
  await db.end();
}
