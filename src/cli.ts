import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { connectors } from './connectors/index.ts';
import { ingest, type Source } from './core/ingest.ts';
import { connect, migrate, search } from './core/store.ts';
import { mergeSources, sourcesForPackageJson } from './track.ts';

const USAGE = `usage:
  npm run migrate                    create the tables
  npm run ingest                     sync every feed in sources.json
  npm run search -- "<query>"        full-text search across everything synced
  npm run track -- <package.json>    follow the release notes and security advisories of its dependencies
options:
  -n, --limit <count>                how many search results to show (default 10)`;

const SOURCES_FILE = new URL('../sources.json', import.meta.url);

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: { limit: { type: 'string', short: 'n', default: '10' } },
});
const [command, arg] = positionals;
const limit = Number(values.limit);

const db = connect();
try {
  if (!Number.isInteger(limit) || limit < 1) throw new Error('--limit must be a positive whole number');
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
      break;
    }

    case 'search': {
      if (!arg) throw new Error('give a search query');
      const hits = await search(db, arg, limit);
      if (!hits.length) console.log('no matches');
      hits.forEach((h, i) =>
        console.log(`${i + 1}. ${h.title}  [${h.source}]\n   ${h.url ?? ''}\n   ${h.snippet.replace(/\s+/g, ' ')}\n`),
      );
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
