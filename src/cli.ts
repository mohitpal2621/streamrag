import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { JOB_SOURCES, rankJobs } from './apps/jobs/match.ts';
import { connectors } from './connectors/index.ts';
import { ingest, type Source } from './core/ingest.ts';
import { connect, migrate, openDocuments, search } from './core/store.ts';

const USAGE = `usage:
  npm run migrate                  create the tables
  npm run ingest                   sync every feed in sources.json
  npm run search -- "<query>"      full-text search across open documents
  npm run match -- <resume.txt>    rank open jobs against a plain-text resume
options:
  -n, --limit <count>              how many results to show (default 10)
  -l, --location <pattern>         match: only jobs whose location matches, e.g. "india|remote"`;

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    limit: { type: 'string', short: 'n', default: '10' },
    location: { type: 'string', short: 'l' },
  },
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
      const sources: Source[] = JSON.parse(await readFile(new URL('../sources.json', import.meta.url), 'utf8'));
      for (const r of await ingest(db, sources, connectors)) {
        const name = `${r.connector}/${r.feed}`.padEnd(24);
        if ('error' in r) {
          console.error(name, `FAILED ${r.error}`);
          process.exitCode = 1;
        } else {
          console.log(name, `${r.inserted} new, ${r.updated} updated, ${r.unchanged} unchanged, ${r.closed} closed`);
        }
      }
      break;
    }

    case 'search': {
      if (!arg) throw new Error('give a search query');
      const hits = await search(db, arg, limit);
      if (!hits.length) console.log('no matches');
      hits.forEach((h, i) =>
        console.log(`${i + 1}. ${h.title} — ${h.feed} (${h.source})\n   ${h.url ?? ''}\n   ${h.snippet.replace(/\s+/g, ' ')}\n`),
      );
      break;
    }

    case 'match': {
      if (!arg) throw new Error('give a plain-text resume file');
      const where = values.location ? new RegExp(values.location, 'i') : null;
      const open = (await openDocuments(db, JOB_SOURCES)).filter((d) => !where || where.test(d.meta.location ?? ''));
      const { skills, jobs, gaps } = rankJobs(await readFile(arg, 'utf8'), open, limit);
      console.log(`Skills found in your resume (${skills.length}): ${skills.join(', ')}\n`);
      jobs.forEach((j, i) =>
        console.log(
          `${i + 1}. ${j.title} — ${j.feed}${j.location ? ` (${j.location})` : ''}\n` +
            `   ${j.url ?? ''}\n` +
            `   have ${j.have.length}/${j.have.length + j.missing.length}: ${j.have.join(', ')}\n` +
            `   missing: ${j.missing.join(', ') || 'nothing'}\n`,
        ),
      );
      if (gaps.length) console.log(`Missing most often in these matches: ${gaps.slice(0, 8).map(([s, n]) => `${s} (${n})`).join(', ')}`);
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
