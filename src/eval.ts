// Scores each search mode on eval/queries.json. It starts a throwaway Postgres in Docker, syncs the
// advisories of the listed packages fresh from OSV, embeds them, and runs every question in every mode.
import { readFile } from 'node:fs/promises';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { connectors } from './connectors/index.ts';
import { embedPending } from './core/embed.ts';
import { ingest } from './core/ingest.ts';
import { embed, rerank } from './core/models.ts';
import { MODES, search } from './core/search.ts';
import { connect, migrate } from './core/store.ts';

type EvalSet = { packages: string[]; queries: { q: string; expect: string[] }[] };
const set: EvalSet = JSON.parse(await readFile(new URL('../eval/queries.json', import.meta.url), 'utf8'));

const container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
const db = connect(container.getConnectionUri());
try {
  await migrate(db);
  const synced = await ingest(db, set.packages.map((p) => ({ connector: 'osv', feed: `npm/${p}` })), connectors, 200);
  const failed = synced.filter((r) => 'error' in r);
  if (failed.length) throw new Error(`could not sync: ${failed.map((r) => r.feed).join(', ')}`);
  const docs = await embedPending(db, embed);
  console.log(`corpus: ${docs} advisories from ${set.packages.length} packages, ${set.queries.length} questions\n`);

  console.log('| mode | recall@1 | recall@5 | recall@10 | MRR@10 |');
  console.log('|---|---|---|---|---|');
  const misses: string[] = [];
  for (const mode of MODES) {
    const ranks: number[] = []; // 1-based rank of the first expected advisory, 0 if not in the top 10
    for (const { q, expect } of set.queries) {
      const hits = await search(db, q, { mode, limit: 10, embed, rerank });
      const rank = hits.findIndex((h) => expect.some((id) => h.url?.endsWith(`/${id}`))) + 1;
      ranks.push(rank);
      if (mode === 'rerank' && rank !== 1) misses.push(`${rank || '>10'}  ${q}`);
    }
    const recall = (k: number) => (ranks.filter((r) => r > 0 && r <= k).length / ranks.length).toFixed(2);
    const mrr = (ranks.reduce((sum, r) => sum + (r ? 1 / r : 0), 0) / ranks.length).toFixed(2);
    console.log(`| ${mode} | ${recall(1)} | ${recall(5)} | ${recall(10)} | ${mrr} |`);
  }
  if (misses.length) console.log(`\nrerank: questions not answered first (rank, question)\n${misses.join('\n')}`);
} finally {
  await db.end();
  await container.stop();
}
