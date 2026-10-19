import type pg from 'pg';
import type { Embed, Rerank } from './models.ts';

export const MODES = ['keyword', 'vector', 'hybrid', 'rerank'] as const;
export type Mode = (typeof MODES)[number];

export type Hit = { documentId: string; title: string; source: string; url: string | null; text: string; score: number };

const CANDIDATES = 50; // per leg, and the pool the final results come from
const RERANK_POOL = 30; // the cross-encoder is slow, so it only sees the best fused chunks

// Keyword and vector search each rank their best chunks, then reciprocal rank fusion adds
// 1 / (60 + rank) from each list. A chunk both legs like wins, and raw scores from the two legs,
// which aren't comparable, never get mixed. A disabled leg returns nothing, so one query serves all modes.
// A question rarely has all of its words in one chunk, so the keyword leg matches any of them (OR)
// and leaves it to ts_rank_cd to favour chunks that have more of them, close together.
const FUSED = `
  with terms as (
    select to_tsquery('simple', string_agg(quote_literal(lexeme), ' | ')) as q
    from unnest(to_tsvector('english', $1))
  ),
  keyword as (
    select c.id, row_number() over (order by ts_rank_cd(c.tsv, terms.q) desc) as rank
    from chunks c join documents d on d.id = c.document_id, terms
    where $3 and d.closed_at is null and c.tsv @@ terms.q
    order by rank limit ${CANDIDATES}
  ),
  vector as (
    select id, row_number() over (order by distance) as rank
    from (
      select c.id, c.embedding <=> $2::vector as distance
      from chunks c join documents d on d.id = c.document_id
      where $4 and d.closed_at is null
      order by distance limit ${CANDIDATES}
    ) nearest
  ),
  fused as (
    select coalesce(k.id, v.id) as id, coalesce(1.0 / (60 + k.rank), 0) + coalesce(1.0 / (60 + v.rank), 0) as score
    from keyword k full join vector v on k.id = v.id
  )
  select d.id as "documentId", d.title, d.source, d.url, c.text, f.score::float8 as score
  from fused f join chunks c on c.id = f.id join documents d on d.id = c.document_id
  order by f.score desc limit ${CANDIDATES}`;

export async function search(db: pg.Pool, query: string, opts: { mode: Mode; limit: number; embed: Embed; rerank: Rerank }) {
  const useKeyword = opts.mode !== 'vector';
  const useVector = opts.mode !== 'keyword';
  const vector = useVector ? `[${(await opts.embed([query], 'query'))[0].join(',')}]` : null;

  const client = await db.connect();
  let chunks: Hit[];
  try {
    await client.query('begin');
    // pgvector 0.8: keep walking the HNSW graph when the closed-document filter throws results away.
    await client.query('set local hnsw.iterative_scan = relaxed_order');
    chunks = (await client.query<Hit>(FUSED, [query, vector, useKeyword, useVector])).rows;
    await client.query('commit');
  } catch (err) {
    await client.query('rollback');
    throw err;
  } finally {
    client.release();
  }

  // The cross-encoder was trained on questions, and the eval showed it pushing exact matches for a
  // bare identifier (CVE-2025-29927, v16.4.2) out of the top 10, so those keep the hybrid order.
  const isIdentifier = /^\S*\d\S*$/.test(query.trim());
  if (opts.mode === 'rerank' && chunks.length && !isIdentifier) {
    const pool = chunks.slice(0, RERANK_POOL);
    const scores = await opts.rerank(query, pool.map((c) => c.text));
    chunks = pool.map((c, i) => ({ ...c, score: scores[i] })).sort((a, b) => b.score - a.score);
  }

  // Results are documents: each shows up once, with its best chunk.
  const hits: Hit[] = [];
  const seen = new Set<string>();
  for (const c of chunks) {
    if (seen.has(c.documentId)) continue;
    seen.add(c.documentId);
    hits.push(c);
    if (hits.length === opts.limit) break;
  }
  return hits;
}
