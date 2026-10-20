import type pg from 'pg';
import { trace } from '@opentelemetry/api';
import { rerankSkipped, searchSeconds } from '../metrics.ts';
import { traced, tracer } from '../tracing.ts';
import type { Embed, Rerank } from './models.ts';

export const MODES = ['keyword', 'vector', 'hybrid', 'rerank'] as const;
export type Mode = (typeof MODES)[number];

export type Hit = { documentId: string; title: string; source: string; url: string | null; text: string; score: number };

const CANDIDATES = 50; // per leg, and the pool the final results come from
const RERANK_POOL = 30; // the cross-encoder is slow, so it only sees the best fused chunks

// The cross-encoder takes one to two seconds for 30 chunks on 2 cores and keeps every core busy, so
// reranks run one at a time, and a search that would wait behind more than RERANK_QUEUE others keeps
// the hybrid order instead. Without this, at 2 rerank searches a second, the queue grew until
// requests timed out; with it, the slowest stay under a few seconds and the rest take milliseconds.
const RERANK_QUEUE = 1;
let reranking: Promise<unknown> = Promise.resolve();
let queued = 0;

async function rerankInTurn(rerank: Rerank, query: string, texts: string[]) {
  if (queued >= RERANK_QUEUE + 1) return null;
  queued++;
  const turn = reranking.then(() => traced(tracer, 'rerank', { 'rerank.candidates': texts.length }, () => rerank(query, texts)));
  reranking = turn.catch(() => {});
  try {
    return await turn;
  } finally {
    queued--;
  }
}

// Keyword and vector search each rank their best chunks, then reciprocal rank fusion adds
// 1 / (60 + rank) from each list. A chunk both legs like wins, and raw scores from the two legs,
// which aren't comparable, never get mixed. A disabled leg returns nothing, so one query serves all modes.
// The keyword leg needs every word of the query. Matching any word looked better on its own in the
// eval (recall@10 0.14 -> 0.77) but made hybrid worse (1.00 -> 0.93), because fusion trusts a noisy
// leg as much as a precise one. Kept strict, it fires on exact names and ids, which vectors miss.
const FUSED = `
  with keyword as (
    select c.id, row_number() over (order by ts_rank_cd(c.tsv, q) desc) as rank
    from chunks c join documents d on d.id = c.document_id, websearch_to_tsquery('english', $1) q
    where $3 and d.closed_at is null and c.tsv @@ q
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

type SearchOptions = { mode: Mode; limit: number; embed: Embed; rerank: Rerank };

export function search(db: pg.Pool, query: string, opts: SearchOptions) {
  const done = searchSeconds.startTimer({ mode: opts.mode });
  return traced(tracer, 'search', { 'search.query': query, 'search.mode': opts.mode, 'search.limit': opts.limit }, async (span) => {
    const hits = await find(db, query, opts);
    span.setAttribute('search.hits', hits.length);
    return hits;
  }).finally(done);
}

async function find(db: pg.Pool, query: string, opts: SearchOptions) {
  const useKeyword = opts.mode !== 'vector';
  const useVector = opts.mode !== 'keyword';
  const vector = useVector ? `[${(await traced(tracer, 'embed query', {}, () => opts.embed([query], 'query')))[0].join(',')}]` : null;

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
    const scores = await rerankInTurn(opts.rerank, query, pool.map((c) => c.text));
    if (scores) chunks = pool.map((c, i) => ({ ...c, score: scores[i] })).sort((a, b) => b.score - a.score);
    else rerankSkipped.inc();
    trace.getActiveSpan()?.setAttribute('search.reranked', scores !== null);
  }

  // Results are documents: each shows up once, with its best chunk.
  const hits: Hit[] = [];
  const seen = new Set<string>();
  for (const c of chunks) {
    if (seen.has(c.documentId)) continue;
    seen.add(c.documentId);
    hits.push(atMoment(c));
    if (hits.length === opts.limit) break;
  }
  return hits;
}

// A transcript's URL is its audio file. The first time mark in the chunk ([12:34]) becomes a media
// fragment, so the link starts playback at that moment.
function atMoment(hit: Hit): Hit {
  const m = hit.source === 'transcript' && hit.url ? hit.text.match(/\[(?:(\d+):)?(\d+):(\d\d)\]/) : null;
  return m ? { ...hit, url: `${hit.url}#t=${Number(m[1] ?? 0) * 3600 + Number(m[2]) * 60 + Number(m[3])}` } : hit;
}
