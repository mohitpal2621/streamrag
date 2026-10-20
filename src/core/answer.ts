import type { Span } from '@opentelemetry/api';
import type pg from 'pg';
import type { Gateway } from '../llm/gateway.ts';
import { answers } from '../metrics.ts';
import { llmTracer, tracedStream } from '../tracing.ts';
import type { Embed, Rerank } from './models.ts';
import { search } from './search.ts';

export type Source = { documentId: string; title: string; url: string | null; hash: string };
export type AnswerEvent =
  | { type: 'sources'; sources: Source[] }
  | { type: 'token'; text: string }
  | { type: 'done'; answer: string; cited: number[]; invalid: number[]; provider: string | null; cached: boolean };

const SYSTEM = `You answer developer questions using only the numbered sources you are given.
Cite every claim with its source number in square brackets, like [2]. Use only the numbers listed.
If the sources do not answer the question, say that plainly instead of guessing. Keep it short.`;

const NOT_FOUND = "I couldn't find anything about that in the synced sources.";

// Cosine distance under which two questions count as the same one reworded. Kept tight on purpose:
// a wrong cache hit serves a confident answer to a different question.
const CACHE_DISTANCE = 0.05;

// Finds every number the answer cites, including [1, 3] and [1][3].
function citations(answer: string) {
  const numbers = [...answer.matchAll(/\[(\d+(?:\s*,\s*\d+)*)\]/g)].flatMap((m) => m[1].split(',').map(Number));
  return [...new Set(numbers)].sort((a, b) => a - b);
}

type AskOptions = { gateway: Gateway; embed: Embed; rerank: Rerank; k?: number };
type Done = Extract<AnswerEvent, { type: 'done' }>;

export const ask = (db: pg.Pool, question: string, opts: AskOptions): AsyncGenerator<AnswerEvent> =>
  tracedStream(llmTracer, 'answer', { 'input.value': question }, (span) => answer(db, question, opts, span));

function finish(span: Span, done: Done) {
  span.setAttributes({ 'output.value': done.answer, 'answer.cached': done.cached, 'answer.cited': done.cited.length });
  answers.inc({ cached: String(done.cached) });
  return done;
}

async function* answer(db: pg.Pool, question: string, opts: AskOptions, span: Span): AsyncGenerator<AnswerEvent> {
  const [vector] = await opts.embed([question], 'query');
  const literal = `[${vector.join(',')}]`;

  // A cached answer only counts while every document it cites is still open and unchanged.
  const { rows: cached } = await db.query<{ answer: string; sources: Source[]; cited: number[]; provider: string }>(
    `select a.answer, a.sources, a.cited, a.provider from answers a
     where a.created_at > now() - interval '1 day'
       and a.embedding <=> $1::vector < ${CACHE_DISTANCE}
       and not exists (
         select 1 from unnest(a.cited) n
         join documents d on d.id = (a.sources -> (n - 1) ->> 'documentId')::bigint
         where d.content_hash <> a.sources -> (n - 1) ->> 'hash' or d.closed_at is not null
       )
     order by a.embedding <=> $1::vector limit 1`,
    [literal],
  );
  if (cached[0]) {
    yield { type: 'sources', sources: cached[0].sources };
    yield { type: 'token', text: cached[0].answer };
    yield finish(span, { type: 'done', answer: cached[0].answer, cited: cached[0].cited, invalid: [], provider: cached[0].provider, cached: true });
    return;
  }

  const embedOnce: Embed = async (texts, kind) => (kind === 'query' && texts.length === 1 && texts[0] === question ? [vector] : opts.embed(texts, kind));
  const hits = await search(db, question, { mode: 'rerank', limit: opts.k ?? 6, embed: embedOnce, rerank: opts.rerank });
  if (!hits.length) {
    yield { type: 'token', text: NOT_FOUND };
    yield finish(span, { type: 'done', answer: NOT_FOUND, cited: [], invalid: [], provider: null, cached: false });
    return;
  }

  const { rows: hashes } = await db.query<{ id: string; content_hash: string }>('select id, content_hash from documents where id = any($1::bigint[])', [
    hits.map((h) => h.documentId),
  ]);
  const hashOf = new Map(hashes.map((r) => [r.id, r.content_hash]));
  const sources: Source[] = hits.map((h) => ({ documentId: h.documentId, title: h.title, url: h.url, hash: hashOf.get(h.documentId) ?? '' }));
  yield { type: 'sources', sources };

  const context = hits.map((h, i) => `[${i + 1}] ${h.title}\n${h.url ?? ''}\n${h.text.slice(h.text.indexOf('\n') + 1)}`).join('\n\n');
  let answer = '';
  let provider: string | null = null;
  for await (const token of opts.gateway.stream([
    { role: 'system', content: SYSTEM },
    { role: 'user', content: `Sources:\n\n${context}\n\nQuestion: ${question}` },
  ])) {
    provider ??= token.provider;
    answer += token.text;
    yield { type: 'token', text: token.text };
  }

  const all = citations(answer);
  const cited = all.filter((n) => n >= 1 && n <= sources.length);
  const invalid = all.filter((n) => n < 1 || n > sources.length);
  // Only grounded answers are cached. "Not in the sources" may stop being true after the next sync.
  if (cited.length && provider) {
    await db.query(
      `insert into answers (question, embedding, answer, sources, cited, provider)
       values ($1, $2::vector, $3, $4::jsonb, $5::int[], $6)`,
      [question, literal, answer, JSON.stringify(sources), cited, provider],
    );
  }
  yield finish(span, { type: 'done', answer, cited, invalid, provider, cached: false });
}
