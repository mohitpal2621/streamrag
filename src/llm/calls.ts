import type pg from 'pg';
import { llmCallSeconds, llmSkips, llmTokens } from '../metrics.ts';
import { createGateway, type CallLog } from './gateway.ts';
import { providersFromEnv } from './providers.ts';

function count(c: CallLog) {
  const labels = { provider: c.provider, model: c.model };
  if (c.status === 'skipped') return llmSkips.inc(labels);
  llmCallSeconds.observe({ ...labels, status: c.status }, (c.latencyMs ?? 0) / 1000);
  if (c.inputTokens) llmTokens.inc({ ...labels, direction: 'input' }, c.inputTokens);
  if (c.outputTokens) llmTokens.inc({ ...labels, direction: 'output' }, c.outputTokens);
}

// A gateway over the providers configured in .env that logs every call to llm_calls and counts it
// in the metrics. flush() waits for the inserts, so a short-lived command can close the pool after it.
export function loggedGateway(db: pg.Pool) {
  const pending = new Set<Promise<unknown>>(); // only the inserts still running, since the API runs for days
  const log = (c: CallLog) => {
    count(c);
    const insert = db
      .query(
        `insert into llm_calls (provider, model, status, error, latency_ms, input_tokens, output_tokens)
         values ($1, $2, $3, $4, $5, $6, $7)`,
        [c.provider, c.model, c.status, c.error ?? null, c.latencyMs ?? null, c.inputTokens ?? null, c.outputTokens ?? null],
      )
      .catch((err) => console.error(`could not log a model call: ${err}`))
      .finally(() => pending.delete(insert));
    pending.add(insert);
  };
  return { gateway: createGateway({ providers: providersFromEnv(), onCall: log }), flush: () => Promise.all(pending) };
}

// Calls, errors, latency and tokens per provider and model over the last 7 days.
export async function usage(db: pg.Pool) {
  const { rows } = await db.query<{
    provider: string;
    model: string;
    ok: number;
    errors: number;
    skipped: number;
    avgMs: number | null;
    p95Ms: number | null;
    inputTokens: number;
    outputTokens: number;
  }>(
    `select provider, model,
            count(*) filter (where status = 'ok')::int as ok,
            count(*) filter (where status = 'error')::int as errors,
            count(*) filter (where status = 'skipped')::int as skipped,
            round(avg(latency_ms) filter (where status = 'ok'))::int as "avgMs",
            round(percentile_cont(0.95) within group (order by latency_ms) filter (where status = 'ok'))::int as "p95Ms",
            coalesce(sum(input_tokens), 0)::int as "inputTokens",
            coalesce(sum(output_tokens), 0)::int as "outputTokens"
     from llm_calls where created_at > now() - interval '7 days'
     group by provider, model order by provider, model`,
  );
  return rows;
}
