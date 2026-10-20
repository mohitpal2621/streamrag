import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import type pg from 'pg';
import client from 'prom-client';
import { brokers, DLQ, lag, TOPIC } from './kafka.ts';
import { SYNC_QUEUE } from './scheduler.ts';

const reason = (err: unknown) => (err instanceof Error ? err.message : String(err));

// Runs fn against the sync queue, or says why Redis can't be reached.
async function withQueue<T>(fn: (queue: Queue) => Promise<T>): Promise<T | { unreachable: string }> {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', { maxRetriesPerRequest: 1, connectTimeout: 3_000, lazyConnect: true });
  redis.on('error', () => {}); // connect() rejects with the same error below; this stops ioredis printing it as well
  try {
    await redis.connect();
    const queue = new Queue(SYNC_QUEUE, { connection: redis });
    try {
      return await fn(queue);
    } finally {
      await queue.close();
    }
  } catch (err) {
    return { unreachable: reason(err) };
  } finally {
    redis.disconnect();
  }
}

async function kafkaLag() {
  try {
    const [embedder, alerts, transcriber, deadLetters] = await Promise.all(
      [lag('streamrag-embedder', TOPIC), lag('streamrag-alerts', TOPIC), lag('streamrag-transcriber', TOPIC), lag('streamrag-dlq-replay', DLQ)],
    );
    return { embedder: Number(embedder), alerts: Number(alerts), transcriber: Number(transcriber), deadLetters: Number(deadLetters) };
  } catch (err) {
    return { unreachable: `${brokers().join(', ')}: ${reason(err)}` };
  }
}

// How the pipeline is doing. Kafka and Redis are optional for everything but the pipeline itself,
// so either being down shows up as a reason instead of an error.
export async function pipelineStatus(db: pg.Pool) {
  const [{ rows }, kafka, syncs] = await Promise.all([
    db.query<{ documents: number; chunks: number; outbox: number; oldest: number | null; pending: number; alerts: number }>(
      `select (select count(*)::int from documents where closed_at is null) as documents,
              (select count(*)::int from chunks) as chunks,
              (select count(*)::int from outbox where published_at is null) as outbox,
              (select extract(epoch from now() - min(created_at))::int from outbox where published_at is null) as oldest,
              (select count(*)::int from documents where closed_at is null and embedded_hash is distinct from content_hash) as pending,
              (select count(*)::int from alerts where created_at > now() - interval '7 days') as alerts`,
    ),
    kafkaLag(),
    withQueue(async (queue) => {
      const [counts, scheduled] = await Promise.all([queue.getJobCounts('waiting', 'active', 'delayed', 'failed'), queue.getJobSchedulersCount()]);
      return { scheduled, waiting: counts.waiting, active: counts.active, delayed: counts.delayed, failed: counts.failed };
    }),
  ]);
  const s = rows[0];
  return {
    documents: s.documents,
    chunks: s.chunks,
    outbox: s.outbox,
    oldestOutboxSeconds: s.oldest,
    pendingEmbeddings: s.pending,
    alertsThisWeek: s.alerts,
    kafka,
    syncs,
  };
}

// Every feed with its open documents, the last time a sync saw it, and when the pipeline syncs it next.
export async function feeds(db: pg.Pool) {
  const [{ rows }, next] = await Promise.all([
    db.query<{ source: string; feed: string; documents: number; synced_at: Date }>(
      `select source, feed, count(*) filter (where closed_at is null)::int as documents, max(last_seen_at) as synced_at
       from documents group by source, feed order by source, feed`,
    ),
    withQueue(async (queue) => new Map((await queue.getJobSchedulers()).map((s) => [s.key, s.next]))),
  ]);
  return rows.map((r) => {
    const at = next instanceof Map ? next.get(`${r.source} ${r.feed}`) : undefined;
    return { source: r.source, feed: r.feed, documents: r.documents, syncedAt: r.synced_at.toISOString(), nextSyncAt: at ? new Date(at).toISOString() : null };
  });
}

type Status = Awaited<ReturnType<typeof pipelineStatus>>;

// The status, read once and shared for a couple of seconds. Each read opens connections to Kafka and
// Redis, which the dashboard's polling and Prometheus's scrapes shouldn't multiply.
export function sharedStatus(db: pg.Pool, ttlMs = 2_000) {
  let pending: Promise<Status> | null = null;
  return () => (pending ??= pipelineStatus(db).finally(() => setTimeout(() => (pending = null), ttlMs).unref()));
}

// The pipeline's backlog as Prometheus gauges, read when Prometheus scrapes, so they stay current
// while the pipeline is down. The API serves them.
export function statusGauges(db: pg.Pool) {
  const status = sharedStatus(db);
  const gauge = (name: string, help: string, labelNames: string[], read: (s: Status, g: client.Gauge) => void) =>
    new client.Gauge({
      name,
      help,
      labelNames,
      async collect() {
        this.reset();
        const s = await status().catch(() => null); // with the database down, leave it empty rather than fail the scrape
        if (s) read(s, this);
      },
    });
  gauge('streamrag_documents', 'Open documents', [], (s, g) => g.set(s.documents));
  gauge('streamrag_outbox_events', 'Outbox events not yet published to Kafka', [], (s, g) => g.set(s.outbox));
  gauge('streamrag_pending_embeddings', 'Open documents whose current version has no chunks yet', [], (s, g) => g.set(s.pendingEmbeddings));
  gauge('streamrag_up', 'Whether Kafka and Redis answer (1) or not (0)', ['service'], (s, g) => {
    g.set({ service: 'kafka' }, 'unreachable' in s.kafka ? 0 : 1);
    g.set({ service: 'redis' }, 'unreachable' in s.syncs ? 0 : 1);
  });
  gauge('streamrag_consumer_lag', 'Events a consumer group or the dead-letter topic has not handled yet', ['group'], (s, g) => {
    if ('unreachable' in s.kafka) return;
    const { embedder, alerts, transcriber, deadLetters } = s.kafka;
    for (const [group, behind] of Object.entries({ embedder, alerts, transcriber, dlq: deadLetters })) g.set({ group }, behind);
  });
}
