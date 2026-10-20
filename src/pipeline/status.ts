import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import type pg from 'pg';
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
