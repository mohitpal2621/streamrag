import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import type pg from 'pg';
import { brokers, DLQ, lag, TOPIC } from './kafka.ts';
import { SYNC_QUEUE } from './scheduler.ts';

const reason = (err: unknown) => (err instanceof Error ? err.message : String(err));

// How the pipeline is doing. Kafka and Redis are optional for everything but the pipeline itself,
// so either being down shows up as a reason instead of an error.
export async function pipelineStatus(db: pg.Pool) {
  const { rows } = await db.query<{ outbox: number; oldest: number | null; pending: number; alerts: number }>(
    `select (select count(*)::int from outbox where published_at is null) as outbox,
            (select extract(epoch from now() - min(created_at))::int from outbox where published_at is null) as oldest,
            (select count(*)::int from documents where closed_at is null and embedded_hash is distinct from content_hash) as pending,
            (select count(*)::int from alerts where created_at > now() - interval '7 days') as alerts`,
  );
  const s = rows[0];

  let kafka: { embedder: number; alerts: number; transcriber: number; deadLetters: number } | { unreachable: string };
  try {
    const [embedder, alerts, transcriber, deadLetters] = await Promise.all(
      [lag('streamrag-embedder', TOPIC), lag('streamrag-alerts', TOPIC), lag('streamrag-transcriber', TOPIC), lag('streamrag-dlq-replay', DLQ)],
    );
    kafka = { embedder: Number(embedder), alerts: Number(alerts), transcriber: Number(transcriber), deadLetters: Number(deadLetters) };
  } catch (err) {
    kafka = { unreachable: `${brokers().join(', ')}: ${reason(err)}` };
  }

  let syncs: { scheduled: number; waiting: number; active: number; delayed: number; failed: number } | { unreachable: string };
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', { maxRetriesPerRequest: 1, connectTimeout: 3_000, lazyConnect: true });
  redis.on('error', () => {}); // connect() rejects with the same error below; this stops ioredis printing it as well
  try {
    await redis.connect();
    const queue = new Queue(SYNC_QUEUE, { connection: redis });
    const [counts, scheduled] = await Promise.all([queue.getJobCounts('waiting', 'active', 'delayed', 'failed'), queue.getJobSchedulersCount()]);
    syncs = { scheduled, waiting: counts.waiting, active: counts.active, delayed: counts.delayed, failed: counts.failed };
    await queue.close();
  } catch (err) {
    syncs = { unreachable: reason(err) };
  } finally {
    redis.disconnect();
  }

  return { outbox: s.outbox, oldestOutboxSeconds: s.oldest, pendingEmbeddings: s.pending, alertsThisWeek: s.alerts, kafka, syncs };
}
