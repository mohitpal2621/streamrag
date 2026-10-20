import { randomUUID } from 'node:crypto';
import { Queue, Worker } from 'bullmq';
import type { Redis } from 'ioredis';
import type pg from 'pg';
import type { Connector, Source } from '../core/ingest.ts';
import { syncFeed } from '../core/store.ts';

export const SYNC_QUEUE = 'streamrag-sync';

// How often each kind of source is synced unless sources.json says otherwise. Hacker News is the
// live stream; blogs and advisories change slowly.
const EVERY: Record<string, number> = { hackernews: 60_000, rss: 15 * 60_000, osv: 6 * 60 * 60_000 };

export type ScheduledSource = Source & { every?: number };

// Deletes the key only while it still holds this worker's token, so a worker whose lock already
// expired can't release a lock that another worker has taken since.
const RELEASE = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0`;

// shortcut: no fencing token, so the TTL must outlast the slowest sync; at a 30 s fetch timeout,
// five minutes does. A sync that can run longer needs a fencing token checked on write.
export async function withLock<T>(redis: Redis, key: string, ttlMs: number, fn: () => Promise<T>) {
  const token = randomUUID();
  if ((await redis.set(key, token, 'PX', ttlMs, 'NX')) !== 'OK') return { held: true as const };
  try {
    return { held: false as const, value: await fn() };
  } finally {
    await redis.eval(RELEASE, 1, key, token);
  }
}

// One BullMQ job scheduler per source, kept in step with sources.json: new ones are added, removed
// ones deleted, changed intervals updated.
export async function scheduleSyncs(queue: Queue, sources: ScheduledSource[]) {
  const ids = new Set<string>();
  for (const s of sources) {
    const id = `${s.connector} ${s.feed}`;
    ids.add(id);
    await queue.upsertJobScheduler(
      id,
      { every: s.every ?? EVERY[s.connector] ?? 15 * 60_000 },
      {
        name: 'sync',
        data: { connector: s.connector, feed: s.feed },
        opts: { attempts: 3, backoff: { type: 'exponential', delay: 10_000 }, removeOnComplete: 1_000, removeOnFail: 1_000 },
      },
    );
  }
  for (const s of await queue.getJobSchedulers()) if (!ids.has(s.key)) await queue.removeJobScheduler(s.key);
  return ids.size;
}

// Syncs one feed per job, at most one job a second across all workers to stay polite. The lock keeps
// two workers (or a slow run and its retry) from syncing the same feed at once, which syncFeed's
// close-what-vanished step can't tolerate.
export function startSyncWorker(db: pg.Pool, redis: Redis, connectors: Record<string, Connector>, log: (line: string) => void) {
  const worker = new Worker(
    SYNC_QUEUE,
    async (job) => {
      const { connector, feed } = job.data as Source;
      const c = connectors[connector];
      if (!c) throw new Error(`unknown connector "${connector}"`);
      const r = await withLock(redis, `streamrag:sync:${connector}:${feed}`, 5 * 60_000, async () =>
        syncFeed(db, connector, feed, await c.fetchFeed(feed), c.snapshot),
      );
      if (r.held) return 'skipped: another worker is syncing this feed';
      const { inserted, updated, closed } = r.value;
      if (inserted || updated || closed) log(`sync: ${inserted} new, ${updated} updated, ${closed} closed  ${connector} ${feed}`);
      return r.value;
    },
    { connection: redis, concurrency: 1, limiter: { max: 1, duration: 1_000 } },
  );
  worker.on('failed', (job, err) => log(`sync failed (try ${job?.attemptsMade} of 3): ${job?.data.connector} ${job?.data.feed}: ${err.message}`));
  return worker;
}
