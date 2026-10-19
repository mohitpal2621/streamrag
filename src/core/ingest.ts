import { setTimeout as sleep } from 'node:timers/promises';
import type pg from 'pg';
import { syncFeed, type Doc } from './store.ts';

export type Source = { connector: string; feed: string };
export type Connector = {
  snapshot: boolean; // true: each fetch lists everything that exists, so missing items get closed
  fetchFeed: (feed: string) => Promise<Doc[]>;
};

// Syncs each feed in turn. A feed that fails to fetch is skipped, so its documents stay open
// rather than being closed because of a network error.
export async function ingest(db: pg.Pool, sources: Source[], connectors: Record<string, Connector>, delayMs = 1000) {
  const results = [];
  for (const [i, { connector, feed }] of sources.entries()) {
    if (i) await sleep(delayMs); // keep to about one request a second
    try {
      const c = connectors[connector];
      if (!c) throw new Error(`unknown connector "${connector}"`);
      results.push({ connector, feed, ...(await syncFeed(db, connector, feed, await c.fetchFeed(feed), c.snapshot)) });
    } catch (err) {
      results.push({ connector, feed, error: String(err) });
    }
  }
  return results;
}
