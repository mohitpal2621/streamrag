import { setTimeout as sleep } from 'node:timers/promises';
import type pg from 'pg';
import { syncFeed, type Doc } from './store.ts';

export type Source = { connector: string; feed: string };
export type Connector = (feed: string) => Promise<Doc[]>;

// Syncs each feed in turn. A feed that fails to fetch is skipped, so its documents stay open
// rather than being closed because of a network error.
export async function ingest(db: pg.Pool, sources: Source[], connectors: Record<string, Connector>, delayMs = 1000) {
  const results = [];
  for (const [i, { connector, feed }] of sources.entries()) {
    if (i) await sleep(delayMs); // job boards ask crawlers to keep to about one request a second
    try {
      const fetchFeed = connectors[connector];
      if (!fetchFeed) throw new Error(`unknown connector "${connector}"`);
      results.push({ connector, feed, ...(await syncFeed(db, connector, feed, await fetchFeed(feed))) });
    } catch (err) {
      results.push({ connector, feed, error: String(err) });
    }
  }
  return results;
}
