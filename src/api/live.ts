import { setTimeout as sleep } from 'node:timers/promises';
import { createPubSub } from 'graphql-yoga';
import type pg from 'pg';

export type DocumentActivity = {
  type: 'document';
  id: string; // the outbox row
  at: string;
  change: 'added' | 'changed' | 'closed';
  searchableAfterMs: number | null;
  document: { id: string; source: string; feed: string; title: string; url: string | null; snippet: string; searchable: boolean };
};
export type Alert = { id: string; at: string; package: string; title: string; url: string | null };
export type Activity = DocumentActivity | { type: 'searchable'; at: string; documentIds: string[] } | { type: 'alert'; alert: Alert };

// Outbox rows as feed entries, newest first, with each document's current title and whether its
// current version is embedded yet. The relay prunes published rows after a day, so this is recent history.
// A sync writes the outbox row in the transaction that sets updated_at, so the row whose created_at
// equals updated_at is the change the current chunks were made for, and its time to searchable is
// embedded_at minus that.
export async function documentActivity(db: pg.Pool, { ids, limit }: { ids?: string[]; limit?: number }) {
  const { rows } = await db.query<{
    id: string;
    created_at: Date;
    type: string;
    inserted: boolean;
    document_id: string;
    source: string;
    feed: string;
    title: string;
    url: string | null;
    snippet: string;
    searchable: boolean;
    searchable_after_ms: number | null;
  }>(
    // The snippet skips a leading markdown heading, which OSV advisories open with.
    `select o.id, o.created_at, o.payload->>'type' as type, (o.payload->>'inserted')::boolean as inserted,
            d.id as document_id, d.source, d.feed, d.title, d.url,
            trim(left(regexp_replace(regexp_replace(d.body, '^\\s*#[^\\n]*\\n', ''), '\\s+', ' ', 'g'), 200)) as snippet,
            d.embedded_hash is not distinct from d.content_hash as searchable,
            case when d.embedded_hash is not distinct from d.content_hash and o.created_at = d.updated_at and d.embedded_at >= o.created_at
              then (extract(epoch from d.embedded_at - o.created_at) * 1000)::float8 end as searchable_after_ms
     from outbox o join documents d on d.id = (o.payload->>'documentId')::bigint
     where $1::bigint[] is null or o.id = any($1::bigint[])
     order by o.id desc limit $2`,
    [ids ?? null, limit ?? null],
  );
  return rows.map(
    (r): DocumentActivity => ({
      type: 'document',
      id: r.id,
      at: r.created_at.toISOString(),
      change: r.type === 'document.closed' ? 'closed' : r.inserted ? 'added' : 'changed',
      searchableAfterMs: r.type === 'document.closed' ? null : r.searchable_after_ms,
      document: { id: r.document_id, source: r.source, feed: r.feed, title: r.title, url: r.url, snippet: r.snippet, searchable: r.searchable },
    }),
  );
}

export async function listAlerts(db: pg.Pool, { ids, limit }: { ids?: string[]; limit?: number }) {
  const { rows } = await db.query<{ id: string; created_at: Date; feed: string; title: string; url: string | null }>(
    `select a.id, a.created_at, d.feed, d.title, d.url from alerts a join documents d on d.id = a.document_id
     where $1::bigint[] is null or a.id = any($1::bigint[])
     order by a.id desc limit $2`,
    [ids ?? null, limit ?? null],
  );
  return rows.map((r): Alert => ({ id: r.id, at: r.created_at.toISOString(), package: r.feed.replace(/^npm\//, ''), title: r.title, url: r.url }));
}

// Documents added or changed per source, in 10-second buckets over the last `minutes`.
export async function pulse(db: pg.Pool, minutes: number) {
  const { rows } = await db.query<{ at: Date; source: string; documents: number }>(
    `select to_timestamp(floor(extract(epoch from created_at) / 10) * 10) as at, payload->>'source' as source, count(*)::int as documents
     from outbox where created_at > now() - make_interval(mins => $1) and payload->>'type' = 'document.changed'
     group by 1, 2 order by 1`,
    [minutes],
  );
  return rows.map((r) => ({ at: r.at.toISOString(), source: r.source, documents: r.documents }));
}

const CHANNELS = ['outbox', 'embedded', 'alert'];

// Turns the NOTIFYs that syncs, the embedder and the alerts consumer send on commit into one stream
// of events, fanned out to every subscriber. It keeps a single connection LISTENing and comes back
// after the database restarts; events sent while it was gone are missed, so a client reloads
// recent activity when its stream reconnects.
export async function startLive(db: pg.Pool, log: (line: string) => void = console.error) {
  const pubsub = createPubSub<{ activity: [Activity] }>();
  const publish = (a: Activity) => pubsub.publish('activity', a);

  // One notification at a time, so subscribers see changes in the order they committed.
  let queue = Promise.resolve();
  const onNotification = (n: pg.Notification) => {
    const ids = n.payload ? n.payload.split(',') : [];
    queue = queue
      .then(async () => {
        if (n.channel === 'outbox') for (const a of (await documentActivity(db, { ids })).reverse()) publish(a);
        else if (n.channel === 'embedded') publish({ type: 'searchable', at: new Date().toISOString(), documentIds: ids });
        else for (const alert of await listAlerts(db, { ids })) publish({ type: 'alert', alert });
      })
      .catch((err) => log(`live: ${err}`));
  };

  let client: pg.PoolClient | undefined;
  let stopped = false;
  let reconnecting: Promise<void> | undefined;
  const reconnect = (err: unknown) => {
    client?.release(true);
    client = undefined;
    if (stopped || reconnecting) return;
    log(`live: lost the database connection (${err instanceof Error ? err.message : err}); listening again in 2s`);
    reconnecting = sleep(2_000).then(() => {
      reconnecting = undefined;
      return listen();
    });
  };
  const listen = async () => {
    if (stopped) return;
    let c: pg.PoolClient | undefined;
    try {
      c = await db.connect();
      client = c;
      c.on('notification', onNotification);
      c.on('error', (err) => client === c && reconnect(err));
      await c.query(CHANNELS.map((ch) => `listen ${ch}`).join('; '));
    } catch (err) {
      if (!c || client === c) reconnect(err);
    }
  };
  await listen();

  return {
    subscribe: () => pubsub.subscribe('activity'),
    close: async () => {
      stopped = true;
      await reconnecting;
      await queue;
      client?.release(true); // drops the connection, and its LISTENs with it
      client = undefined;
    },
  };
}

export type Live = Awaited<ReturnType<typeof startLive>>;
