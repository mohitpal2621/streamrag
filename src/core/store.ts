import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';

export type Doc = {
  externalId: string;
  title: string;
  url: string | null;
  body: string;
  meta: Record<string, unknown>;
};

export const connect = (url = process.env.DATABASE_URL ?? 'postgres://streamrag:streamrag@localhost:5432/streamrag') =>
  new pg.Pool({ connectionString: url });

export async function migrate(db: pg.Pool) {
  await db.query(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'));
}

const contentHash = (d: Doc) =>
  createHash('sha256').update(JSON.stringify([d.title, d.url, d.body, d.meta])).digest('hex');

// Inserts new documents and updates changed ones; unchanged documents keep their updated_at.
// closeMissing is for snapshot feeds, which list everything that exists, so whatever they stop
// listing is gone. Append-only feeds (RSS, Hacker News) just roll old items off; those stay open.
// shortcut: assumes one sync per feed at a time; two concurrent syncs of the same feed need a lock.
export async function syncFeed(db: pg.Pool, source: string, feed: string, docs: Doc[], closeMissing = true) {
  const rows = [...new Map(docs.map((d) => [d.externalId, d])).values()]; // one upsert can't touch a row twice
  const client = await db.connect();
  try {
    await client.query('begin');
    // now() is fixed for the whole transaction, so "= now()" marks rows written by this sync.
    const upserted = await client.query<{ inserted: boolean; changed: boolean }>(
      `insert into documents (source, feed, external_id, title, url, body, meta, content_hash)
       select $1::text, $2::text, * from unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::jsonb[], $8::text[])
       on conflict (source, feed, external_id) do update set
         title = excluded.title, url = excluded.url, body = excluded.body, meta = excluded.meta,
         content_hash = excluded.content_hash,
         updated_at = case when documents.content_hash = excluded.content_hash then documents.updated_at else now() end,
         last_seen_at = now(),
         closed_at = null
       returning first_seen_at = now() as inserted, updated_at = now() as changed`,
      [
        source,
        feed,
        rows.map((d) => d.externalId),
        rows.map((d) => d.title),
        rows.map((d) => d.url),
        rows.map((d) => d.body),
        rows.map((d) => JSON.stringify(d.meta)),
        rows.map(contentHash),
      ],
    );
    const closed = closeMissing
      ? await client.query(
          `update documents set closed_at = now()
           where source = $1 and feed = $2 and closed_at is null and last_seen_at < now()`,
          [source, feed],
        )
      : null;
    await client.query('commit');
    const inserted = upserted.rows.filter((r) => r.inserted).length;
    const updated = upserted.rows.filter((r) => !r.inserted && r.changed).length;
    return { inserted, updated, unchanged: rows.length - inserted - updated, closed: closed?.rowCount ?? 0 };
  } catch (err) {
    await client.query('rollback');
    throw err;
  } finally {
    client.release();
  }
}

export async function search(db: pg.Pool, query: string, limit: number) {
  const { rows } = await db.query<{ title: string; source: string; feed: string; url: string | null; snippet: string }>(
    // Rank in the inner query; ts_headline is slow, so it only runs on the rows that survive the limit.
    `select title, source, feed, url,
            ts_headline('english', body, q, 'StartSel=**, StopSel=**, MaxFragments=1, MaxWords=30, MinWords=12') as snippet
     from (
       select title, source, feed, url, body, q, ts_rank_cd(tsv, q) as rank
       from documents, websearch_to_tsquery('english', $1) q
       where closed_at is null and tsv @@ q
       order by rank desc
       limit $2
     ) top
     order by rank desc`,
    [query, limit],
  );
  return rows;
}
