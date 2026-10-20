import type pg from 'pg';
import { chunk } from './chunk.ts';
import type { Embed } from './models.ts';
import { notifyIds } from './store.ts';

type Pending = { id: string; title: string; body: string; content_hash: string };

// Open documents among `ids` whose current version has no chunks yet. Asking twice is harmless,
// which is what makes the Kafka embedder idempotent.
export async function pendingDocs(db: pg.Pool, ids: string[]) {
  const { rows } = await db.query<Pending>(
    `select id, title, body, content_hash from documents
     where id = any($1::bigint[]) and closed_at is null and embedded_hash is distinct from content_hash`,
    [ids],
  );
  return rows;
}

// Replaces the chunks of these documents in one transaction.
export async function embedDocs(db: pg.Pool, embed: Embed, docs: Pending[]) {
  const pieces = docs.flatMap((d) => chunk(d.title, d.body).map((text, ord) => ({ documentId: d.id, ord, text })));
  const vectors = await embed(pieces.map((p) => p.text), 'passage');

  const client = await db.connect();
  try {
    await client.query('begin');
    await client.query('delete from chunks where document_id = any($1::bigint[])', [docs.map((d) => d.id)]);
    await client.query(
      `insert into chunks (document_id, ord, text, embedding)
       select * from unnest($1::bigint[], $2::int[], $3::text[], $4::vector[])`,
      [pieces.map((p) => p.documentId), pieces.map((p) => p.ord), pieces.map((p) => p.text), vectors.map((v) => `[${v.join(',')}]`)],
    );
    // Stores the hash that was read, so a document edited in the meantime is redone next time, and
    // when, so the dashboard can show how long the document took to become searchable.
    await client.query(
      `update documents set embedded_hash = v.hash, embedded_at = clock_timestamp()
       from unnest($1::bigint[], $2::text[]) v(id, hash) where documents.id = v.id`,
      [docs.map((d) => d.id), docs.map((d) => d.content_hash)],
    );
    await notifyIds(client, 'embedded', docs.map((d) => d.id)); // the dashboard marks them searchable
    await client.query('commit');
  } catch (err) {
    await client.query('rollback');
    throw err;
  } finally {
    client.release();
  }
}

// Chunks and embeds open documents that are new or changed since their last embedding, a batch
// at a time, so an interrupted run picks up where it stopped. Unchanged documents are never redone.
export async function embedPending(db: pg.Pool, embed: Embed, onBatch?: (done: number) => void) {
  let done = 0;
  for (;;) {
    const { rows: docs } = await db.query<Pending>(
      `select id, title, body, content_hash from documents
       where closed_at is null and embedded_hash is distinct from content_hash
       order by id limit 32`,
    );
    if (!docs.length) return done;
    await embedDocs(db, embed, docs);
    done += docs.length;
    onBatch?.(done);
  }
}
