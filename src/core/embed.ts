import type pg from 'pg';
import { chunk } from './chunk.ts';
import type { Embed } from './models.ts';

// Chunks and embeds open documents that are new or changed since their last embedding, a batch
// at a time, so an interrupted run picks up where it stopped. Unchanged documents are never redone.
export async function embedPending(db: pg.Pool, embed: Embed, onBatch?: (done: number) => void) {
  let done = 0;
  for (;;) {
    const { rows: docs } = await db.query<{ id: string; title: string; body: string; content_hash: string }>(
      `select id, title, body, content_hash from documents
       where closed_at is null and embedded_hash is distinct from content_hash
       order by id limit 32`,
    );
    if (!docs.length) return done;

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
      // Stores the hash that was read above, so a document edited during this batch is redone next round.
      await client.query(
        `update documents set embedded_hash = v.hash
         from unnest($1::bigint[], $2::text[]) v(id, hash) where documents.id = v.id`,
        [docs.map((d) => d.id), docs.map((d) => d.content_hash)],
      );
      await client.query('commit');
    } catch (err) {
      await client.query('rollback');
      throw err;
    } finally {
      client.release();
    }
    done += docs.length;
    onBatch?.(done);
  }
}
