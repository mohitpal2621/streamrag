import type { Producer } from '@platformatic/kafka';
import type pg from 'pg';
import { TOPIC } from './kafka.ts';

// Publishes outbox rows to Kafka in id order and marks them published once Kafka has acknowledged
// them. A crash in between sends those rows again, so delivery is at least once and consumers
// dedupe. SKIP LOCKED lets several relays share one outbox.
export async function drainOutbox(db: pg.Pool, producer: Producer<string, string, string, string>, batch = 500) {
  let sent = 0;
  for (;;) {
    const client = await db.connect();
    try {
      await client.query('begin');
      const { rows } = await client.query<{ id: string; key: string; payload: object }>(
        'select id, key, payload from outbox where published_at is null order by id limit $1 for update skip locked',
        [batch],
      );
      if (rows.length) {
        await producer.send({ messages: rows.map((r) => ({ topic: TOPIC, key: r.key, value: JSON.stringify({ ...r.payload, id: r.id }) })) });
        await client.query('update outbox set published_at = now() where id = any($1::bigint[])', [rows.map((r) => r.id)]);
      }
      await client.query('commit');
      sent += rows.length;
      if (rows.length < batch) return sent;
    } catch (err) {
      await client.query('rollback');
      throw err;
    } finally {
      client.release();
    }
  }
}

// Drains whenever a sync commits (LISTEN/NOTIFY) and on a timer as a fallback, one drain at a time.
export async function startRelay(
  db: pg.Pool,
  producer: Producer<string, string, string, string>,
  log: (line: string) => void,
  pollMs = 5_000,
) {
  let running: Promise<void> | null = null;
  let again = false;
  const kick = () => {
    if (running) {
      again = true;
      return;
    }
    running = (async () => {
      do {
        again = false;
        try {
          const sent = await drainOutbox(db, producer);
          if (sent) log(`relay: published ${sent} event${sent === 1 ? '' : 's'}`);
        } catch (err) {
          log(`relay: ${err}; retrying on the next tick`);
        }
      } while (again);
      running = null;
    })();
  };

  const listener = await db.connect();
  await listener.query('listen outbox');
  listener.on('notification', kick);
  const timer = setInterval(() => {
    kick();
    db.query("delete from outbox where published_at < now() - interval '1 day'").catch((err) => log(`relay: could not prune the outbox: ${err}`));
  }, pollMs);
  kick();

  return async () => {
    clearInterval(timer);
    await listener.query('unlisten outbox');
    listener.release();
    await running;
  };
}
