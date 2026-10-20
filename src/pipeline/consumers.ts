import { setTimeout as sleep } from 'node:timers/promises';
import {
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  stringDeserializers,
  type Message,
  type Producer,
} from '@platformatic/kafka';
import type pg from 'pg';
import { embedDocs, pendingDocs } from '../core/embed.ts';
import type { Embed } from '../core/models.ts';
import { notifyIds } from '../core/store.ts';
import { clock, transcribeEpisode, untranscribed, type Recognize } from '../media/transcribe.ts';
import { brokers, DLQ, TOPIC, type DocumentEvent } from './kafka.ts';

export type Handler = (events: DocumentEvent[]) => Promise<void>;
type Log = (line: string) => void;

// Runs one consumer group over the document events. With batchSize above 1, events that have already
// arrived are handled together; if that fails they are redone one at a time. An event that keeps
// failing on its own goes to the dead-letter topic with its error, so it can't stall its partition.
// Offsets are committed only after events were handled or dead-lettered, so a crash replays them
// instead of losing them, which is why every handler has to be safe to run twice.
export async function startConsumer(
  name: string,
  handle: Handler,
  producer: Producer<string, string, string, string>,
  log: Log,
  { batchSize = 1, attempts = 3, retryDelayMs = 200, bootstrapBrokers = brokers() } = {},
) {
  const consumer = new Consumer({
    groupId: `streamrag-${name}`,
    clientId: `streamrag-${name}`,
    bootstrapBrokers,
    deserializers: stringDeserializers,
    autocommit: false,
  });
  const stream = await consumer.consume({ topics: [TOPIC], mode: MessagesStreamModes.COMMITTED, fallbackMode: MessagesStreamFallbackModes.EARLIEST });
  const parse = (m: Message<string, string, string, string>) => JSON.parse(m.value) as DocumentEvent;

  const done = (async () => {
    for await (const first of stream) {
      const messages = [first];
      while (messages.length < batchSize) {
        const next = stream.read();
        if (next === null) break;
        messages.push(next);
      }
      if (messages.length > 1) {
        try {
          await handle(messages.map(parse));
          for (const m of messages) await m.commit();
          continue;
        } catch {
          // redone one at a time below, which leaves the bad event alone in the dead-letter topic
        }
      }
      for (const message of messages) {
        for (let attempt = 1; ; attempt++) {
          try {
            await handle([parse(message)]);
            break;
          } catch (err) {
            if (attempt < attempts) {
              await sleep(retryDelayMs * 4 ** (attempt - 1));
              continue;
            }
            log(`${name}: event ${message.partition}/${message.offset} failed ${attempts} times, moved to ${DLQ}: ${err}`);
            await producer.send({
              messages: [
                {
                  topic: DLQ,
                  key: message.key,
                  value: message.value,
                  headers: { consumer: name, error: String(err).slice(0, 1_000), source: `${message.topic}/${message.partition}/${message.offset}` },
                },
              ],
            });
            break;
          }
        }
        await message.commit();
      }
    }
  })();
  const stop = async () => {
    await stream.close();
    await done;
    await consumer.close();
  };
  return { done, stop };
}

// Embeds the current version of each document the events name. A repeated or outdated event finds
// nothing pending, so replays need no bookkeeping, and a burst of events costs one model call.
export const embedder =
  (db: pg.Pool, embed: Embed, log: Log): Handler =>
  async (events) => {
    const ids = new Set(events.filter((e) => e.type === 'document.changed').map((e) => e.documentId));
    const docs = await pendingDocs(db, [...ids]);
    if (!docs.length) return;
    await embedDocs(db, embed, docs);
    log(`embedder: ${docs.length === 1 ? docs[0].title : `${docs.length} documents`}`);
  };

// Records each new security advisory once. Alerting isn't naturally repeatable the way embedding is,
// so the processed_events row commits in the same transaction as the alert. The optional webhook
// (Slack and Discord both accept this body) is posted inside that transaction: if it fails, nothing
// commits and the retry posts it again, so it is delivered at least once.
export const alerter =
  (db: pg.Pool, log: Log, webhook = process.env.ALERT_WEBHOOK_URL): Handler =>
  async (events) => {
    for (const event of events) {
      if (event.type !== 'document.changed' || event.source !== 'osv' || !event.inserted || event.initial) continue;
      const client = await db.connect();
      try {
        await client.query('begin');
        const fresh = await client.query(`insert into processed_events (consumer, event_key) values ('alerts', $1) on conflict do nothing`, [
          event.documentId,
        ]);
        if (fresh.rowCount) {
          const { rows } = await client.query<{ title: string; url: string | null }>('select title, url from documents where id = $1', [event.documentId]);
          const alert = await client.query<{ id: string }>('insert into alerts (document_id) values ($1) returning id', [event.documentId]);
          await notifyIds(client, 'alert', [alert.rows[0].id]);
          const text = `New advisory for ${event.feed.replace(/^npm\//, '')}: ${rows[0].title} ${rows[0].url ?? ''}`.trim();
          log(`alerts: ${text}`);
          if (webhook) {
            const res = await fetch(webhook, {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({ text, content: text }),
              signal: AbortSignal.timeout(10_000),
            });
            if (!res.ok) throw new Error(`alert webhook: HTTP ${res.status}`);
          }
        }
        await client.query('commit');
      } catch (err) {
        await client.query('rollback');
        throw err;
      } finally {
        client.release();
      }
    }
  };

// Transcribes new podcast episodes as they arrive. A feed's first sync is left to npm run transcribe,
// so adding a podcast doesn't queue its whole back catalogue. An episode that already has a
// transcript is skipped, so a repeated event costs nothing.
export const transcriber =
  (db: pg.Pool, log: Log, recognize?: Recognize): Handler =>
  async (events) => {
    for (const event of events) {
      if (event.type !== 'document.changed' || !event.inserted || event.initial) continue;
      const [episode] = await untranscribed(db, 1, [event.documentId]);
      if (!episode) continue;
      log(`transcriber: ${episode.title}`);
      const { seconds } = await transcribeEpisode(db, episode, { recognize });
      log(`transcriber: ${clock(seconds)} of ${episode.title} done`);
    }
  };
