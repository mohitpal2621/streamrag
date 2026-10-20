import { setTimeout as sleep } from 'node:timers/promises';
import { Consumer, MessagesStreamFallbackModes, MessagesStreamModes, stringDeserializers, type Producer } from '@platformatic/kafka';
import { brokers, DLQ, TOPIC } from './kafka.ts';

export type DeadLetter = { offset: string; consumer: string; error: string; source: string; event: string };

// Reads the dead-letter topic until it goes quiet. Listing always starts from the beginning. Replay
// sends each event back to the main topic and commits, as its own consumer group, so after a bug fix
// every dead-lettered event is retried exactly once more.
export async function deadLetters(producer: Producer<string, string, string, string> | null, { quietMs = 2_000, bootstrapBrokers = brokers() } = {}) {
  const consumer = new Consumer({
    groupId: producer ? 'streamrag-dlq-replay' : 'streamrag-dlq-list',
    clientId: 'streamrag-dlq',
    bootstrapBrokers,
    deserializers: stringDeserializers,
    autocommit: false,
  });
  const found: DeadLetter[] = [];
  try {
    const stream = await consumer.consume({
      topics: [DLQ],
      mode: producer ? MessagesStreamModes.COMMITTED : MessagesStreamModes.EARLIEST,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    });
    const it = stream[Symbol.asyncIterator]();
    for (;;) {
      const next = await Promise.race([it.next(), sleep(quietMs).then(() => null)]);
      if (!next || next.done) break;
      const m = next.value;
      found.push({
        offset: `${m.partition}/${m.offset}`,
        consumer: m.headers.get('consumer') ?? '',
        error: m.headers.get('error') ?? '',
        source: m.headers.get('source') ?? '',
        event: m.value,
      });
      if (producer) {
        await producer.send({ messages: [{ topic: TOPIC, key: m.key, value: m.value }] });
        await m.commit();
      }
    }
    await stream.close();
  } finally {
    await consumer.close(true);
  }
  return found;
}
