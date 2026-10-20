import { Admin, Consumer, ListOffsetTimestamps, ProduceAcks, Producer, stringSerializers } from '@platformatic/kafka';

export const brokers = () => (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(',');
export const TOPIC = 'streamrag.documents';
export const DLQ = 'streamrag.documents.dlq';

// What the outbox carries. `id` is the outbox row's id, unique per event.
export type DocumentEvent = {
  id: string;
  type: 'document.changed' | 'document.closed';
  documentId: string;
  contentHash: string;
  source: string;
  feed: string;
  inserted: boolean; // false when an existing document changed
  initial: boolean; // from the first sync of its feed
};

// Three partitions let up to three members of a consumer group share the work.
export async function ensureTopics(bootstrapBrokers = brokers()) {
  const admin = new Admin({ clientId: 'streamrag-admin', bootstrapBrokers });
  try {
    const existing = new Set(await admin.listTopics());
    const missing = [TOPIC, DLQ].filter((t) => !existing.has(t));
    if (missing.length) await admin.createTopics({ topics: missing, partitions: 3, replicas: 1 });
  } finally {
    await admin.close();
  }
}

// Idempotent, so a send retried after a lost acknowledgement doesn't duplicate the message.
export const createProducer = (bootstrapBrokers = brokers()) =>
  new Producer({ clientId: 'streamrag', bootstrapBrokers, serializers: stringSerializers, idempotent: true, acks: ProduceAcks.ALL });

// Events a consumer group hasn't processed yet: end offset minus committed offset, over all partitions.
// (The client's getLag only covers partitions the asking consumer owns, so it is no use from outside.)
export async function lag(groupId: string, topic: string, bootstrapBrokers = brokers()) {
  const consumer = new Consumer({ groupId, clientId: 'streamrag-status', bootstrapBrokers, timeout: 3_000, retries: 1 });
  try {
    const ends = (await consumer.listOffsets({ topics: [topic] })).get(topic) ?? [];
    const starts = (await consumer.listOffsets({ topics: [topic], timestamp: ListOffsetTimestamps.EARLIEST })).get(topic) ?? [];
    const committed = (await consumer.listCommittedOffsets({ topics: [{ topic, partitions: ends.map((_, p) => p) }] })).get(topic) ?? [];
    // A partition the group never committed on reports -1, so all of it is still to do.
    return ends.reduce((sum, end, p) => sum + end - ((committed[p] ?? -1n) >= 0n ? committed[p] : starts[p]), 0n);
  } finally {
    await consumer.close(true);
  }
}
