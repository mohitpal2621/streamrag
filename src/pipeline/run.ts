// npm run pipeline: the streaming side of streamrag. BullMQ syncs every source on a schedule, each
// sync writes its documents and their outbox events in one transaction, the relay publishes the
// events to Kafka, and two consumer groups embed what changed and raise advisory alerts.
// --only picks roles, so each one can run, and scale, as its own process.
import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import { connectors } from '../connectors/index.ts';
import { embed } from '../core/models.ts';
import { connect } from '../core/store.ts';
import { alerter, embedder, startConsumer } from './consumers.ts';
import { createProducer, ensureTopics } from './kafka.ts';
import { startRelay } from './relay.ts';
import { scheduleSyncs, startSyncWorker, SYNC_QUEUE, type ScheduledSource } from './scheduler.ts';

const ROLES = ['scheduler', 'relay', 'embedder', 'alerts'];
const { values } = parseArgs({ options: { only: { type: 'string' } } });
const roles = values.only?.split(',').map((r) => r.trim()) ?? ROLES;
const unknown = roles.filter((r) => !ROLES.includes(r));
if (unknown.length) {
  console.error(`unknown role ${unknown.join(', ')}; pick from ${ROLES.join(', ')}`);
  process.exit(1);
}

const log = (line: string) => console.log(`${new Date().toTimeString().slice(0, 8)} ${line}`);
const db = connect();
await ensureTopics();
const producer = createProducer();
const stops: (() => Promise<unknown>)[] = [];
const fatal = (role: string) => (err: unknown) => {
  log(`${role} stopped: ${err}`);
  process.exit(1); // let whatever runs the pipeline restart it; Kafka and the outbox replay what was in flight
};

if (roles.includes('scheduler')) {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', { maxRetriesPerRequest: null });
  redis.on('error', (err) => log(`redis: ${err.message}`)); // ioredis keeps reconnecting; this says why it has to
  const queue = new Queue(SYNC_QUEUE, { connection: redis });
  const sources: ScheduledSource[] = JSON.parse(await readFile(new URL('../../sources.json', import.meta.url), 'utf8'));
  log(`scheduler: ${await scheduleSyncs(queue, sources)} sources scheduled`);
  const worker = startSyncWorker(db, redis, connectors, log);
  stops.push(() => worker.close(), () => queue.close(), () => redis.quit());
}
if (roles.includes('relay')) stops.push(await startRelay(db, producer, log));
for (const [role, handler] of [['embedder', embedder(db, embed, log)], ['alerts', alerter(db, log)]] as const) {
  if (!roles.includes(role)) continue;
  // Embedding is cheaper per document in batches; alerts go one at a time.
  const consumer = await startConsumer(role, handler, producer, log, { batchSize: role === 'embedder' ? 64 : 1 });
  consumer.done.catch(fatal(role));
  stops.push(consumer.stop);
}
log(`pipeline running: ${roles.join(', ')}. Ctrl+C stops it.`);

let stopping = false;
const shutdown = async () => {
  if (stopping) return;
  stopping = true;
  log('stopping…');
  for (const stop of stops) await stop();
  await producer.close();
  await db.end();
};
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
