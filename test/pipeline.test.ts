import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { Consumer, MessagesStreamModes, stringDeserializers, type Message, type Producer } from '@platformatic/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { RedpandaContainer, type StartedRedpandaContainer } from '@testcontainers/redpanda';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import type { Embed } from '../src/core/models.ts';
import { connect, migrate, syncFeed, type Doc } from '../src/core/store.ts';
import { alerter, embedder, startConsumer, transcriber, type Handler } from '../src/pipeline/consumers.ts';
import { deadLetters } from '../src/pipeline/dlq.ts';
import { createProducer, ensureTopics, lag, TOPIC } from '../src/pipeline/kafka.ts';
import { drainOutbox, startRelay } from '../src/pipeline/relay.ts';
import { scheduleSyncs, startSyncWorker, SYNC_QUEUE, withLock } from '../src/pipeline/scheduler.ts';

let pg: StartedPostgreSqlContainer;
let kafka: StartedRedpandaContainer;
let redisContainer: StartedRedisContainer;
let db: ReturnType<typeof connect>;
let brokers: string[];
let redis: Redis;
let producer: Producer<string, string, string, string>;

before(async () => {
  [pg, kafka, redisContainer] = await Promise.all([
    new PostgreSqlContainer('pgvector/pgvector:pg16').start(),
    new RedpandaContainer('docker.redpanda.com/redpandadata/redpanda:v26.2.5').start(),
    new RedisContainer('redis:8.8-alpine').start(),
  ]);
  db = connect(pg.getConnectionUri());
  await migrate(db);
  brokers = [kafka.getBootstrapServers()];
  await ensureTopics(brokers);
  producer = createProducer(brokers);
  redis = new Redis(redisContainer.getConnectionUrl(), { maxRetriesPerRequest: null });
});

after(async () => {
  await producer?.close();
  redis?.disconnect();
  await db?.end();
  await Promise.all([pg?.stop(), kafka?.stop(), redisContainer?.stop()]);
});

const doc = (id: string, body = `body ${id}`): Doc => ({ externalId: id, title: `Doc ${id}`, url: null, body, meta: {} });
const quiet = () => {};
const fakeEmbed: Embed = async (texts) => texts.map(() => new Array(384).fill(0.05));
const outboxEvents = async (feed: string) =>
  (await db.query<{ id: string; payload: Record<string, unknown> }>(`select id, payload from outbox where payload->>'feed' = $1 order by id`, [feed])).rows;
const chunksIn = async (feed: string) =>
  (await db.query('select 1 from chunks c join documents d on d.id = c.document_id where d.feed = $1', [feed])).rowCount ?? 0;
// Sends one outbox row to Kafka again, the way a relay that crashed before marking it would.
const resend = (row: { id: string; payload: Record<string, unknown> }) =>
  producer.send({ messages: [{ topic: TOPIC, key: String(row.payload.documentId), value: JSON.stringify({ ...row.payload, id: row.id }) }] });

async function eventually<T>(check: () => Promise<T>, what: string, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(200);
  }
}
const caughtUp = (name: string) => eventually(async () => (await lag(`streamrag-${name}`, TOPIC, brokers)) === 0n, `${name} to catch up`);

async function readTopic(topic: string, count: number) {
  const consumer = new Consumer({ groupId: `test-read-${Date.now()}`, clientId: 'test', bootstrapBrokers: brokers, deserializers: stringDeserializers, autocommit: false });
  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST });
  const messages: Message<string, string, string, string>[] = [];
  for await (const m of stream) {
    messages.push(m);
    if (messages.length >= count) break;
  }
  await consumer.close(true);
  return messages;
}

test('a sync writes one outbox event per document it adds, changes or closes, or none if it fails', async () => {
  await syncFeed(db, 'test', 'outbox', [doc('a'), doc('b')]);
  await syncFeed(db, 'test', 'outbox', [doc('a'), doc('b')]); // nothing changed, so no events
  await syncFeed(db, 'test', 'outbox', [doc('b', 'edited')]); // b changed and a is gone
  const kinds = (await outboxEvents('outbox')).map((e) => [e.payload.type, e.payload.inserted, e.payload.initial]);
  assert.deepEqual(kinds, [
    ['document.changed', true, true],
    ['document.changed', true, true],
    ['document.changed', false, false],
    ['document.closed', false, false],
  ]);
  await assert.rejects(syncFeed(db, 'test', 'outbox', [{ ...doc('c'), title: null as unknown as string }]));
  assert.equal((await outboxEvents('outbox')).length, 4); // the failed sync took its events with it
});

test('the relay publishes every outbox event to Kafka, keyed by document, and marks it published', async () => {
  const expected = await outboxEvents('outbox');
  assert.equal(await drainOutbox(db, producer), expected.length);
  assert.equal((await db.query('select 1 from outbox where published_at is null')).rowCount, 0);
  const got = (await readTopic(TOPIC, expected.length)).map((m) => ({ key: m.key, id: JSON.parse(m.value).id }));
  assert.deepEqual(
    got.sort((x, y) => Number(x.id) - Number(y.id)),
    expected.map((e) => ({ key: String(e.payload.documentId), id: e.id })),
  );
});

test('the embedder embeds a changed document once, however often its event arrives', async () => {
  let embedded = 0;
  const counting: Embed = async (texts) => {
    embedded += texts.length;
    return fakeEmbed(texts, 'passage');
  };
  const consumer = await startConsumer('embedder-test', embedder(db, counting, quiet), producer, quiet, { bootstrapBrokers: brokers });
  try {
    await syncFeed(db, 'test', 'embed', [doc('e1')]);
    await drainOutbox(db, producer);
    await eventually(() => chunksIn('embed'), 'chunks for e1');
    await caughtUp('embedder-test');
    const before = embedded;
    await resend((await outboxEvents('embed'))[0]);
    await resend((await outboxEvents('embed'))[0]);
    await caughtUp('embedder-test');
    assert.equal(embedded, before);
  } finally {
    await consumer.stop();
  }
});

test('an event that keeps failing is dead-lettered without holding up the rest, and replays once', async () => {
  await syncFeed(db, 'test', 'dlq', [doc('bad'), doc('good')]);
  const [bad, good] = (await db.query<{ id: string }>(`select id from documents where feed = 'dlq' order by external_id`)).rows.map((r) => r.id);
  const handled = new Set<string>();
  let broken = true;
  // Fails any batch holding the bad event, the way one bad document fails a whole embedding call.
  const flaky: Handler = async (events) => {
    if (broken && events.some((e) => e.documentId === bad)) throw new Error('cannot handle this one');
    for (const e of events) handled.add(e.documentId);
  };
  await drainOutbox(db, producer); // already waiting when the consumer starts, so they arrive as one batch
  const consumer = await startConsumer('flaky', flaky, producer, quiet, { bootstrapBrokers: brokers, retryDelayMs: 10, batchSize: 50 });
  try {
    await eventually(async () => handled.has(good), 'the good event');
    await caughtUp('flaky');
    const dead = (await deadLetters(null, { bootstrapBrokers: brokers })).filter((d) => JSON.parse(d.event).documentId === bad);
    assert.equal(dead.length, 1);
    assert.equal(dead[0].consumer, 'flaky');
    assert.match(dead[0].error, /cannot handle this one/);

    broken = false; // the bug is fixed
    assert.ok((await deadLetters(producer, { bootstrapBrokers: brokers })).some((d) => JSON.parse(d.event).documentId === bad));
    await eventually(async () => handled.has(bad), 'the replayed event');
    assert.equal((await deadLetters(producer, { bootstrapBrokers: brokers })).length, 0); // nothing left to replay
  } finally {
    await consumer.stop();
  }
});

test('a new advisory alerts once, through the webhook too, and a first sync alerts nothing', async () => {
  const posts: string[] = [];
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      posts.push(JSON.parse(body).text);
      res.end('ok');
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const hook = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hook`;
  const advisory = (id: string): Doc => ({ externalId: id, title: `${id}: prototype pollution`, url: `https://osv.dev/vulnerability/${id}`, body: id, meta: {} });
  const consumer = await startConsumer('alerts-test', alerter(db, quiet, hook), producer, quiet, { bootstrapBrokers: brokers });
  try {
    await syncFeed(db, 'osv', 'npm/widget', [advisory('GHSA-old')]);
    await syncFeed(db, 'osv', 'npm/widget', [advisory('GHSA-old'), advisory('GHSA-new')]);
    await drainOutbox(db, producer);
    await eventually(async () => posts.length, 'the webhook post');
    await resend((await outboxEvents('npm/widget')).find((e) => e.payload.inserted && !e.payload.initial)!);
    await caughtUp('alerts-test');
    assert.deepEqual(posts, ['New advisory for widget: GHSA-new: prototype pollution https://osv.dev/vulnerability/GHSA-new']);
    assert.equal((await db.query('select 1 from alerts')).rowCount, 1);
  } finally {
    await consumer.stop();
    server.close();
  }
});

test('a new podcast episode gets transcribed, while the back catalogue from a first sync does not', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'streamrag-podcast-'));
  const audio = join(dir, 'episode.wav');
  execFileSync('ffmpeg', ['-nostdin', '-loglevel', 'error', '-f', 'lavfi', '-i', 'sine=duration=1', audio]);
  const episode = (id: string): Doc => ({ externalId: id, title: id, url: null, body: 'show notes', meta: { published: null, audio } });
  let heard = 0;
  const consumer = await startConsumer('transcriber-test', transcriber(db, quiet, async () => (heard++, [{ start: 0, text: 'hello from the episode' }])), producer, quiet, {
    bootstrapBrokers: brokers,
  });
  try {
    await syncFeed(db, 'rss', 'https://pod.example/rss', [episode('back catalogue')], false);
    await syncFeed(db, 'rss', 'https://pod.example/rss', [episode('back catalogue'), episode('brand new')], false);
    await drainOutbox(db, producer);
    await caughtUp('transcriber-test');
    const { rows } = await db.query<{ title: string; body: string }>(`select title, body from documents where source = 'transcript'`);
    assert.deepEqual(rows, [{ title: 'brand new (transcript)', body: '[0:00] hello from the episode' }]);
    assert.equal(heard, 1);
  } finally {
    await consumer.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the Redis lock admits one holder at a time, and only the holder can release it', async () => {
  const nested = await withLock(redis, 'test:lock', 10_000, () => withLock(redis, 'test:lock', 10_000, async () => 'inside'));
  assert.deepEqual(nested, { held: false, value: { held: true } });
  assert.deepEqual(await withLock(redis, 'test:lock', 10_000, async () => 'again'), { held: false, value: 'again' });

  // A holder that outlives its TTL must not release the lock the next holder took meanwhile.
  const slow = withLock(redis, 'test:expiring', 100, () => sleep(400));
  await sleep(200);
  const stillMine = await withLock(redis, 'test:expiring', 10_000, async () => {
    await slow;
    return (await redis.exists('test:expiring')) === 1;
  });
  assert.deepEqual(stillMine, { held: false, value: true });
});

test('BullMQ schedules each source and the worker syncs it, skipping a feed another worker holds', async () => {
  const queue = new Queue(SYNC_QUEUE, { connection: redis });
  const fetched: string[] = [];
  const fake = { snapshot: false, fetchFeed: async (feed: string) => (fetched.push(feed), [doc(`${feed}-1`)]) };
  const every = 60_000;
  try {
    assert.equal(await scheduleSyncs(queue, ['one', 'two', 'held'].map((feed) => ({ connector: 'fake', feed, every }))), 3);
    assert.equal(await scheduleSyncs(queue, ['one', 'held'].map((feed) => ({ connector: 'fake', feed, every }))), 2);
    assert.equal(await queue.getJobSchedulersCount(), 2);
    await redis.set('streamrag:sync:fake:held', 'another worker', 'PX', 60_000);
    const worker = startSyncWorker(db, redis, { fake }, quiet);
    try {
      await eventually(async () => (await db.query(`select 1 from documents where source = 'fake' and feed = 'one'`)).rowCount, 'the sync of feed one');
      await eventually(async () => (await queue.getCompleted()).some((j) => j.data.feed === 'held'), 'the held feed to be skipped');
      assert.ok(!fetched.includes('held'));
    } finally {
      await worker.close();
    }
  } finally {
    await queue.obliterate({ force: true });
    await queue.close();
  }
});

test('end to end: a synced document becomes searchable through the outbox, Kafka and the embedder', async (t) => {
  const stopRelay = await startRelay(db, producer, quiet, 60_000); // a long poll, so only NOTIFY can wake it in time
  const consumer = await startConsumer('embedder-e2e', embedder(db, fakeEmbed, quiet), producer, quiet, { bootstrapBrokers: brokers });
  try {
    await caughtUp('embedder-e2e');
    const started = Date.now();
    await syncFeed(db, 'test', 'live', [doc('fresh', 'just published')]);
    await eventually(() => chunksIn('live'), 'the new document to be embedded', 10_000);
    t.diagnostic(`sync commit to embedded: ${Date.now() - started} ms`);
  } finally {
    await consumer.stop();
    await stopRelay();
  }
});
