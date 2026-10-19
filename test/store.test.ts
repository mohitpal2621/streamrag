import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { ingest } from '../src/core/ingest.ts';
import { connect, migrate, search, syncFeed, type Doc } from '../src/core/store.ts';

let container: StartedPostgreSqlContainer;
let db: ReturnType<typeof connect>;

before(async () => {
  container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
  db = connect(container.getConnectionUri());
  await migrate(db);
  await migrate(db); // must be safe to run again
});

after(async () => {
  await db?.end();
  await container?.stop();
});

const doc = (id: string, body = `body ${id}`): Doc => ({ externalId: id, title: `Doc ${id}`, url: null, body, meta: {} });
const openFeeds = async () =>
  (await db.query<{ feed: string }>('select feed from documents where closed_at is null order by feed, external_id')).rows.map((r) => r.feed);

test('snapshot sync inserts, updates only what changed, closes what vanished and reopens what returns', async () => {
  assert.deepEqual(await syncFeed(db, 'test', 'acme', [doc('1'), doc('2'), doc('2')]), { inserted: 2, updated: 0, unchanged: 0, closed: 0 });
  assert.deepEqual(await syncFeed(db, 'test', 'acme', [doc('1'), doc('2', 'new body')]), { inserted: 0, updated: 1, unchanged: 1, closed: 0 });
  assert.deepEqual(await syncFeed(db, 'test', 'acme', [doc('2', 'new body')]), { inserted: 0, updated: 0, unchanged: 1, closed: 1 });
  assert.deepEqual(await syncFeed(db, 'test', 'acme', [doc('1'), doc('2', 'new body')]), { inserted: 0, updated: 0, unchanged: 2, closed: 0 });
  assert.deepEqual(await openFeeds(), ['acme', 'acme']);
});

test('a feed only closes its own documents', async () => {
  await syncFeed(db, 'test', 'other', [doc('1')]);
  await syncFeed(db, 'test', 'acme', []);
  assert.deepEqual(await openFeeds(), ['other']);
});

test('append-only sync keeps items open after they roll off the feed', async () => {
  await syncFeed(db, 'test', 'blog', [doc('a'), doc('b')], false);
  assert.deepEqual(await syncFeed(db, 'test', 'blog', [doc('c')], false), { inserted: 1, updated: 0, unchanged: 0, closed: 0 });
  assert.deepEqual((await openFeeds()).filter((f) => f === 'blog'), ['blog', 'blog', 'blog']);
});

test('a failed fetch keeps the feed open', async () => {
  await syncFeed(db, 'test', 'flaky', [doc('9')]);
  const failing = {
    snapshot: true,
    fetchFeed: async (): Promise<Doc[]> => {
      throw new Error('HTTP 503');
    },
  };
  const results = await ingest(db, [{ connector: 'test', feed: 'flaky' }, { connector: 'nope', feed: 'x' }], { test: failing }, 0);
  assert.deepEqual(results.map((r) => ('error' in r ? r.error : 'ok')), ['Error: HTTP 503', 'Error: unknown connector "nope"']);
  assert.ok((await openFeeds()).includes('flaky'));
});

test('search ranks open documents and skips closed ones', async () => {
  await syncFeed(db, 'test', 'acme', [doc('k1', 'We run Kafka and Postgres at scale'), doc('k2', 'Frontend role with React')]);
  await syncFeed(db, 'test', 'gone', [doc('k3', 'Kafka streaming')]);
  await syncFeed(db, 'test', 'gone', []);
  const hits = await search(db, 'kafka', 10);
  assert.deepEqual(hits.map((h) => h.feed), ['acme']);
  assert.match(hits[0].snippet, /\*\*Kafka\*\*/);
});
