import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { embedPending } from '../src/core/embed.ts';
import { ingest } from '../src/core/ingest.ts';
import type { Embed, Rerank } from '../src/core/models.ts';
import { search } from '../src/core/search.ts';
import { connect, migrate, syncFeed, type Doc } from '../src/core/store.ts';

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

const doc = (id: string, body = `body ${id}`, title = `Doc ${id}`): Doc => ({ externalId: id, title, url: null, body, meta: {} });
const openFeeds = async () =>
  (await db.query<{ feed: string }>('select feed from documents where closed_at is null order by feed, external_id')).rows.map((r) => r.feed);

describe('sync', () => {
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
});

// Stand-ins for the real models, so the tests need no downloads. The embedder puts each text on
// one of three axes by topic; the reranker prefers anything that mentions vacuum.
const fakeEmbed: Embed = async (texts) =>
  texts.map((t) => {
    const v = new Array(384).fill(0);
    v[/postgres|database|vacuum/i.test(t) ? 0 : /kafka|stream/i.test(t) ? 1 : 2] = 1;
    return v;
  });
const fakeRerank: Rerank = async (_query, texts) => texts.map((t) => (/vacuum/i.test(t) ? 10 : 0));
const find = async (query: string, mode: 'keyword' | 'vector' | 'hybrid' | 'rerank') =>
  (await search(db, query, { mode, limit: 10, embed: fakeEmbed, rerank: fakeRerank })).map((h) => h.title);

describe('embedding and search', () => {
  before(async () => {
    await db.query('truncate documents cascade');
    await syncFeed(db, 'kb', 'notes', [
      doc('tuning', 'database tuning checklist for MySQL', 'Tuning checklist'),
      doc('vacuum', 'postgres vacuum settings that keep tables small', 'Vacuum settings'),
      doc('kafka', 'kafka consumer groups and partitions', 'Consumer groups'),
    ]);
  });

  test('embeds only documents that are new or changed', async () => {
    assert.equal(await embedPending(db, fakeEmbed), 3);
    assert.equal(await embedPending(db, fakeEmbed), 0);
    await syncFeed(db, 'kb', 'notes', [
      doc('tuning', 'database tuning checklist for MySQL', 'Tuning checklist'),
      doc('vacuum', 'postgres vacuum settings that keep tables small and fast', 'Vacuum settings'),
      doc('kafka', 'kafka consumer groups and partitions', 'Consumer groups'),
    ]);
    assert.equal(await embedPending(db, fakeEmbed), 1);
  });

  test('keyword search needs the words; vector search finds the meaning', async () => {
    assert.deepEqual(await find('database tuning', 'keyword'), ['Tuning checklist']);
    // Any of the words counts, and chunks with more of them rank higher.
    assert.deepEqual(await find('database tuning for kafka', 'keyword'), ['Tuning checklist', 'Consumer groups']);
    const byMeaning = await find('database tuning', 'vector');
    assert.deepEqual(byMeaning.slice(0, 2).sort(), ['Tuning checklist', 'Vacuum settings']);
    assert.equal(byMeaning[2], 'Consumer groups');
  });

  test('hybrid search ranks what both legs agree on first', async () => {
    assert.deepEqual(await find('database tuning', 'hybrid'), ['Tuning checklist', 'Vacuum settings', 'Consumer groups']);
  });

  test('the reranker reorders the fused results', async () => {
    assert.deepEqual(await find('database tuning', 'rerank'), ['Vacuum settings', 'Tuning checklist', 'Consumer groups']);
  });

  test('a long document shows up once, and closed documents not at all', async () => {
    const long = `postgres ${'word '.repeat(450)}`;
    await syncFeed(db, 'kb', 'notes', [doc('long', long, 'Long postgres guide'), doc('kafka', 'kafka consumer groups and partitions', 'Consumer groups')]);
    await embedPending(db, fakeEmbed);
    const titles = await find('postgres', 'vector');
    assert.equal(titles.filter((t) => t === 'Long postgres guide').length, 1);
    assert.ok(!titles.includes('Tuning checklist') && !titles.includes('Vacuum settings'));
  });
});
