import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { ask, type AnswerEvent } from '../src/core/answer.ts';
import { embedPending } from '../src/core/embed.ts';
import { ingest } from '../src/core/ingest.ts';
import type { Embed, Rerank } from '../src/core/models.ts';
import { search } from '../src/core/search.ts';
import { connect, migrate, syncFeed, type Doc } from '../src/core/store.ts';
import type { Message } from '../src/llm/openai.ts';
import { breakingChanges } from '../src/upgrade.ts';

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

  test('a bare identifier keeps the hybrid order instead of the reranker’s', async () => {
    await syncFeed(db, 'kb', 'advisories', [doc('cve', 'fixes CVE-2025-1234 in the parser', 'Advisory')]);
    await embedPending(db, fakeEmbed);
    assert.equal((await find('CVE-2025-1234', 'rerank'))[0], 'Advisory');
  });

  // Answers with a fixed reply, one word per token, and counts how often it is asked.
  const fakeGateway = (reply: string) => {
    const gateway = {
      calls: 0,
      async *stream(_messages: Message[]) {
        gateway.calls++;
        for (const word of reply.split(' ')) yield { provider: 'fake', text: `${word} ` };
      },
    };
    return gateway;
  };
  const askAll = async (question: string, gateway: ReturnType<typeof fakeGateway>) => {
    const events: AnswerEvent[] = [];
    for await (const ev of ask(db, question, { gateway, embed: fakeEmbed, rerank: fakeRerank })) events.push(ev);
    const done = events.at(-1);
    assert.ok(done?.type === 'done');
    return { done, tokens: events.filter((e) => e.type === 'token').length };
  };

  test('ask streams an answer, keeps real citations and drops invented ones', async () => {
    const gateway = fakeGateway('Vacuum keeps tables small [1]. See also [9].');
    const { done, tokens } = await askAll('how do I keep postgres tables small', gateway);
    assert.equal(tokens, 8);
    assert.deepEqual([done.cited, done.invalid, done.cached, done.provider], [[1], [9], false, 'fake']);
  });

  test('a repeated question comes from the cache until a cited document changes', async () => {
    const gateway = fakeGateway('Cached? [1]');
    assert.equal((await askAll('how do I keep postgres tables small', gateway)).done.cached, true);
    assert.equal(gateway.calls, 0);
    await syncFeed(db, 'kb', 'notes', [
      doc('tuning', 'database tuning checklist for MySQL', 'Tuning checklist'),
      doc('vacuum', 'postgres vacuum settings, rewritten', 'Vacuum settings'),
      doc('kafka', 'kafka consumer groups and partitions', 'Consumer groups'),
    ]);
    await embedPending(db, fakeEmbed);
    assert.equal((await askAll('how do I keep postgres tables small', gateway)).done.cached, false);
    assert.equal(gateway.calls, 1);
  });

  test('an answer without citations is not cached', async () => {
    const gateway = fakeGateway('The sources do not say.');
    await askAll('what does kafka do with offsets', gateway);
    await askAll('what does kafka do with offsets', gateway);
    assert.equal(gateway.calls, 2);
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

describe('breaking changes', () => {
  // npm lists 1.1.0 and 2.0.0 as the minor and major after 1.0.0. The releases feed only has 2.0.0,
  // so 1.1.0 comes from GitHub's API, where the project tagged it without a v.
  const routes: Record<string, () => Response> = {
    'https://registry.npmjs.org/widget': () =>
      Response.json({ 'dist-tags': { latest: '2.0.1' }, versions: { '1.0.0': {}, '1.0.1': {}, '1.1.0': {}, '2.0.0-beta.1': {}, '2.0.0': {}, '2.0.1': {} } }),
    'https://registry.npmjs.org/widget/latest': () => Response.json({ repository: { url: 'git+https://github.com/acme/widget.git' } }),
    'https://github.com/acme/widget/releases.atom': () =>
      new Response(`<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><entry>
        <id>tag:github.com,2008:Repository/1/v2.0.0</id>
        <link rel="alternate" href="https://github.com/acme/widget/releases/tag/v2.0.0"/>
        <title>v2.0.0</title><content type="html">&lt;p&gt;Drops Node 16.&lt;/p&gt;</content></entry></feed>`),
    'https://api.github.com/repos/acme/widget/releases/tags/1.1.0': () =>
      Response.json({ tag_name: '1.1.0', name: '1.1.0', body: 'Renames the `size` option to `width`.', html_url: 'https://github.com/acme/widget/releases/tag/1.1.0', published_at: null }),
  };
  const requested: string[] = [];
  const fakeFetch = async (input: string | URL | Request) => {
    const url = String(input);
    requested.push(url);
    return routes[url]?.() ?? new Response('not found', { status: 404 });
  };
  // Lists a breaking change for any notes that mention one, the way a model would.
  const extractor = () => {
    const g = {
      calls: 0,
      async *stream(messages: Message[]) {
        g.calls++;
        const notes = messages.at(-1)?.content ?? '';
        const breaking = [
          ...(/Node 16/.test(notes) ? [{ change: 'Node 16 is no longer supported', migration: 'Use Node 18 or later' }] : []),
          ...(/Renames/.test(notes) ? [{ change: 'The size option is now called width', migration: '' }] : []),
        ];
        yield { provider: 'fake', text: JSON.stringify({ breaking }) };
      },
    };
    return g;
  };

  test('reads each release once, from the feed or the API, and keeps what it found', async (t) => {
    t.mock.method(globalThis, 'fetch', fakeFetch);
    const g = extractor();
    const first = await breakingChanges(db, g, 'widget', '1.0.0');
    assert.deepEqual(first, {
      repo: 'acme/widget',
      to: '2.0.1',
      releases: [
        { version: '1.1.0', url: 'https://github.com/acme/widget/releases/tag/1.1.0', short: true, changes: [{ change: 'The size option is now called width', migration: '' }] },
        { version: '2.0.0', url: 'https://github.com/acme/widget/releases/tag/v2.0.0', short: true, changes: [{ change: 'Node 16 is no longer supported', migration: 'Use Node 18 or later' }] },
      ],
    });
    assert.equal(g.calls, 2);
    const api = 'https://api.github.com/repos/acme/widget/releases/tags';
    assert.deepEqual(requested.filter((u) => u.startsWith(api)), [`${api}/v1.1.0`, `${api}/1.1.0`]); // 2.0.0 came from the feed

    requested.length = 0;
    assert.deepEqual(await breakingChanges(db, g, 'widget', '1.0.0'), first);
    assert.equal(g.calls, 2); // nothing changed, so nothing is read again
    assert.ok(!requested.some((u) => u.startsWith('https://api.github.com/')));
  });

  test('edited notes get read again', async (t) => {
    t.mock.method(globalThis, 'fetch', fakeFetch);
    const g = extractor();
    await syncFeed(db, 'github', 'acme/widget', [
      { externalId: '1.1.0', title: '1.1.0', url: 'https://github.com/acme/widget/releases/tag/1.1.0', body: 'Nothing breaks after all.', meta: { published: null } },
    ], false);
    const { releases } = await breakingChanges(db, g, 'widget', '1.0.0', '1.1.0');
    assert.deepEqual(releases, [{ version: '1.1.0', url: 'https://github.com/acme/widget/releases/tag/1.1.0', short: true, changes: [] }]);
    assert.equal(g.calls, 1);
  });

  test('a used-up GitHub API limit is reported for the releases it kept out, after one request', async (t) => {
    t.mock.method(globalThis, 'fetch', async (input: string | URL | Request) => {
      const url = String(input);
      if (url === 'https://registry.npmjs.org/widget') return Response.json({ 'dist-tags': { latest: '5.0.0' }, versions: { '3.0.0': {}, '4.0.0': {}, '5.0.0': {} } });
      if (!url.startsWith('https://api.github.com/')) return fakeFetch(input);
      requested.push(url);
      if (url.includes('3.0.0')) return new Response('not found', { status: 404 }); // never released on GitHub
      return new Response('{"message":"API rate limit exceeded"}', { status: 403, headers: { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': '1800000000' } });
    });
    requested.length = 0;
    const { releases } = await breakingChanges(db, extractor(), 'widget', '2.0.0');
    assert.deepEqual(releases.map((r) => [r.version, r.changes, r.error?.replace(/at .+\)/, 'at …)')]), [
      ['3.0.0', undefined, undefined],
      ['4.0.0', undefined, "GitHub's API limit for this hour is used up (it resets at …); a GITHUB_TOKEN raises it"],
      ['5.0.0', undefined, "GitHub's API limit for this hour is used up (it resets at …); a GITHUB_TOKEN raises it"],
    ]);
    assert.equal(requested.filter((u) => u.startsWith('https://api.github.com/')).length, 4); // three tags for 3.0.0, one for 4.0.0
  });
});
