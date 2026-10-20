import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { askAgent, createAgent, type AgentEvent } from '../src/agent/graph.ts';
import type { Deps } from '../src/agent/tools.ts';
import { createApi } from '../src/api/graphql.ts';
import { startLive, type Activity, type Live } from '../src/api/live.ts';
import { createMcp } from '../src/api/mcp.ts';
import { embedPending } from '../src/core/embed.ts';
import type { Embed, Rerank } from '../src/core/models.ts';
import { connect, migrate, syncFeed } from '../src/core/store.ts';
import type { ChatOptions, Message, ToolCall } from '../src/llm/openai.ts';
import { alerter } from '../src/pipeline/consumers.ts';

// Nothing listens on port 1, so status and feeds see Kafka and Redis as down, the same on every machine.
process.env.KAFKA_BROKERS = '127.0.0.1:1';
process.env.REDIS_URL = 'redis://127.0.0.1:1';

let container: StartedPostgreSqlContainer;
let db: ReturnType<typeof connect>;
let live: Live;

// The same stand-ins as the store tests: three topic axes for embeddings, and a reranker that likes vacuum.
const fakeEmbed: Embed = async (texts) =>
  texts.map((t) => {
    const v = new Array(384).fill(0);
    v[/postgres|database|vacuum/i.test(t) ? 0 : /kafka|stream/i.test(t) ? 1 : 2] = 1;
    return v;
  });
const fakeRerank: Rerank = async (_query, texts) => texts.map((t) => (/vacuum/i.test(t) ? 10 : 0));

before(async () => {
  container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
  db = connect(container.getConnectionUri());
  await migrate(db);
  await syncFeed(db, 'kb', 'notes', [
    { externalId: 'vacuum', title: 'Vacuum settings', url: 'https://kb.example/vacuum', body: 'postgres vacuum settings that keep tables small', meta: {} },
    { externalId: 'kafka', title: 'Consumer groups', url: 'https://kb.example/kafka', body: 'kafka consumer groups and partitions', meta: {} },
  ]);
  await embedPending(db, fakeEmbed);
  live = await startLive(db, () => {});
});

after(async () => {
  await live?.close();
  await db?.end();
  await container?.stop();
});

// A model that replies from a script, with text or with tool calls, and records what it was sent.
function scripted(replies: ({ text: string } | { calls: ToolCall[] })[]) {
  const g = {
    asked: [] as { messages: Message[]; options?: ChatOptions }[],
    async *stream(messages: Message[], options?: ChatOptions) {
      g.asked.push({ messages: structuredClone(messages), options });
      const reply = replies.shift();
      if (!reply) throw new Error('the script ran out of replies');
      if ('calls' in reply) yield { provider: 'fake', text: '', calls: reply.calls };
      else for (const word of reply.text.split(' ')) yield { provider: 'fake', text: `${word} ` };
    },
  };
  return g;
}
const call = (id: string, name: string, args: object): ToolCall => ({ id, type: 'function', function: { name, arguments: JSON.stringify(args) } });
const deps = (gateway: Deps['gateway']): Deps => ({ db, embed: fakeEmbed, rerank: fakeRerank, gateway });
const collect = async (events: AsyncIterable<AgentEvent>) => {
  const out: AgentEvent[] = [];
  for await (const e of events) out.push(e);
  return out;
};

test('the agent calls a tool, answers from its result, and a thread remembers the conversation', async () => {
  const g = scripted([
    { calls: [call('c1', 'search', { query: 'keep postgres tables small', limit: '2' })] }, // a string limit, the way small models send it
    { text: 'Vacuum keeps tables small.' },
    { text: 'You asked about postgres tables.' },
  ]);
  const agent = createAgent(deps(g));
  const events = await collect(askAgent(agent, 'how do I keep postgres tables small?', 'thread-1'));
  assert.deepEqual(events[0], { type: 'tool', name: 'search', arguments: '{"query":"keep postgres tables small","limit":"2"}' });
  assert.equal(events.filter((e) => e.type === 'token').map((e) => (e.type === 'token' ? e.text : '')).join(''), 'Vacuum keeps tables small. ');
  assert.equal((g.asked[0].options?.tools as unknown[]).length, 3);
  const result = g.asked[1].messages.find((m) => m.role === 'tool');
  assert.match(result?.content ?? '', /\[1\] Vacuum settings\nhttps:\/\/kb\.example\/vacuum/);

  await collect(askAgent(agent, 'what did I just ask?', 'thread-1'));
  assert.ok(g.asked[2].messages.some((m) => m.role === 'user' && m.content.includes('keep postgres tables small')));
});

test('a failing tool call goes back to the model, and after three rounds it has to answer', async () => {
  const g = scripted([
    { calls: [call('c1', 'no_such_tool', {})] },
    { calls: [call('c2', 'search', {})] },
    { calls: [call('c3', 'search', { query: 'vacuum' })] },
    { text: 'Vacuum, it seems.' },
    { text: 'A new question gets the tools back.' },
  ]);
  const agent = createAgent(deps(g));
  const events = await collect(askAgent(agent, 'anything?', 'thread-2'));
  const results = g.asked[3].messages.filter((m) => m.role === 'tool').map((m) => m.content);
  assert.match(results[0], /^error: there is no tool called no_such_tool/);
  assert.match(results[1], /^error: .*\n.*at query/); // zod says which argument is missing
  assert.match(results[2], /Vacuum settings/);
  assert.equal(g.asked[3].options?.tools, undefined); // tools withheld on the last round
  assert.equal(events.filter((e) => e.type === 'tool').length, 3);
  await collect(askAgent(agent, 'and now?', 'thread-2'));
  assert.equal((g.asked[4].options?.tools as unknown[]).length, 3); // the cap counts per question, not per thread
});

test('GraphQL serves search and streams ask and the agent as server-sent events', async () => {
  const api = createApi(
    deps(scripted([{ text: 'Vacuum keeps tables small [1].' }, { calls: [call('c1', 'search', { query: 'vacuum' })] }, { text: 'Use vacuum.' }])),
    live,
  );
  const post = (body: object, accept = 'application/json') =>
    api.fetch('http://streamrag.test/graphql', { method: 'POST', headers: { 'content-type': 'application/json', accept }, body: JSON.stringify(body) });

  const found = await (await post({ query: '{ search(query: "vacuum", mode: vector, limit: 1) { title url source snippet } }' })).json();
  assert.deepEqual(found.data.search, [{ title: 'Vacuum settings', url: 'https://kb.example/vacuum', source: 'kb', snippet: 'postgres vacuum settings that keep tables small' }]);

  const stream = await post(
    { query: 'subscription { ask(question: "how do I keep postgres tables small") { __typename ... on AnswerToken { text } ... on AnswerDone { cited cached } } }' },
    'text/event-stream',
  );
  assert.match(stream.headers.get('content-type') ?? '', /text\/event-stream/);
  const events = (await stream.text())
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => JSON.parse(line.slice(6)).data?.ask)
    .filter(Boolean);
  assert.equal(events[0].__typename, 'AnswerSources');
  assert.equal(events.filter((e) => e.__typename === 'AnswerToken').length, 5);
  assert.deepEqual(events.at(-1), { __typename: 'AnswerDone', cited: [1], cached: false });

  const agent = await post(
    { query: 'subscription { agent(question: "vacuum?") { __typename ... on AgentToolCall { name } ... on AgentToken { text } } }' },
    'text/event-stream',
  );
  const steps = (await agent.text())
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => JSON.parse(line.slice(6)).data?.agent)
    .filter(Boolean);
  assert.deepEqual(steps, [
    { __typename: 'AgentToolCall', name: 'search' },
    { __typename: 'AgentToken', text: 'Use ' },
    { __typename: 'AgentToken', text: 'vacuum. ' },
  ]);
});

test('MCP offers the three tools and turns a failure into a tool error', async () => {
  const [clientSide, serverSide] = InMemoryTransport.createLinkedPair();
  const server = createMcp(deps(scripted([])));
  await server.connect(serverSide);
  const client = new Client({ name: 'test', version: '1.0.0' });
  await client.connect(clientSide);
  try {
    assert.deepEqual((await client.listTools()).tools.map((t) => t.name).sort(), ['breaking_changes', 'search', 'security_advisories']);
    const found = await client.callTool({ name: 'search', arguments: { query: 'vacuum', limit: 1 } });
    assert.match((found.content as { text: string }[])[0].text, /^\[1\] Vacuum settings/);
    const missing = await client.callTool({ name: 'search', arguments: {} });
    assert.equal(missing.isError, true);
  } finally {
    await client.close();
    await server.close();
  }
});

// The events a server-sent event stream carries for one subscription field, as they arrive.
async function* sse(res: Response, field: string) {
  const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += value;
      for (let end = buffer.indexOf('\n\n'); end >= 0; end = buffer.indexOf('\n\n')) {
        const data = buffer.slice(0, end).split('\n').find((line) => line.startsWith('data: '));
        buffer = buffer.slice(end + 2);
        const event = data && JSON.parse(data.slice(6)).data?.[field];
        if (event) yield event;
      }
    }
  } finally {
    await reader.cancel();
  }
}

const comment = (id: string, body: string) => ({ externalId: id, title: `Re: ${body}`, url: `https://news.example/${id}`, body, meta: {} });
const advisory = (id: string) => ({ externalId: id, title: `${id}: prototype pollution`, url: `https://osv.dev/${id}`, body: 'Fixed in: 4.17.21', meta: {} });

test('live turns each commit into activity: changes, when they become searchable, and new alerts', async () => {
  const events = live.subscribe()[Symbol.asyncIterator]();
  const next = async () => (await events.next()).value as Activity;
  try {
    const first = next(); // subscribes before anything commits
    await syncFeed(db, 'hackernews', 'comments', [comment('c1', 'vacuum full locks the table')], false);
    const added = await first;
    assert.ok(added.type === 'document');
    assert.equal(added.change, 'added');
    assert.equal(added.searchableAfterMs, null);
    assert.deepEqual(added.document, {
      id: added.document.id,
      source: 'hackernews',
      feed: 'comments',
      title: 'Re: vacuum full locks the table',
      url: 'https://news.example/c1',
      snippet: 'vacuum full locks the table',
      searchable: false,
    });

    await embedPending(db, fakeEmbed);
    const searchable = await next();
    assert.ok(searchable.type === 'searchable');
    assert.deepEqual(searchable.documentIds, [added.document.id]);

    // An edit is a change, and a snapshot feed closes what it stops listing.
    await syncFeed(db, 'hackernews', 'comments', [comment('c1', 'vacuum full rewrites the table')], false);
    const changed = await next();
    assert.ok(changed.type === 'document' && changed.change === 'changed' && changed.document.title === 'Re: vacuum full rewrites the table');
    await syncFeed(db, 'kb', 'drafts', [comment('d1', 'draft one'), comment('d2', 'draft two')]);
    const drafts = [await next(), await next()];
    assert.deepEqual(drafts.map((e) => e.type === 'document' && e.document.title), ['Re: draft one', 'Re: draft two']); // in commit order
    await syncFeed(db, 'kb', 'drafts', [comment('d2', 'draft two')]);
    const closed = await next();
    assert.ok(closed.type === 'document' && closed.change === 'closed' && closed.document.title === 'Re: draft one');

    // The alerts consumer is fed by Kafka in the pipeline; here it gets the event directly.
    await syncFeed(db, 'osv', 'npm/lodash', [advisory('GHSA-1')]);
    const osv = await next();
    assert.ok(osv.type === 'document');
    await alerter(db, () => {}, '')([
      { id: '1', type: 'document.changed', documentId: osv.document.id, contentHash: 'h', source: 'osv', feed: 'npm/lodash', inserted: true, initial: false },
    ]);
    const alert = await next();
    assert.ok(alert.type === 'alert');
    assert.deepEqual({ ...alert.alert, id: undefined, at: undefined }, {
      id: undefined,
      at: undefined,
      package: 'lodash',
      title: 'GHSA-1: prototype pollution',
      url: 'https://osv.dev/GHSA-1',
    });
  } finally {
    await events.return?.();
  }
});

test('live listens again after its database connection drops', async () => {
  const events = live.subscribe()[Symbol.asyncIterator]();
  try {
    const first = events.next();
    await db.query("select pg_terminate_backend(pid) from pg_stat_activity where query like 'listen outbox%'");
    // Commits while it is reconnecting are missed (clients reload recent activity), so keep syncing until one arrives.
    let n = 0;
    const arrived = Promise.race([first.then(() => true), sleep(15_000).then(() => false)]);
    while (!(await Promise.race([arrived, sleep(500).then(() => null)]))) {
      if (n > 30) break;
      await syncFeed(db, 'hackernews', 'comments', [comment(`r${n++}`, 'after the restart')], false);
    }
    assert.ok(await arrived, 'no activity after the connection dropped');
    assert.equal(((await first).value as Activity).type, 'document');
  } finally {
    await events.return?.();
  }
});

test('GraphQL streams activity and answers the dashboard queries', async () => {
  const api = createApi(deps(scripted([])), live);
  const post = (query: string, accept = 'application/json', signal?: AbortSignal) =>
    api.fetch('http://streamrag.test/graphql', { method: 'POST', headers: { 'content-type': 'application/json', accept }, body: JSON.stringify({ query }), signal });

  const controller = new AbortController();
  const stream = sse(
    await post(
      `subscription { activity { __typename ... on DocumentActivity { change document { title searchable } } ... on Searchable { documentIds } } }`,
      'text/event-stream',
      controller.signal,
    ),
    'activity',
  );
  const first = stream.next();
  await sleep(100); // the subscription starts listening once Yoga begins reading it, just after the response
  await syncFeed(db, 'hackernews', 'comments', [comment('g1', 'autovacuum tuning')], false);
  assert.deepEqual((await first).value, { __typename: 'DocumentActivity', change: 'added', document: { title: 'Re: autovacuum tuning', searchable: false } });
  await embedPending(db, fakeEmbed);
  assert.equal((await stream.next()).value.__typename, 'Searchable');
  controller.abort();
  await stream.return(undefined);

  await db.query(
    `insert into llm_calls (provider, model, status, latency_ms, input_tokens, output_tokens) values
     ('ollama', 'llama3.2:3b', 'ok', 100, 1000, 50), ('ollama', 'llama3.2:3b', 'ok', 200, 1000, 50),
     ('ollama', 'llama3.2:3b', 'ok', 300, 1000, 50), ('ollama', 'llama3.2:3b', 'error', null, null, null)`,
  );
  const { data, errors } = await (
    await post(`{
      status { documents chunks outbox pendingEmbeddings lag { group } kafkaError syncs { scheduled } redisError }
      recent(limit: 2) { change searchableAfterMs document { title searchable } }
      pulse(minutes: 5) { source documents }
      feeds { source feed documents nextSyncAt }
      usage { provider ok errors avgMs p95Ms inputTokens outputTokens }
    }`)
  ).json();
  assert.equal(errors, undefined);
  const open = (await db.query('select count(*)::int as n from documents where closed_at is null')).rows[0].n;
  assert.equal(data.status.documents, open);
  assert.ok(data.status.chunks > 0);
  assert.equal(data.status.pendingEmbeddings, 0);
  assert.equal(data.status.lag, null);
  assert.match(data.status.kafkaError, /127\.0\.0\.1:1/);
  assert.equal(data.status.syncs, null);
  assert.ok(data.status.redisError);
  const { searchableAfterMs, ...latest } = data.recent[0];
  assert.deepEqual(latest, { change: 'added', document: { title: 'Re: autovacuum tuning', searchable: true } });
  assert.ok(searchableAfterMs >= 0 && searchableAfterMs < 60_000, `searchableAfterMs ${searchableAfterMs}`);
  assert.ok(data.pulse.some((b: { source: string; documents: number }) => b.source === 'hackernews' && b.documents > 0));
  assert.deepEqual(
    data.feeds.find((f: { feed: string }) => f.feed === 'drafts'),
    { source: 'kb', feed: 'drafts', documents: 1, nextSyncAt: null },
  );
  assert.deepEqual(data.usage, [{ provider: 'ollama', ok: 3, errors: 1, avgMs: 200, p95Ms: 290, inputTokens: 3000, outputTokens: 150 }]);
});
