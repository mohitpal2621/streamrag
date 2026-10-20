import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { askAgent, createAgent, type AgentEvent } from '../src/agent/graph.ts';
import type { Deps } from '../src/agent/tools.ts';
import { createApi } from '../src/api/graphql.ts';
import { createMcp } from '../src/api/mcp.ts';
import { embedPending } from '../src/core/embed.ts';
import type { Embed, Rerank } from '../src/core/models.ts';
import { connect, migrate, syncFeed } from '../src/core/store.ts';
import type { ChatOptions, Message, ToolCall } from '../src/llm/openai.ts';

let container: StartedPostgreSqlContainer;
let db: ReturnType<typeof connect>;

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
});

after(async () => {
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
