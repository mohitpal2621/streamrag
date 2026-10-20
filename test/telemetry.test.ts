import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { context, propagation, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor, type ReadableSpan } from '@opentelemetry/sdk-trace-base';
import type { Producer } from '@platformatic/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import client from 'prom-client';
import { createApi } from '../src/api/graphql.ts';
import { startLive, type Live } from '../src/api/live.ts';
import { ask } from '../src/core/answer.ts';
import { embedPending } from '../src/core/embed.ts';
import type { Embed, Rerank } from '../src/core/models.ts';
import { connect, migrate, syncFeed } from '../src/core/store.ts';
import { createGateway } from '../src/llm/gateway.ts';
import type { ChatEvent } from '../src/llm/openai.ts';
import { drainOutbox } from '../src/pipeline/relay.ts';
import { statusGauges } from '../src/pipeline/status.ts';

process.env.KAFKA_BROKERS = '127.0.0.1:1';
process.env.REDIS_URL = 'redis://127.0.0.1:1';

// What src/telemetry.ts sets up, minus the exporters: spans land in memory instead.
const spans = new InMemorySpanExporter();
trace.setGlobalTracerProvider(new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(spans)] }));
context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
propagation.setGlobalPropagator(new W3CTraceContextPropagator());

let container: StartedPostgreSqlContainer;
let db: ReturnType<typeof connect>;
let live: Live;

const fakeEmbed: Embed = async (texts) =>
  texts.map((t) => {
    const v = new Array(384).fill(0);
    v[/vacuum|postgres/i.test(t) ? 0 : 1] = 1;
    return v;
  });
const fakeRerank: Rerank = async (_query, texts) => texts.map((t) => (/vacuum/i.test(t) ? 10 : 0));
// The real gateway, over a provider that streams a fixed reply and its token counts.
const gateway = createGateway({
  providers: [{ name: 'stand-in', baseUrl: 'http://stand-in', model: 'tiny-model', rpm: Infinity }],
  connect: async function* (): AsyncGenerator<ChatEvent> {
    for (const text of ['Vacuum ', 'keeps tables small [1].']) yield { type: 'token', text };
    yield { type: 'usage', inputTokens: 120, outputTokens: 9 };
  },
});

before(async () => {
  container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
  db = connect(container.getConnectionUri());
  await migrate(db);
  live = await startLive(db, () => {});
});

after(async () => {
  await live?.close();
  await db?.end();
  await container?.stop();
});

const named = (name: string) => spans.getFinishedSpans().filter((s) => s.name === name);
const parentOf = (s: ReadableSpan) => s.parentSpanContext?.spanId;

test('an answer is one trace: the search and the model call nest under it, with GenAI attributes', async () => {
  await syncFeed(db, 'kb', 'notes', [{ externalId: 'vacuum', title: 'Vacuum settings', url: 'https://kb.example/vacuum', body: 'postgres vacuum keeps tables small', meta: {} }]);
  await embedPending(db, fakeEmbed);
  spans.reset();

  for await (const _ of ask(db, 'how do I keep postgres tables small?', { gateway, embed: fakeEmbed, rerank: fakeRerank }));

  const [answer] = named('answer');
  const [search] = named('search');
  const [chat] = named('chat tiny-model');
  assert.equal(answer.instrumentationScope.name, 'streamrag.llm');
  assert.equal(parentOf(search), answer.spanContext().spanId);
  assert.equal(parentOf(chat), answer.spanContext().spanId);
  assert.equal(parentOf(named('rerank')[0]), search.spanContext().spanId);
  assert.equal(chat.spanContext().traceId, answer.spanContext().traceId);
  assert.equal(answer.attributes['input.value'], 'how do I keep postgres tables small?');
  assert.equal(answer.attributes['output.value'], 'Vacuum keeps tables small [1].');
  assert.equal(answer.attributes['answer.cached'], false);
  assert.equal(chat.attributes['gen_ai.request.model'], 'tiny-model');
  assert.equal(chat.attributes['gen_ai.provider.name'], 'stand-in');
  assert.equal(chat.attributes['gen_ai.usage.input_tokens'], 120);
  assert.equal(chat.attributes['gen_ai.usage.output_tokens'], 9);
  assert.equal(chat.attributes['gen_ai.completion'], 'Vacuum keeps tables small [1].');
  assert.match(String(chat.attributes['gen_ai.prompt']), /how do I keep postgres tables small\?/);
});

test('a sync puts its trace in each outbox event, and the relay links back to it', async () => {
  spans.reset();
  await syncFeed(db, 'hackernews', 'comment', [{ externalId: 'c1', title: 'Re: vacuum', url: null, body: 'vacuum full locks the table', meta: {} }], false);
  const [sync] = named('sync');
  assert.equal(sync.attributes['sync.inserted'], 1);
  const { rows } = await db.query<{ payload: { traceparent?: string } }>(`select payload from outbox where payload->>'feed' = 'comment'`);
  assert.equal(rows[0].payload.traceparent, `00-${sync.spanContext().traceId}-${sync.spanContext().spanId}-01`);

  const sent: unknown[] = [];
  const producer = { send: async (batch: { messages: unknown[] }) => void sent.push(...batch.messages) } as unknown as Producer<string, string, string, string>;
  await drainOutbox(db, producer);
  const [relay] = named('relay publish');
  assert.ok(sent.length >= 1);
  assert.ok(relay.links.some((l) => l.context.spanId === sync.spanContext().spanId));
});

test('metrics count searches, syncs, embeddings, answers and GraphQL operations, and read the backlog', async () => {
  statusGauges(db);
  const api = createApi({ db, embed: fakeEmbed, rerank: fakeRerank, gateway }, live);
  const res = await api.fetch('http://streamrag.test/graphql', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ query: '{ search(query: "vacuum", mode: vector, limit: 1) { title } }' }),
  });
  assert.equal(res.status, 200);

  const text = await client.register.metrics();
  const value = (series: string) => Number(text.match(new RegExp(`^${series.replace(/[{}"]/g, '\\$&')} (\\S+)$`, 'm'))?.[1]);
  assert.equal(value('streamrag_search_seconds_count{mode="vector"}'), 1);
  assert.equal(value('streamrag_graphql_seconds_count{operation="search",type="query"}'), 1);
  assert.equal(value('streamrag_documents_synced_total{source="kb",change="added"}'), 1);
  assert.equal(value('streamrag_documents_embedded_total'), 1);
  assert.equal(value('streamrag_answers_total{cached="false"}'), 1);
  assert.equal(value('streamrag_documents'), 2);
  assert.equal(value('streamrag_pending_embeddings'), 1); // the comment synced in the test before
  assert.equal(value('streamrag_up{service="kafka"}'), 0);
  assert.equal(value('streamrag_up{service="redis"}'), 0);
  assert.ok(!/^streamrag_consumer_lag/m.test(text), 'no lag is reported while Kafka is unreachable');
});
