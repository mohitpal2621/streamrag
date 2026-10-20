// What the Playwright tests run against: the real GraphQL API and live stream on a Testcontainers
// Postgres, with stand-in models so nothing is downloaded, plus a small control server the tests
// call to make a sync or an alert happen. The pipeline itself (Kafka, Redis) isn't started; the
// control server does the embedder's and the alerts consumer's part directly.
import { createServer } from 'node:http';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import type { Deps } from '../../src/agent/tools.ts';
import { createApi } from '../../src/api/graphql.ts';
import { startLive } from '../../src/api/live.ts';
import { embedPending } from '../../src/core/embed.ts';
import type { Embed, Rerank } from '../../src/core/models.ts';
import { connect, migrate, syncFeed, type Doc } from '../../src/core/store.ts';
import type { ChatOptions, Message } from '../../src/llm/openai.ts';
import { alerter } from '../../src/pipeline/consumers.ts';

const port = Number(process.env.E2E_API_PORT ?? 4100);
process.env.KAFKA_BROKERS = '127.0.0.1:1'; // nothing listens there, so the status shows the pipeline as stopped
process.env.REDIS_URL = 'redis://127.0.0.1:1';

const container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
const db = connect(container.getConnectionUri());
await migrate(db);

// Three topic axes stand in for embeddings, and the reranker favours vacuum, as in the API tests.
const embed: Embed = async (texts) =>
  texts.map((t) => {
    const v = new Array(384).fill(0);
    v[/postgres|database|vacuum/i.test(t) ? 0 : /kafka|stream/i.test(t) ? 1 : 2] = 1;
    return v;
  });
const rerank: Rerank = async (_query, texts) => texts.map((t) => (/vacuum/i.test(t) ? 10 : 0));

// Answers with a citation, and as the agent searches once and then says how many questions its
// thread has seen, which is how the tests check that a follow-up keeps the conversation.
const words = (text: string) => text.split(/(?<= )/).map((w) => ({ provider: 'stand-in', text: w }));
const gateway: Deps['gateway'] = {
  async *stream(messages: Message[], options?: ChatOptions) {
    const last = messages.at(-1);
    if (options?.tools && last?.role === 'user') {
      yield { provider: 'stand-in', text: '', calls: [{ id: 'c1', type: 'function' as const, function: { name: 'search', arguments: JSON.stringify({ query: last.content }) } }] };
    } else if (last?.role === 'tool') {
      yield* words(`The search found the vacuum settings note. This is question ${messages.filter((m) => m.role === 'user').length} in this conversation.`);
    } else {
      yield* words('Vacuum reclaims dead rows, which keeps tables small [1].');
    }
  },
};

const doc = (externalId: string, title: string, body: string, url: string | null = null): Doc => ({ externalId, title, url, body, meta: {} });
await syncFeed(db, 'rss', 'https://kb.example/feed', [
  doc('vacuum', 'Vacuum settings', 'postgres vacuum settings that keep tables small', 'https://kb.example/vacuum'),
  doc('kafka', 'Consumer groups', 'kafka consumer groups and partitions', 'https://kb.example/kafka'),
]);
await syncFeed(db, 'osv', 'npm/lodash', [doc('GHSA-seed', 'GHSA-seed: prototype pollution in lodash', 'Fixed in: 4.17.21', 'https://osv.dev/GHSA-seed')]);
await embedPending(db, embed);
await db.query(
  `insert into llm_calls (provider, model, status, latency_ms, input_tokens, output_tokens)
   values ('ollama', 'llama3.2:3b', 'ok', 41000, 1700, 120), ('ollama', 'llama3.2:3b', 'error', null, null, null)`,
);

const live = await startLive(db);
const api = createServer(createApi({ db, embed, rerank, gateway }, live));
api.listen(port);

// POST /sync {title} adds a Hacker News comment and embeds it 300 ms later, as the pipeline would.
// POST /alert {id} adds an advisory for express and raises an alert for it.
// POST /api/stop and /api/start take the API down, open streams included, and bring it back.
let n = 0;
const control = createServer(async (req, res) => {
  try {
    if (req.url === '/ready') return void res.end('ok');
    let body = '';
    for await (const part of req) body += part;
    const input = body ? JSON.parse(body) : {};
    if (req.url === '/sync') {
      await syncFeed(db, 'hackernews', 'comment', [doc(`e2e-${++n}`, `Re: ${input.title}`, `${input.title}, said a commenter.`, `https://news.example/${n}`)], false);
      setTimeout(() => embedPending(db, embed).catch(console.error), 300);
    } else if (req.url === '/api/stop') {
      api.closeAllConnections();
      await new Promise((resolve) => api.close(resolve));
    } else if (req.url === '/api/start') {
      await new Promise<void>((resolve) => api.listen(port, resolve));
    } else if (req.url === '/alert') {
      await syncFeed(db, 'osv', 'npm/express', [doc(input.id, `${input.id}: open redirect in express`, 'Fixed in: 5.0.1', `https://osv.dev/${input.id}`)]);
      const { rows } = await db.query<{ id: string }>(`select id from documents where source = 'osv' and external_id = $1`, [input.id]);
      await alerter(db, console.log, '')([
        { id: input.id, type: 'document.changed', documentId: rows[0].id, contentHash: '', source: 'osv', feed: 'npm/express', inserted: true, initial: false },
      ]);
    } else {
      res.statusCode = 404;
    }
    res.end('{}');
  } catch (err) {
    res.statusCode = 500;
    res.end(String(err));
  }
});
control.listen(port + 1, () => console.log(`e2e API at http://localhost:${port}/graphql, control at http://localhost:${port + 1}`));

const stop = async () => {
  api.closeAllConnections();
  api.close();
  control.close();
  await live.close();
  await db.end();
  await container.stop();
  process.exit(0);
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
