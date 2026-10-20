import { createServer, type ServerResponse } from 'node:http';
import client from 'prom-client';

// Prometheus metrics. They are counted in whichever process does the work and served at /metrics
// by the API, and by the pipeline on PIPELINE_METRICS_PORT.

client.collectDefaultMetrics(); // CPU, memory, event loop lag, GC

export const searchSeconds = new client.Histogram({
  name: 'streamrag_search_seconds',
  help: 'Search latency by mode',
  labelNames: ['mode'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
});

export const rerankSkipped = new client.Counter({
  name: 'streamrag_rerank_skipped_total',
  help: 'Rerank searches that kept the hybrid order because the reranker was busy',
});

export const llmCallSeconds = new client.Histogram({
  name: 'streamrag_llm_call_seconds',
  help: 'Model call latency by provider, model and outcome (ok or error)',
  labelNames: ['provider', 'model', 'status'],
  buckets: [0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300],
});

export const llmTokens = new client.Counter({
  name: 'streamrag_llm_tokens_total',
  help: 'Tokens sent to and received from models',
  labelNames: ['provider', 'model', 'direction'],
});

export const llmSkips = new client.Counter({
  name: 'streamrag_llm_skips_total',
  help: 'Providers skipped by the circuit breaker or the rate limiter',
  labelNames: ['provider', 'model'],
});

export const answers = new client.Counter({
  name: 'streamrag_answers_total',
  help: 'Answers, by whether the semantic cache served them',
  labelNames: ['cached'],
});

export const documentsSynced = new client.Counter({
  name: 'streamrag_documents_synced_total',
  help: 'Documents a sync added, changed or closed',
  labelNames: ['source', 'change'],
});

export const documentsEmbedded = new client.Counter({
  name: 'streamrag_documents_embedded_total',
  help: 'Documents chunked and embedded',
});

export const embedSeconds = new client.Histogram({
  name: 'streamrag_embed_batch_seconds',
  help: 'Time to chunk, embed and store one batch of documents',
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
});

export const eventsPublished = new client.Counter({
  name: 'streamrag_events_published_total',
  help: 'Outbox events the relay published to Kafka',
});

export const eventsHandled = new client.Counter({
  name: 'streamrag_events_handled_total',
  help: 'Events a consumer group handled, by outcome (ok, retried or dead_lettered)',
  labelNames: ['consumer', 'outcome'],
});

export const graphqlSeconds = new client.Histogram({
  name: 'streamrag_graphql_seconds',
  help: 'GraphQL query time by operation; subscriptions are counted when they finish',
  labelNames: ['operation', 'type'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120],
});

export async function sendMetrics(res: ServerResponse) {
  try {
    const body = await client.register.metrics();
    res.writeHead(200, { 'content-type': client.register.contentType }).end(body);
  } catch (err) {
    res.writeHead(500).end(String(err));
  }
}

// For processes without an HTTP server of their own.
export function serveMetrics(port: number, log: (line: string) => void) {
  const server = createServer((req, res) => (req.url === '/metrics' ? sendMetrics(res) : res.writeHead(404).end()));
  server.on('error', (err) => log(`metrics: can't listen on port ${port} (${err.message})`));
  server.listen(port);
  return server;
}
