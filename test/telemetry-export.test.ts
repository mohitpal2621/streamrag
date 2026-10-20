import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createServer, type IncomingHttpHeaders } from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import { promisify } from 'node:util';

type Received = { url: string; headers: IncomingHttpHeaders; body: string };

// A stand-in Langfuse that records what it is sent and answers the way Langfuse does, in JSON.
async function fakeLangfuse() {
  const received: Received[] = [];
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      received.push({ url: req.url ?? '', headers: req.headers, body });
      res.writeHead(200, { 'content-type': 'application/json' }).end('{"name":"otel-ingestion-job","data":{}}');
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { received, server, url: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
}

test('telemetry sends Langfuse only the model work, as OTLP JSON with the project keys', async () => {
  const langfuse = await fakeLangfuse();
  try {
    const { stderr } = await promisify(execFile)(process.execPath, ['--import', './src/telemetry.ts', 'test/fixtures/spans.ts'], {
      env: { PATH: process.env.PATH, LANGFUSE_PUBLIC_KEY: 'pk-test', LANGFUSE_SECRET_KEY: 'sk-test', LANGFUSE_BASE_URL: langfuse.url },
    });
    assert.doesNotMatch(stderr, /could not deserialize/);

    assert.equal(langfuse.received.length, 1);
    const [request] = langfuse.received;
    assert.equal(request.url, '/api/public/otel/v1/traces');
    assert.equal(request.headers.authorization, `Basic ${Buffer.from('pk-test:sk-test').toString('base64')}`);
    assert.match(request.headers['content-type'] ?? '', /^application\/json/);

    const { resourceSpans } = JSON.parse(request.body);
    const scopes = resourceSpans.flatMap((r: { scopeSpans: unknown[] }) => r.scopeSpans);
    const names = scopes.flatMap((s: { scope: { name: string }; spans: { name: string }[] }) => s.spans.map((span) => `${s.scope.name}: ${span.name}`));
    assert.deepEqual(names, ['streamrag.llm: chat tiny-model']);
  } finally {
    langfuse.server.close();
  }
});
