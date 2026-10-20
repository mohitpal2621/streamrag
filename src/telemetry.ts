// Loaded before everything else (node --import ./src/telemetry.ts, as the npm scripts do). It starts
// OpenTelemetry tracing only when there is somewhere to send traces:
//   OTEL_EXPORTER_OTLP_ENDPOINT                 every span, to Jaeger or any OTLP collector
//   LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY  answers, the agent and model calls, to Langfuse
// Without either it does nothing.
import { basename } from 'node:path';
import type { SpanProcessor } from '@opentelemetry/sdk-trace-base';

const otlp = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
const langfuse = process.env.LANGFUSE_PUBLIC_KEY && process.env.LANGFUSE_SECRET_KEY;

if (otlp || langfuse) {
  // ES modules can only be instrumented through this loader hook, registered before they load.
  const { register } = await import('node:module');
  register('@opentelemetry/instrumentation/hook.mjs', import.meta.url);

  const { diag, DiagConsoleLogger, DiagLogLevel } = await import('@opentelemetry/api');
  const { NodeSDK } = await import('@opentelemetry/sdk-node');
  const { BatchSpanProcessor } = await import('@opentelemetry/sdk-trace-base');
  const { OTLPTraceExporter } = await import('@opentelemetry/exporter-trace-otlp-proto');
  const { resourceFromAttributes } = await import('@opentelemetry/resources');
  const { HttpInstrumentation } = await import('@opentelemetry/instrumentation-http');
  const { UndiciInstrumentation } = await import('@opentelemetry/instrumentation-undici');
  const { PgInstrumentation } = await import('@opentelemetry/instrumentation-pg');
  const { IORedisInstrumentation } = await import('@opentelemetry/instrumentation-ioredis');

  diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.WARN); // to stderr, which keeps the MCP server's stdout clean
  // Metrics go to Prometheus through prom-client (src/metrics.ts), and there are no OTel logs.
  process.env.OTEL_METRICS_EXPORTER ??= 'none';
  process.env.OTEL_LOGS_EXPORTER ??= 'none';

  const processors: SpanProcessor[] = [];
  if (otlp) processors.push(new BatchSpanProcessor(new OTLPTraceExporter()));
  if (langfuse) {
    const base = process.env.LANGFUSE_BASE_URL ?? 'http://localhost:3030';
    const auth = Buffer.from(`${process.env.LANGFUSE_PUBLIC_KEY}:${process.env.LANGFUSE_SECRET_KEY}`).toString('base64');
    const exporter = new OTLPTraceExporter({
      url: `${base}/api/public/otel/v1/traces`,
      headers: { authorization: `Basic ${auth}`, 'x-langfuse-ingestion-version': '4' },
    });
    // Only the model work: database queries and syncs would bury the generations.
    const inner = new BatchSpanProcessor(exporter);
    processors.push({
      onStart: () => {},
      onEnd: (span) => {
        if (span.instrumentationScope.name === 'streamrag.llm') inner.onEnd(span);
      },
      forceFlush: () => inner.forceFlush(),
      shutdown: () => inner.shutdown(),
    });
  }

  const entry = basename(process.argv[1] ?? '', '.ts');
  const service = process.env.OTEL_SERVICE_NAME ?? `streamrag-${{ serve: 'api', run: 'pipeline' }[entry] ?? entry}`;
  // Background polling (BullMQ's Redis commands, the relay's outbox checks) would be a trace every
  // few seconds, so queries, Redis commands and fetches are only traced inside an operation.
  const sdk = new NodeSDK({
    resource: resourceFromAttributes({ 'service.name': service }),
    spanProcessors: processors,
    instrumentations: [
      new HttpInstrumentation({ ignoreIncomingRequestHook: (req) => req.url === '/metrics' }),
      new UndiciInstrumentation({ requireParentforSpans: true }),
      new PgInstrumentation({ requireParentSpan: true }),
      new IORedisInstrumentation({ requireParentSpan: true }),
    ],
  });
  sdk.start();

  // Short commands exit as soon as their work is done; send what they traced first.
  let flushed = false;
  process.on('beforeExit', async () => {
    if (flushed) return;
    flushed = true;
    await sdk.shutdown();
  });
}
