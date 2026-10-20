import { context, propagation, ROOT_CONTEXT, SpanStatusCode, trace, type Attributes, type Context, type Link, type Span, type Tracer } from '@opentelemetry/api';

// Spans for the work streamrag does. They cost next to nothing until src/telemetry.ts starts the
// OpenTelemetry SDK. Model work gets its own tracer, which is what Langfuse is sent.
export const tracer = trace.getTracer('streamrag');
export const llmTracer = trace.getTracer('streamrag.llm');

function fail(span: Span, err: unknown) {
  span.recordException(err instanceof Error ? err : String(err));
  span.setStatus({ code: SpanStatusCode.ERROR, message: err instanceof Error ? err.message : String(err) });
}

// Runs fn in a new active span, ended whether fn returns or throws.
export function traced<T>(t: Tracer, name: string, attributes: Attributes, fn: (span: Span) => Promise<T>, links: Link[] = []): Promise<T> {
  return t.startActiveSpan(name, { attributes, links }, async (span) => {
    try {
      return await fn(span);
    } catch (err) {
      fail(span, err);
      throw err;
    } finally {
      span.end();
    }
  });
}

// An async generator's body runs in the context of whoever calls next(), not of whoever created it.
// This drives one inside ctx, so the spans it starts (database queries, fetches) nest where they belong.
export async function* inContext<T>(ctx: Context, source: AsyncIterable<T>): AsyncGenerator<T> {
  const it = source[Symbol.asyncIterator]();
  try {
    for (;;) {
      const r = await context.with(ctx, () => it.next());
      if (r.done) return;
      yield r.value;
    }
  } finally {
    await context.with(ctx, async () => {
      await it.return?.();
    });
  }
}

// A span around a stream, from the first value to the last, ended on an error or an early return too.
export async function* tracedStream<T>(t: Tracer, name: string, attributes: Attributes, source: (span: Span) => AsyncIterable<T>): AsyncGenerator<T> {
  const span = t.startSpan(name, { attributes });
  try {
    yield* inContext(trace.setSpan(context.active(), span), source(span));
  } catch (err) {
    fail(span, err);
    throw err;
  } finally {
    span.end();
  }
}

// The active span as a W3C traceparent, so a trace can follow a change through the outbox and Kafka.
export function traceparent(): string | undefined {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent;
}

// Links from a batch's span to the spans that produced its events.
export function linksTo(traceparents: (string | undefined)[]): Link[] {
  return traceparents.flatMap((tp) => {
    const span = tp ? trace.getSpanContext(propagation.extract(ROOT_CONTEXT, { traceparent: tp })) : undefined;
    return span ? [{ context: span }] : [];
  });
}
