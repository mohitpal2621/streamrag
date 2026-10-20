// Run by telemetry-export.test.ts under node --import ./src/telemetry.ts: one span from each tracer.
import { llmTracer, tracer } from '../../src/tracing.ts';

tracer.startSpan('sync').end();
llmTracer.startSpan('chat tiny-model', { attributes: { 'gen_ai.request.model': 'tiny-model' } }).end();
