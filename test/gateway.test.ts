import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createGateway, type CallLog } from '../src/llm/gateway.ts';
import { HttpError, streamChat, type ChatEvent, type Message, type Provider } from '../src/llm/openai.ts';
import { sseData } from '../src/llm/sse.ts';

const provider = (name: string, rpm = Infinity): Provider => ({ name, baseUrl: `http://${name}.test`, model: `${name}-model`, rpm });
const messages: Message[] = [{ role: 'user', content: 'hi' }];

// Each provider answers its calls in order from a script: tokens, an error, or tokens and then an error.
type Step = string[] | Error | { thenFail: string[]; error: Error };
function setup(names: Provider[], script: Record<string, Step[]>) {
  let clock = 0;
  const waits: number[] = [];
  const calls: string[] = [];
  const logs: CallLog[] = [];
  const gateway = createGateway({
    providers: names,
    random: () => 0.5,
    now: () => clock,
    sleep: async (ms) => {
      waits.push(ms);
      clock += ms;
    },
    onCall: (log) => logs.push(log),
    connect: async function* (p): AsyncGenerator<ChatEvent> {
      calls.push(p.name);
      const step = script[p.name]?.shift();
      if (!step) throw new Error(`no more scripted replies for ${p.name}`);
      if (step instanceof Error) throw step;
      for (const text of Array.isArray(step) ? step : step.thenFail) yield { type: 'token', text };
      if (!Array.isArray(step)) throw step.error;
      yield { type: 'usage', inputTokens: 10, outputTokens: step.length };
    },
  });
  const answer = async () => {
    let text = '';
    let by = '';
    for await (const t of gateway.stream(messages)) {
      text += t.text;
      by = t.provider;
    }
    return `${by}: ${text}`;
  };
  return { answer, calls, waits, logs, gateway, advance: (ms: number) => (clock += ms) };
}

const unavailable = () => new HttpError(503, null, 'overloaded');

test('falls back to the next provider when one keeps failing before it answers', async () => {
  const g = setup([provider('a'), provider('b')], { a: [unavailable(), unavailable(), unavailable()], b: [['hi', ' there']] });
  assert.equal(await g.answer(), 'b: hi there');
  assert.deepEqual(g.calls, ['a', 'a', 'a', 'b']);
  assert.deepEqual(g.waits, [500, 1000]); // full jitter at 0.5 of a 1 s, then 2 s cap
  assert.deepEqual(g.logs.at(-1), { provider: 'b', model: 'b-model', status: 'ok', latencyMs: 0, inputTokens: 10, outputTokens: 2 });
});

test('waits at least as long as Retry-After asks', async () => {
  const g = setup([provider('a')], { a: [new HttpError(429, 3000, ''), ['ok']] });
  assert.equal(await g.answer(), 'a: ok');
  assert.deepEqual(g.waits, [3000]);
});

test('moves on at once from errors that retrying will not fix', async () => {
  const g = setup([provider('a'), provider('b')], { a: [new HttpError(401, null, 'bad key')], b: [['ok']] });
  assert.equal(await g.answer(), 'b: ok');
  assert.deepEqual(g.calls, ['a', 'b']);
  assert.deepEqual(g.waits, []);
});

test('skips a provider whose free quota is gone for a while', async () => {
  const g = setup([provider('a'), provider('b')], { a: [new HttpError(429, 60_000, 'daily limit')], b: [['ok']] });
  assert.equal(await g.answer(), 'b: ok');
  assert.deepEqual(g.waits, []);
});

test('never switches provider once the answer has started', async () => {
  const g = setup([provider('a'), provider('b')], { a: [{ thenFail: ['Hel'], error: unavailable() }], b: [['should not appear']] });
  let text = '';
  await assert.rejects(async () => {
    for await (const t of g.gateway.stream(messages)) text += t.text;
  }, /HTTP 503/);
  assert.equal(text, 'Hel');
  assert.deepEqual(g.calls, ['a']);
});

test('opens the circuit after three failed calls and tries again after the cooldown', async () => {
  const badKey = () => new HttpError(401, null, '');
  const g = setup([provider('a'), provider('b')], { a: [badKey(), badKey(), badKey(), badKey()], b: [['1'], ['2'], ['3'], ['4'], ['5']] });
  for (let i = 0; i < 4; i++) await g.answer();
  assert.deepEqual(g.calls, ['a', 'b', 'a', 'b', 'a', 'b', 'b']); // the fourth call skips a
  assert.equal(g.logs.filter((l) => l.status === 'skipped').length, 1);
  g.advance(30_001);
  assert.equal(await g.answer(), 'b: 5');
  assert.deepEqual(g.calls.slice(-2), ['a', 'b']); // a gets a trial again, fails, circuit reopens
});

test('sends overflow past the per-minute limit to the next provider', async () => {
  const g = setup([provider('a', 2), provider('b')], { a: [['1'], ['2']], b: [['3']] });
  assert.deepEqual([await g.answer(), await g.answer(), await g.answer()], ['a: 1', 'a: 2', 'b: 3']);
});

test('says why each provider failed when none can answer', async () => {
  const refused = new TypeError('fetch failed', { cause: Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:11434'), { code: 'ECONNREFUSED' }) });
  const g = setup([provider('a'), provider('b')], { a: [refused], b: [unavailable(), unavailable(), unavailable()] });
  await assert.rejects(g.answer(), /a: TypeError: fetch failed \(connect ECONNREFUSED 127\.0\.0\.1:11434\); b: Error: HTTP 503: overloaded/);
  assert.deepEqual(g.calls, ['a', 'b', 'b', 'b']); // a refused connection is not worth retrying
});

test('sseData reassembles lines and characters split across network chunks', async () => {
  const bytes = new TextEncoder().encode('data: {"a":"héllo"}\n\ndata: [DONE]\n');
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      for (const part of [bytes.slice(0, 14), bytes.slice(14, 30), bytes.slice(30)]) c.enqueue(part); // splits é in two
      c.close();
    },
  });
  const out: string[] = [];
  for await (const data of sseData(body)) out.push(data);
  assert.deepEqual(out, ['{"a":"héllo"}', '[DONE]']);
});

test('streamChat reads tokens and token usage from an OpenAI-style stream', async (t) => {
  const stream = [
    'data: {"choices":[{"delta":{"content":"Hel"}}]}',
    'data: {"choices":[{"delta":{"content":"lo"}}]}',
    'data: {"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":2}}',
    'data: [DONE]',
  ].join('\n\n');
  t.mock.method(globalThis, 'fetch', async () => new Response(`${stream}\n\n`, { status: 200 }));
  const events: ChatEvent[] = [];
  for await (const ev of streamChat(provider('x'), messages)) events.push(ev);
  assert.deepEqual(events, [
    { type: 'token', text: 'Hel' },
    { type: 'token', text: 'lo' },
    { type: 'usage', inputTokens: 12, outputTokens: 2 },
  ]);
});

test('streamChat turns a 429 into an error that carries Retry-After', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => new Response('slow down', { status: 429, headers: { 'retry-after': '7' } }));
  await assert.rejects(
    async () => {
      for await (const _ of streamChat(provider('x'), messages));
    },
    (err) => err instanceof HttpError && err.status === 429 && err.retryAfterMs === 7000,
  );
});
