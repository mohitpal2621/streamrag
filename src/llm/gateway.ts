import { HttpError, streamChat, type ChatEvent, type Message, type Provider } from './openai.ts';

export type CallLog = {
  provider: string;
  model: string;
  status: 'ok' | 'error' | 'skipped';
  error?: string;
  latencyMs?: number;
  inputTokens?: number;
  outputTokens?: number;
};

type Options = {
  providers: Provider[];
  connect?: (p: Provider, messages: Message[]) => AsyncIterable<ChatEvent>;
  onCall?: (log: CallLog) => void;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  random?: () => number;
};

const ATTEMPTS = 3; // per provider, before moving on to the next one
const BREAKER_FAILURES = 3; // failed calls in a row that take a provider out of rotation
const BREAKER_COOLDOWN = 30_000;
const MAX_WAIT = 2_000; // a longer wait for a rate-limit slot means trying the next provider instead
const MAX_RETRY_AFTER = 10_000; // a free tier asking for longer than this is out of quota for now

// Connection refused or a bad hostname means the provider is down or misconfigured: move on.
// A reset connection, a 429 or a 5xx is usually brief: retry.
const RETRYABLE_NETWORK = new Set(['ECONNRESET', 'EPIPE', 'ETIMEDOUT', 'UND_ERR_SOCKET']);
function isRetryable(err: unknown) {
  if (err instanceof HttpError) return err.status === 429 || err.status >= 500;
  const code = (err as { cause?: { code?: string } })?.cause?.code;
  return code !== undefined && RETRYABLE_NETWORK.has(code);
}

// fetch puts the useful part of a network error ("connect ECONNREFUSED ...") in err.cause.
function describe(err: unknown) {
  const cause = (err as { cause?: Error })?.cause?.message;
  return cause ? `${err} (${cause})` : String(err);
}

export type Gateway = ReturnType<typeof createGateway>;

export function createGateway(o: Options) {
  const connect = o.connect ?? streamChat;
  const sleep = o.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const now = o.now ?? Date.now;
  const random = o.random ?? Math.random;
  // In-process state: fine for a CLI and one server process; several processes need it shared.
  const slots = o.providers.map((p) => ({ p, failures: 0, openUntil: 0, recent: [] as number[] }));

  // Yields the answer's tokens and the provider writing them, trying providers in order. Switching
  // provider is only safe before the first token; after that the answer would repeat itself, so a
  // failure goes to the caller.
  async function* stream(messages: Message[]): AsyncGenerator<{ provider: string; text: string }> {
    const reasons: string[] = [];
    for (const slot of slots) {
      const { p } = slot;
      const skip = (why: string) => {
        reasons.push(`${p.name}: ${why}`);
        o.onCall?.({ provider: p.name, model: p.model, status: 'skipped', error: why });
      };

      if (slot.openUntil > now()) {
        skip('circuit open after repeated failures');
        continue;
      }
      // Sliding one-minute window of request start times, to stay under the provider's free-tier limit.
      slot.recent = slot.recent.filter((at) => at > now() - 60_000);
      if (slot.recent.length >= p.rpm) {
        const wait = slot.recent[0] + 60_000 - now();
        if (wait > MAX_WAIT) {
          skip(`rate limited for another ${Math.ceil(wait / 1000)}s`);
          continue;
        }
        await sleep(wait);
      }

      for (let attempt = 1; ; attempt++) {
        slot.recent.push(now());
        const started = now();
        let emitted = false;
        let usage = { inputTokens: 0, outputTokens: 0 };
        try {
          for await (const ev of connect(p, messages)) {
            if (ev.type === 'usage') {
              usage = { inputTokens: ev.inputTokens, outputTokens: ev.outputTokens };
            } else {
              emitted = true;
              yield { provider: p.name, text: ev.text };
            }
          }
          slot.failures = 0;
          o.onCall?.({ provider: p.name, model: p.model, status: 'ok', latencyMs: now() - started, ...usage });
          return;
        } catch (err) {
          o.onCall?.({ provider: p.name, model: p.model, status: 'error', error: describe(err), latencyMs: now() - started });
          if (emitted) throw err;
          const retryAfterMs = err instanceof HttpError ? err.retryAfterMs : null;
          if (attempt < ATTEMPTS && isRetryable(err) && (retryAfterMs ?? 0) <= MAX_RETRY_AFTER) {
            // Full jitter: a random wait under an exponentially growing cap, so clients that failed
            // together don't all retry together. Retry-After, when given, is the floor.
            await sleep(Math.max(retryAfterMs ?? 0, random() * Math.min(8_000, 500 * 2 ** attempt)));
            continue;
          }
          // A provider still failing after its cooldown reopens the circuit straight away.
          slot.failures++;
          if (slot.failures >= BREAKER_FAILURES) slot.openUntil = now() + BREAKER_COOLDOWN;
          reasons.push(`${p.name}: ${describe(err)}`);
          break;
        }
      }
    }
    throw new Error(`no provider could answer (${reasons.join('; ') || 'none configured'})`);
  }

  return { stream };
}
