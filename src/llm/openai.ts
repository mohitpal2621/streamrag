import { sseData } from './sse.ts';

export type ToolCall = { id: string; type: 'function'; function: { name: string; arguments: string } };
export type Message =
  | { role: 'system' | 'user'; content: string }
  | { role: 'assistant'; content: string; tool_calls?: ToolCall[] }
  | { role: 'tool'; content: string; tool_call_id: string };
export type Provider = { name: string; baseUrl: string; model: string; apiKey?: string; rpm: number };
export type ChatEvent =
  | { type: 'token'; text: string }
  | { type: 'tool_calls'; calls: ToolCall[] }
  | { type: 'usage'; inputTokens: number; outputTokens: number };

export class HttpError extends Error {
  status: number;
  retryAfterMs: number | null;
  constructor(status: number, retryAfterMs: number | null, detail: string) {
    super(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
    this.status = status;
    this.retryAfterMs = retryAfterMs;
  }
}

// Retry-After comes as seconds or as an HTTP date.
function retryAfter(header: string | null): number | null {
  if (!header) return null;
  const seconds = Number(header);
  if (Number.isFinite(seconds)) return seconds * 1000;
  const date = Date.parse(header);
  return Number.isNaN(date) ? null : Math.max(0, date - Date.now());
}

// Extra request fields: response_format for JSON output, tools for tool calling, a temperature.
export type ChatOptions = Record<string, unknown>;

// Streams a chat completion from an OpenAI-compatible endpoint. Ollama, Gemini and Groq all offer
// one, so a single client covers every provider.
export async function* streamChat(p: Provider, messages: Message[], options: ChatOptions = {}): AsyncGenerator<ChatEvent> {
  const res = await fetch(`${p.baseUrl}/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(p.apiKey ? { authorization: `Bearer ${p.apiKey}` } : {}) },
    body: JSON.stringify({ model: p.model, messages, stream: true, stream_options: { include_usage: true }, temperature: 0.2, ...options }),
    signal: AbortSignal.timeout(180_000),
  });
  if (!res.ok || !res.body) {
    // Error bodies are often pretty-printed JSON; one line reads better in a log.
    throw new HttpError(res.status, retryAfter(res.headers.get('retry-after')), (await res.text()).replace(/\s+/g, ' ').slice(0, 200));
  }
  // Tool calls stream in fragments: the first one for a call carries its id and name, the rest add
  // to its arguments. They go out whole, once the reply is complete.
  const calls: ToolCall[] = [];
  for await (const data of sseData(res.body)) {
    if (data === '[DONE]') break;
    const chunk = JSON.parse(data);
    const delta = chunk.choices?.[0]?.delta;
    if (delta?.content) yield { type: 'token', text: delta.content };
    for (const part of delta?.tool_calls ?? []) {
      const index = part.index ?? calls.length;
      const call = (calls[index] ??= { id: '', type: 'function', function: { name: '', arguments: '' } });
      call.id ||= part.id ?? `call_${index}`;
      call.function.name += part.function?.name ?? '';
      call.function.arguments += part.function?.arguments ?? '';
    }
    if (chunk.usage) yield { type: 'usage', inputTokens: chunk.usage.prompt_tokens ?? 0, outputTokens: chunk.usage.completion_tokens ?? 0 };
  }
  if (calls.length) yield { type: 'tool_calls', calls };
}
