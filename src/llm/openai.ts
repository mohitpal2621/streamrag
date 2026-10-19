import { sseData } from './sse.ts';

export type Message = { role: 'system' | 'user' | 'assistant'; content: string };
export type Provider = { name: string; baseUrl: string; model: string; apiKey?: string; rpm: number };
export type ChatEvent = { type: 'token'; text: string } | { type: 'usage'; inputTokens: number; outputTokens: number };

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

// Streams a chat completion from an OpenAI-compatible endpoint. Ollama, Gemini and Groq all offer
// one, so a single client covers every provider.
export async function* streamChat(p: Provider, messages: Message[]): AsyncGenerator<ChatEvent> {
  const res = await fetch(`${p.baseUrl}/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(p.apiKey ? { authorization: `Bearer ${p.apiKey}` } : {}) },
    body: JSON.stringify({ model: p.model, messages, stream: true, stream_options: { include_usage: true }, temperature: 0.2 }),
    signal: AbortSignal.timeout(180_000),
  });
  if (!res.ok || !res.body) {
    // Error bodies are often pretty-printed JSON; one line reads better in a log.
    throw new HttpError(res.status, retryAfter(res.headers.get('retry-after')), (await res.text()).replace(/\s+/g, ' ').slice(0, 200));
  }
  for await (const data of sseData(res.body)) {
    if (data === '[DONE]') return;
    const chunk = JSON.parse(data);
    const text = chunk.choices?.[0]?.delta?.content;
    if (text) yield { type: 'token', text };
    if (chunk.usage) yield { type: 'usage', inputTokens: chunk.usage.prompt_tokens ?? 0, outputTokens: chunk.usage.completion_tokens ?? 0 };
  }
}
