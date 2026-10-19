import type { Provider } from './openai.ts';

// Ollama runs on your own machine, so it goes first and needs no key. Gemini and Groq join only when
// their free-tier keys are set; with billing left off on those accounts they can never charge you.
// rpm is the free-tier request limit per minute, kept on the low side.
export function providersFromEnv(env: NodeJS.ProcessEnv = process.env): Provider[] {
  const all: Record<string, Provider | null> = {
    ollama: { name: 'ollama', baseUrl: env.OLLAMA_URL ?? 'http://localhost:11434/v1', model: env.OLLAMA_MODEL ?? 'llama3.2:3b', rpm: Infinity },
    gemini: env.GEMINI_API_KEY
      ? { name: 'gemini', baseUrl: 'https://generativelanguage.googleapis.com/v1beta/openai', model: env.GEMINI_MODEL ?? 'gemini-3.8-flash', apiKey: env.GEMINI_API_KEY, rpm: 10 }
      : null,
    groq: env.GROQ_API_KEY
      ? { name: 'groq', baseUrl: 'https://api.groq.com/openai/v1', model: env.GROQ_MODEL ?? 'openai/gpt-oss-120b', apiKey: env.GROQ_API_KEY, rpm: 30 }
      : null,
  };
  return (env.LLM_PROVIDERS ?? 'ollama,gemini,groq').split(',').flatMap((name) => {
    const key = name.trim();
    if (!(key in all)) throw new Error(`LLM_PROVIDERS has an unknown provider "${key}"; use ollama, gemini or groq`);
    return all[key] ?? [];
  });
}
