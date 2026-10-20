// A GraphQL query over fetch, and subscriptions read as server-sent events, which is how the
// streamrag API streams them.

export async function query<T>(url: string, document: string, variables: Record<string, unknown> = {}, init: RequestInit = {}): Promise<T> {
  const res = await fetch(url, {
    ...init,
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ query: document, variables }),
  });
  const body = (await res.json().catch(() => null)) as { data?: T; errors?: { message: string }[] } | null;
  if (body?.errors?.length) throw new Error(body.errors.map((e) => e.message).join('; '));
  if (!res.ok || !body?.data) throw new Error(`the API answered HTTP ${res.status}`);
  return body.data;
}

export async function* subscribe<T>(
  url: string,
  document: string,
  variables: Record<string, unknown>,
  { signal, onOpen }: { signal?: AbortSignal; onOpen?: () => void } = {},
): AsyncGenerator<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    body: JSON.stringify({ query: document, variables }),
    signal,
  });
  if (!res.ok || !res.body) throw new Error(`the API answered HTTP ${res.status}`);
  onOpen?.();
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += value;
      // Events end with a blank line; a line starting with ':' is a keep-alive comment.
      for (let end = buffer.indexOf('\n\n'); end >= 0; end = buffer.indexOf('\n\n')) {
        const event = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const data = event
          .split('\n')
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trimStart())
          .join('\n');
        if (!data) continue;
        const message = JSON.parse(data) as { data?: T; errors?: { message: string }[] };
        if (message.errors?.length) throw new Error(message.errors.map((e) => e.message).join('; '));
        if (message.data) yield message.data;
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}
