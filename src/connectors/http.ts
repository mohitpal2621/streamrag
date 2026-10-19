export async function get(url: string, init: RequestInit = {}) {
  const res = await fetch(url, {
    ...init,
    headers: { 'user-agent': 'streamrag (+https://github.com/mohitpal2621/streamrag)', ...init.headers },
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { res });
  return res;
}
