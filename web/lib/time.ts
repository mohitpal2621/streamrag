// A fixed locale, so the server's render and the browser's agree.
const number = new Intl.NumberFormat('en-US');
const short = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });
export const count = (n: number) => number.format(n);
export const compact = (n: number) => short.format(n);

// 1.9 s, 42 s, 3 min, 2 h
export function duration(ms: number) {
  const s = Math.max(ms, 0) / 1000;
  if (s < 10) return `${s.toFixed(1)} s`;
  if (s < 60) return `${Math.round(s)} s`;
  if (s < 600) return `${(s / 60).toFixed(1).replace('.0', '')} min`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  if (s < 86400) return `${Math.round(s / 3600)} h`;
  return `${Math.round(s / 86400)} d`;
}

export const ago = (iso: string, now: number) => (now - Date.parse(iso) < 5_000 ? 'now' : `${duration(now - Date.parse(iso)).replace('.0', '')} ago`);

export function median(values: number[]) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}
