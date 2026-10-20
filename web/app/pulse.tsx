import { SOURCES } from '../lib/sources';
import { count } from '../lib/time';
import type { PulseBucket } from '../lib/types';

export const BUCKET_MS = 10_000;
const COLUMNS = 90; // 15 minutes
const ORDER = Object.keys(SOURCES);

// Documents per 10-second bucket and source, keyed by the bucket's start in ms.
export type Buckets = Record<number, Record<string, number>>;

export function toBuckets(pulse: PulseBucket[]) {
  const buckets: Buckets = {};
  for (const p of pulse) addTo(buckets, Date.parse(p.at), p.source, p.documents);
  return buckets;
}

export function withDocument(buckets: Buckets, at: string, source: string): Buckets {
  const next = { ...buckets };
  const t = Math.floor(Date.parse(at) / BUCKET_MS) * BUCKET_MS;
  next[t] = { ...next[t] };
  addTo(next, t, source, 1);
  for (const key of Object.keys(next)) if (Number(key) < t - 2 * COLUMNS * BUCKET_MS) delete next[Number(key)];
  return next;
}

function addTo(buckets: Buckets, t: number, source: string, n: number) {
  buckets[t] ??= {};
  buckets[t][source] = (buckets[t][source] ?? 0) + n;
}

// A bar per 10 seconds, stacked by source. Hacker News is polled every minute, so its bursts set the rhythm.
export function Pulse({ buckets, now }: { buckets: Buckets; now: number }) {
  const last = Math.floor(now / BUCKET_MS) * BUCKET_MS;
  const columns = Array.from({ length: COLUMNS }, (_, i) => last - (COLUMNS - 1 - i) * BUCKET_MS);
  const perSource: Record<string, number> = {};
  for (const t of columns) for (const [s, n] of Object.entries(buckets[t] ?? {})) perSource[s] = (perSource[s] ?? 0) + n;
  const order = [...ORDER, ...Object.keys(perSource).filter((s) => !ORDER.includes(s))].filter((s) => perSource[s]);
  const total = Object.values(perSource).reduce((a, b) => a + b, 0);
  const max = Math.max(5, ...columns.map((t) => Object.values(buckets[t] ?? {}).reduce((a, b) => a + b, 0)));
  const kind = (s: string) => SOURCES[s]?.kind ?? 'blog';

  return (
    <figure className="pulse">
      <svg viewBox={`0 0 ${COLUMNS * 10} 100`} preserveAspectRatio="none" role="img" aria-label={`${count(total)} documents synced in the last 15 minutes`}>
        <line className="baseline" x1="0" x2={COLUMNS * 10} y1="99.5" y2="99.5" vectorEffect="non-scaling-stroke" />
        {columns.map((t, i) => {
          // A square-root scale, so a first sync of hundreds doesn't flatten the minute-by-minute bursts after it.
          const column = Object.values(buckets[t] ?? {}).reduce((a, b) => a + b, 0);
          const height = Math.sqrt(column / max) * 96;
          let y = 100;
          return order
            .filter((s) => buckets[t]?.[s])
            .map((s) => {
              const n = buckets[t][s];
              const h = (n / column) * height;
              y -= h;
              return (
                <rect key={`${t}-${s}`} className={`k-${kind(s)}`} x={i * 10 + 1} y={y} width={8} height={h}>
                  <title>{`${count(n)} from ${SOURCES[s]?.label ?? s}`}</title>
                </rect>
              );
            });
        })}
      </svg>
      <div className="pulse-axis" aria-hidden="true">
        <span>15 min ago</span>
        <span>10 min ago</span>
        <span>5 min ago</span>
        <span>Now</span>
      </div>
      {total ? (
        <ul className="legend">
          {order.map((s) => (
            <li key={s} className={`k-${kind(s)}`}>
              <span className="swatch" aria-hidden="true" />
              {SOURCES[s]?.label ?? s} {count(perSource[s])}
            </li>
          ))}
        </ul>
      ) : (
        <p className="legend muted">Nothing synced in the last 15 minutes.</p>
      )}
    </figure>
  );
}
