import { SOURCES } from '../lib/sources';
import { count } from '../lib/time';
import type { PulseBucket } from '../lib/types';

export const BUCKET_MS = 10_000;
const COLUMNS = 90; // 15 minutes
const ORDER = Object.keys(SOURCES);

// Documents per 10-second bucket and source, keyed by the bucket's start in ms.
export type Buckets = Record<number, Record<string, number>>;
// Documents seen arriving that aren't searchable yet, by id, with the bucket each one landed in.
export type Waiting = Record<string, number>;

export const bucketOf = (at: string) => Math.floor(Date.parse(at) / BUCKET_MS) * BUCKET_MS;

export function toBuckets(pulse: PulseBucket[]) {
  const buckets: Buckets = {};
  for (const p of pulse) addTo(buckets, Date.parse(p.at), p.source, p.documents);
  return buckets;
}

export function withDocument(buckets: Buckets, at: string, source: string): Buckets {
  const next = { ...buckets };
  const t = bucketOf(at);
  next[t] = { ...next[t] };
  addTo(next, t, source, 1);
  for (const key of Object.keys(next)) if (Number(key) < t - 2 * COLUMNS * BUCKET_MS) delete next[Number(key)];
  return next;
}

function addTo(buckets: Buckets, t: number, source: string, n: number) {
  buckets[t] ??= {};
  buckets[t][source] = (buckets[t][source] ?? 0) + n;
}

const columnsAt = (now: number) => {
  const last = Math.floor(now / BUCKET_MS) * BUCKET_MS;
  return Array.from({ length: COLUMNS }, (_, i) => last - (COLUMNS - 1 - i) * BUCKET_MS);
};

export const syncedSince = (buckets: Buckets, now: number) =>
  columnsAt(now).reduce((n, t) => n + Object.values(buckets[t] ?? {}).reduce((a, b) => a + b, 0), 0);

// Whole-number steps (1, 2 or 5 times a power of ten) with the top tick above the tallest bar.
function scale(peak: number) {
  const raw = peak / 3;
  const power = 10 ** Math.max(0, Math.floor(Math.log10(raw)));
  const step = [1, 2, 5, 10].map((m) => m * power).find((s) => s >= raw) ?? 10 * power;
  const top = Math.max(1, Math.floor(peak / step) + 1) * step;
  return { top, ticks: Array.from({ length: top / step + 1 }, (_, i) => i * step) };
}

const clock = (t: number) => new Date(t).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

// A bar per 10 seconds. Hacker News is polled every minute, so its bursts set the rhythm. Documents
// still being embedded sit on top in grey and take the colour once they are searchable.
export function Pulse({ buckets, waiting, now }: { buckets: Buckets; waiting: Waiting; now: number }) {
  const columns = columnsAt(now);
  const totals = columns.map((t) => Object.values(buckets[t] ?? {}).reduce((a, b) => a + b, 0));
  const pending: Record<number, number> = {};
  for (const t of Object.values(waiting)) pending[t] = (pending[t] ?? 0) + 1;

  const perSource: Record<string, number> = {};
  for (const t of columns) for (const [s, n] of Object.entries(buckets[t] ?? {})) perSource[s] = (perSource[s] ?? 0) + n;
  const order = [...ORDER, ...Object.keys(perSource).filter((s) => !ORDER.includes(s))].filter((s) => perSource[s]);
  const total = totals.reduce((a, b) => a + b, 0);
  const anyWaiting = columns.some((t, i) => totals[i] && pending[t]);

  // One first sync of thousands would flatten every burst after it, so a bar more than four times the
  // next tallest runs off the top of a scale set by the rest, with its count printed over it.
  const sorted = [...totals].sort((a, b) => b - a);
  const capped = sorted[1] > 0 && sorted[0] > 4 * sorted[1];
  const { top, ticks } = scale(capped ? sorted[1] : Math.max(sorted[0], 3));
  const label = (t: number) => {
    const parts = order.filter((s) => buckets[t]?.[s]).map((s) => `${count(buckets[t][s])} ${SOURCES[s]?.label ?? s}`);
    return `${clock(t)}: ${parts.join(', ')}${pending[t] ? `; ${count(Math.min(pending[t], totals[columns.indexOf(t)]))} waiting to be embedded` : ''}`;
  };

  return (
    <section className="card pulse" aria-labelledby="pulse-title">
      <div className="card-head">
        <h2 id="pulse-title">Synced in the last 15 minutes</h2>
        {anyWaiting ? (
          <ul className="key">
            <li>Searchable</li>
            <li className="waiting">Waiting to be embedded</li>
          </ul>
        ) : (
          <span className="aside">
            <span className="fig">{count(total)}</span> documents
          </span>
        )}
      </div>
      <div className="chart">
        <div className="ticks" aria-hidden="true">
          {(total ? ticks : [0]).map((v) => (
            <span key={v} style={{ bottom: `${(v / top) * 100}%` }}>
              {count(v)}
            </span>
          ))}
        </div>
        <div className="plot">
          <svg viewBox={`0 0 ${COLUMNS * 10} 100`} preserveAspectRatio="none" role="img" aria-label={`${count(total)} documents synced in the last 15 minutes`}>
            {total > 0 &&
              ticks.slice(1).map((v) => (
                <line key={v} className="gridline" x1="0" x2={COLUMNS * 10} y1={100 - (v / top) * 100} y2={100 - (v / top) * 100} vectorEffect="non-scaling-stroke" />
              ))}
            {columns.map((t, i) => {
              if (!totals[i]) return null;
              const height = Math.min(totals[i] / top, 1) * 100;
              const grey = (Math.min(pending[t] ?? 0, totals[i]) / totals[i]) * height;
              return (
                <g key={t} className="column">
                  <title>{label(t)}</title>
                  <rect className="searchable" x={i * 10 + 1} y={100 - height + grey} width={8} height={height - grey} />
                  {grey > 0 && <rect className="waiting" x={i * 10 + 1} y={100 - height} width={8} height={grey} />}
                </g>
              );
            })}
            <line className="baseline" x1="0" x2={COLUMNS * 10} y1="100" y2="100" vectorEffect="non-scaling-stroke" />
          </svg>
          {capped && (
            <span className="peak fig" style={{ left: `${((totals.indexOf(sorted[0]) + 0.5) / COLUMNS) * 100}%` }}>
              {count(sorted[0])}
            </span>
          )}
        </div>
        <div className="axis" aria-hidden="true">
          <span>15 min ago</span>
          <span>10 min ago</span>
          <span>5 min ago</span>
          <span>Now</span>
        </div>
      </div>
      {total ? (
        <ul className="by-source">
          {order.map((s) => (
            <li key={s}>
              {SOURCES[s]?.label ?? s}
              <span className="fig">{count(perSource[s])}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="by-source">Nothing synced in the last 15 minutes. Hacker News is checked every minute while the pipeline runs.</p>
      )}
    </section>
  );
}
