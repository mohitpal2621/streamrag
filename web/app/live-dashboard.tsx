'use client';

import { useEffect, useState } from 'react';
import { query, subscribe } from '../lib/api';
import { feedName, kindOf, KINDS } from '../lib/sources';
import { ago, compact, count, duration, median } from '../lib/time';
import {
  ACTIVITY_FIELDS,
  ACTIVITY_SUBSCRIPTION,
  FEED_ROWS,
  FEEDS_QUERY,
  STATUS_QUERY,
  USAGE_QUERY,
  type Activity,
  type Alert,
  type DocumentActivity,
  type Feed,
  type ModelUsage,
  type Snapshot,
  type Status,
} from '../lib/types';
import { Pulse, toBuckets, withDocument } from './pulse';

// A row of the feed. `searchableAfter` is measured here: the time from the sync's commit to the
// moment the embedder said the document was searchable, for documents seen making that change.
type Row = DocumentActivity & { fresh?: boolean; searchableAfter?: number };

const CONNECTION = { connecting: 'Connecting', live: 'Live', reconnecting: 'Reconnecting' };

function useNow(start: number) {
  const [now, setNow] = useState(start); // the server's render time first, so hydration matches
  useEffect(() => {
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  return now;
}

export function LiveDashboard({ api, initial, renderedAt }: { api: string; initial: Snapshot; renderedAt: number }) {
  const [rows, setRows] = useState<Row[]>(initial.recent);
  const [buckets, setBuckets] = useState(() => toBuckets(initial.pulse));
  const [alerts, setAlerts] = useState(initial.alerts);
  const [status, setStatus] = useState(initial.status);
  const [feeds, setFeeds] = useState(initial.feeds);
  const [usage, setUsage] = useState(initial.usage);
  const [connection, setConnection] = useState<keyof typeof CONNECTION>('connecting');
  const now = useNow(renderedAt);

  // The activity stream, reopened with backoff when it drops. Changes committed while it was down
  // never reach it, so the recent list and the pulse are reloaded once it is back.
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    const reload = () =>
      query<Pick<Snapshot, 'recent' | 'pulse'>>(api, `{ recent(limit: ${FEED_ROWS}) { ${ACTIVITY_FIELDS} } pulse(minutes: 15) { at source documents } }`).then(
        (d) => {
          setRows(d.recent);
          setBuckets(toBuckets(d.pulse));
        },
        () => {},
      );
    const apply = (a: Activity) => {
      if (a.__typename === 'DocumentActivity') {
        const { __typename, ...row } = a;
        setRows((rs) => [{ ...row, fresh: true }, ...rs.filter((r) => r.id !== row.id)].slice(0, FEED_ROWS));
        if (row.change !== 'closed') setBuckets((b) => withDocument(b, row.at, row.document.source));
      } else if (a.__typename === 'Searchable') {
        const ids = new Set(a.documentIds);
        const at = Date.parse(a.at);
        setRows((rs) =>
          rs.map((r) =>
            ids.has(r.document.id) && !r.document.searchable && r.change !== 'closed'
              ? { ...r, document: { ...r.document, searchable: true }, searchableAfter: at - Date.parse(r.at) }
              : r,
          ),
        );
      } else {
        setAlerts((as) => [a.alert, ...as.filter((x) => x.id !== a.alert.id)].slice(0, 8));
      }
    };

    (async () => {
      let failures = 0;
      let dropped = false;
      while (!signal.aborted) {
        try {
          const events = subscribe<{ activity: Activity }>(api, ACTIVITY_SUBSCRIPTION, {}, {
            signal,
            onOpen: () => {
              setConnection('live');
              failures = 0;
              if (dropped) reload();
            },
          });
          for await (const { activity } of events) apply(activity);
        } catch {
          // the API went away or the stream broke; try again below
        }
        if (signal.aborted) return;
        dropped = true;
        setConnection('reconnecting');
        await new Promise((resolve) => setTimeout(resolve, Math.min(1_000 * 2 ** failures++, 15_000)));
      }
    })();
    return () => controller.abort();
  }, [api]);

  // Everything else is polled: the status often, feeds and model usage less so.
  useEffect(() => {
    const poll = <T,>(document: string, apply: (data: T) => void, ms: number) => {
      const timer = setInterval(() => query<T>(api, document).then(apply, () => {}), ms);
      return () => clearInterval(timer);
    };
    const stops = [
      poll<{ status: Status }>(`{ ${STATUS_QUERY} }`, (d) => setStatus(d.status), 5_000),
      poll<{ feeds: Feed[] }>(`{ ${FEEDS_QUERY} }`, (d) => setFeeds(d.feeds), 30_000),
      poll<{ usage: ModelUsage[] }>(`{ ${USAGE_QUERY} }`, (d) => setUsage(d.usage), 60_000),
    ];
    return () => stops.forEach((stop) => stop());
  }, [api]);

  const latency = median(rows.flatMap((r) => (r.searchableAfter === undefined ? [] : [r.searchableAfter])));

  return (
    <main>
      <section className="hero" aria-labelledby="pulse-title">
        <div className="hero-head">
          <h1 id="pulse-title">Synced in the last 15 minutes</h1>
          <span className={`connection ${connection}`} role="status">
            {CONNECTION[connection]}
          </span>
        </div>
        <Pulse buckets={buckets} now={now} />
        <p className="summary">
          {count(status.documents)} documents, split into {count(status.chunks)} searchable chunks.
          {latency !== null && ` New ones were searchable a median of ${duration(latency)} after their sync.`}
          {status.pendingEmbeddings > 0 && ` ${count(status.pendingEmbeddings)} are waiting to be embedded.`}
        </p>
      </section>

      <div className="columns">
        <section aria-labelledby="feed-title">
          <h2 id="feed-title">Latest changes</h2>
          {rows.length ? (
            <ol className="feed">
              {rows.map((r) => (
                <FeedRow key={r.id} row={r} now={now} />
              ))}
            </ol>
          ) : (
            <p className="muted">
              Nothing has synced in the last day. Run <code>npm run pipeline</code> to keep every source in sync, or <code>npm run ingest</code> to sync
              them once.
            </p>
          )}
        </section>

        <aside>
          <PipelinePanel status={status} />
          <AlertsPanel alerts={alerts} now={now} />
          <FeedsPanel feeds={feeds} now={now} />
          <ModelsPanel usage={usage} />
        </aside>
      </div>
    </main>
  );
}

function rowState(r: Row) {
  if (r.change === 'closed') return 'Closed';
  const state =
    r.searchableAfter !== undefined ? `searchable after ${duration(r.searchableAfter)}` : r.document.searchable ? 'searchable' : 'waiting to be embedded';
  return r.change === 'changed' ? `Edited, ${state}` : state[0].toUpperCase() + state.slice(1);
}

function FeedRow({ row, now }: { row: Row; now: number }) {
  const kind = kindOf(row.document.source, row.document.feed);
  const classes = [row.change === 'closed' && 'closed', row.fresh && 'fresh'].filter(Boolean).join(' ');
  return (
    <li className={classes || undefined}>
      <span className="when">{ago(row.at, now)}</span>
      <span className={`kind k-${kind}`}>
        <span className="swatch" aria-hidden="true" />
        {KINDS[kind]}
      </span>
      <span className="title">
        {row.document.url ? (
          <a href={row.document.url} target="_blank" rel="noreferrer">
            {row.document.title}
          </a>
        ) : (
          row.document.title
        )}
        {row.document.snippet && <span className="snippet">{row.document.snippet}</span>}
      </span>
      <span className={`state${row.document.searchable && row.change !== 'closed' ? ' searchable' : ''}`}>{rowState(row)}</span>
    </li>
  );
}

function PipelinePanel({ status: s }: { status: Status }) {
  const behind = s.lag?.reduce((n, l) => n + l.behind, 0) ?? 0;
  return (
    <section aria-labelledby="pipeline-title">
      <h2 id="pipeline-title">Pipeline</h2>
      <dl className="facts">
        <dt>Outbox</dt>
        <dd className={`dot${s.outbox > 100 ? ' warn' : ''}`}>
          {s.outbox ? `${count(s.outbox)} events waiting${s.oldestOutboxSeconds ? `, the oldest for ${duration(s.oldestOutboxSeconds * 1_000)}` : ''}` : 'Empty'}
        </dd>
        <dt>Embeddings</dt>
        <dd className={`dot${s.pendingEmbeddings > 100 ? ' warn' : ''}`}>{s.pendingEmbeddings ? `${count(s.pendingEmbeddings)} documents waiting` : 'Up to date'}</dd>
        <dt>Consumers</dt>
        <dd className={`dot${!s.lag ? ' off' : behind > 100 ? ' warn' : ''}`}>
          {s.lag ? (
            s.lag.map((l) => `${l.group} ${l.behind ? `${count(l.behind)} behind` : 'caught up'}`).join(', ')
          ) : (
            <>
              Kafka isn&apos;t reachable <span className="muted small">({s.kafkaError})</span>
            </>
          )}
        </dd>
        {s.lag && (
          <>
            <dt>Dead letters</dt>
            <dd className={`dot${s.deadLetters ? ' warn' : ''}`}>{s.deadLetters ? `${count(s.deadLetters)} not replayed` : 'None'}</dd>
          </>
        )}
        <dt>Sync jobs</dt>
        <dd className={`dot${!s.syncs ? ' off' : s.syncs.failed ? ' warn' : ''}`}>
          {s.syncs ? (
            `${s.syncs.scheduled} feeds scheduled, ${s.syncs.active} syncing now, ${s.syncs.waiting} waiting, ${s.syncs.failed} failed`
          ) : (
            <>
              Redis isn&apos;t reachable <span className="muted small">({s.redisError})</span>
            </>
          )}
        </dd>
      </dl>
    </section>
  );
}

function AlertsPanel({ alerts, now }: { alerts: Alert[]; now: number }) {
  return (
    <section aria-labelledby="alerts-title">
      <h2 id="alerts-title">Security alerts</h2>
      {alerts.length ? (
        <ul className="alerts">
          {alerts.map((a) => (
            <li key={a.id}>
              <strong>{a.package}</strong>{' '}
              {a.url ? (
                <a href={a.url} target="_blank" rel="noreferrer">
                  {a.title}
                </a>
              ) : (
                a.title
              )}{' '}
              <span className="muted small">{ago(a.at, now)}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="muted small">No new advisories. One shows up here as soon as it is published for a package you track.</p>
      )}
    </section>
  );
}

const SHOWN_FEEDS = 8;

function FeedsPanel({ feeds, now }: { feeds: Feed[]; now: number }) {
  const next = (f: Feed) => (f.nextSyncAt ? Date.parse(f.nextSyncAt) : Infinity);
  const sorted = [...feeds].sort((a, b) => next(a) - next(b) || b.documents - a.documents);
  const row = (f: Feed) => {
    const due = f.nextSyncAt ? Date.parse(f.nextSyncAt) - now : null;
    return (
      <tr key={`${f.source} ${f.feed}`}>
        <td>{feedName(f.source, f.feed)}</td>
        <td className="num">{count(f.documents)}</td>
        <td className="num">{ago(f.syncedAt, now)}</td>
        <td className="num">{due === null ? '–' : due > 1_000 ? `in ${duration(due).replace('.0', '')}` : due > -60_000 ? 'now' : 'overdue'}</td>
      </tr>
    );
  };
  const head = (
    <thead>
      <tr>
        <th>Feed</th>
        <th className="num">Docs</th>
        <th className="num">Synced</th>
        <th className="num">Next</th>
      </tr>
    </thead>
  );
  return (
    <section aria-labelledby="feeds-title">
      <h2 id="feeds-title">Feeds</h2>
      <table>
        {head}
        <tbody>{sorted.slice(0, SHOWN_FEEDS).map(row)}</tbody>
      </table>
      {sorted.length > SHOWN_FEEDS && (
        <details>
          <summary>All {sorted.length} feeds</summary>
          <table>
            {head}
            <tbody>{sorted.slice(SHOWN_FEEDS).map(row)}</tbody>
          </table>
        </details>
      )}
    </section>
  );
}

function ModelsPanel({ usage }: { usage: ModelUsage[] }) {
  const seconds = (ms: number | null) => (ms === null ? '–' : duration(ms));
  return (
    <section aria-labelledby="models-title">
      <h2 id="models-title">Models, last 7 days</h2>
      {usage.length ? (
        <table>
          <thead>
            <tr>
              <th>Model</th>
              <th className="num">Calls</th>
              <th className="num">Errors</th>
              <th className="num">p95</th>
              <th className="num">Tokens</th>
            </tr>
          </thead>
          <tbody>
            {usage.map((u) => (
              <tr key={`${u.provider} ${u.model}`}>
                <td>
                  {u.model} <span className="muted">({u.provider})</span>
                </td>
                <td className="num">{count(u.ok)}</td>
                <td className="num">{count(u.errors)}</td>
                <td className="num">{seconds(u.p95Ms)}</td>
                <td className="num">
                  {compact(u.inputTokens)} in, {compact(u.outputTokens)} out
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="muted small">No model calls yet. Every answer from the Ask page is counted here.</p>
      )}
    </section>
  );
}
