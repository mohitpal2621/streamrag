'use client';

import { Fragment, useEffect, useState, type ReactNode } from 'react';
import { query, subscribe } from '../lib/api';
import { feedName } from '../lib/sources';
import { ago, compact, count, duration, median, roughly } from '../lib/time';
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
import { bucketOf, Pulse, syncedSince, toBuckets, withDocument, type Waiting } from './pulse';

// A row of the feed. `searchableAfter` is the time from the sync's commit to its chunks' commit: the
// API's figure for rows embedded before the page opened, and measured from the stream for the rest.
type Row = DocumentActivity & { fresh?: boolean; searchableAfter?: number };

const CONNECTION = { connecting: 'Connecting', live: 'Live', reconnecting: 'Reconnecting' };
const SHOWN_ROWS = 12;

function useNow(start: number) {
  const [now, setNow] = useState(start); // the server's render time first, so hydration matches
  useEffect(() => {
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  return now;
}

const measured = (rows: DocumentActivity[]): Row[] =>
  rows.map((r) => (r.searchableAfterMs === null ? r : { ...r, searchableAfter: r.searchableAfterMs }));

// Rows the embedder hasn't reached yet, so the pulse can show them waiting.
const waitingIn = (rows: Row[]): Waiting =>
  Object.fromEntries(rows.filter((r) => !r.document.searchable && r.change !== 'closed').map((r) => [r.document.id, bucketOf(r.at)]));

export function LiveDashboard({ api, initial, renderedAt }: { api: string; initial: Snapshot; renderedAt: number }) {
  const [rows, setRows] = useState(() => measured(initial.recent));
  const [buckets, setBuckets] = useState(() => toBuckets(initial.pulse));
  const [waiting, setWaiting] = useState(() => waitingIn(initial.recent));
  const [alerts, setAlerts] = useState(initial.alerts);
  const [status, setStatus] = useState(initial.status);
  const [feeds, setFeeds] = useState(initial.feeds);
  const [usage, setUsage] = useState(initial.usage);
  const [connection, setConnection] = useState<keyof typeof CONNECTION>('connecting');
  const [expanded, setExpanded] = useState(false);
  const now = useNow(renderedAt);

  // The activity stream, reopened with backoff when it drops. Changes committed while it was down
  // never reach it, so the recent list and the pulse are reloaded once it is back.
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    const reload = () =>
      query<Pick<Snapshot, 'recent' | 'pulse'>>(api, `{ recent(limit: ${FEED_ROWS}) { ${ACTIVITY_FIELDS} } pulse(minutes: 15) { at source documents } }`).then(
        (d) => {
          setRows(measured(d.recent));
          setBuckets(toBuckets(d.pulse));
          setWaiting(waitingIn(d.recent));
        },
        () => {},
      );
    const apply = (a: Activity) => {
      if (a.__typename === 'DocumentActivity') {
        const { __typename, ...row } = a;
        setRows((rs) => [{ ...row, fresh: true }, ...rs.filter((r) => r.id !== row.id)].slice(0, FEED_ROWS));
        if (row.change !== 'closed') setBuckets((b) => withDocument(b, row.at, row.document.source));
        if (row.change !== 'closed' && !row.document.searchable) setWaiting((w) => ({ ...w, [row.document.id]: bucketOf(row.at) }));
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
        setWaiting((w) => Object.fromEntries(Object.entries(w).filter(([id]) => !ids.has(id))));
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
  const found = finding(status, feeds, latency, syncedSince(buckets, now), now);
  const newest = rows[0] && ago(rows[0].at, now);
  const shown = expanded ? rows : rows.slice(0, SHOWN_ROWS);

  return (
    <main>
      <div className="title-row">
        <div>
          <h1>Live sync</h1>
          <p className={`finding${found.problem ? ' problem' : ''}`}>{found.text}</p>
        </div>
        <div className="freshness">
          <span className={`connection ${connection}`} role="status">
            {CONNECTION[connection]}
          </span>
          {newest && (
            <span>
              Last change <span className="fig">{newest === 'now' ? 'just now' : newest}</span>
            </span>
          )}
        </div>
      </div>

      <div className="live-grid">
        <Pulse buckets={buckets} waiting={waiting} now={now} />
        <PipelinePanel status={status} />

        <section className="card changes" aria-labelledby="changes-title">
          <div className="card-head">
            <h2 id="changes-title">Latest changes</h2>
            {latency !== null && found.problem && (
              <span className="aside">
                Searchable a median of <span className="fig">{duration(latency)}</span> after their sync
              </span>
            )}
          </div>
          {rows.length ? (
            <>
              <div className="changes-head">
                <span>Synced</span>
                <span>Document</span>
                <span>Feed</span>
                <span className="num">Searchable</span>
              </div>
              <ol className="feed">
                {shown.map((r) => (
                  <FeedRow key={r.id} row={r} now={now} />
                ))}
              </ol>
              {rows.length > SHOWN_ROWS && (
                <button type="button" className="text more" onClick={() => setExpanded((e) => !e)}>
                  {expanded ? 'Show fewer' : `Show all ${rows.length}`}
                </button>
              )}
            </>
          ) : (
            <p className="empty">
              Nothing has synced in the last day. Run <code>npm run pipeline</code> to keep every source in sync, or <code>npm run ingest</code> to sync them
              once.
            </p>
          )}
        </section>

        <aside className="card rail">
          <AlertsPanel alerts={alerts} feeds={feeds} now={now} />
          <FeedsPanel feeds={feeds} now={now} />
          <ModelsPanel usage={usage} />
        </aside>
      </div>
    </main>
  );
}

// The one sentence under the title: the stuck part of the pipeline if there is one, otherwise how
// quickly new documents turned searchable.
function finding(s: Status, feeds: Feed[], latency: number | null, synced: number, now: number): { text: ReactNode; problem: boolean } {
  const problem = (text: ReactNode) => ({ text, problem: true });
  if (!s.lag) return problem("The pipeline can't reach Kafka, so new documents aren't being embedded.");
  if (!s.syncs) return problem("Redis is down, so the feeds aren't being synced.");
  const scheduled = feeds.filter((f) => f.nextSyncAt);
  if (scheduled.length && scheduled.every((f) => Date.parse(f.nextSyncAt!) < now - 60_000))
    return problem(
      <>
        Every feed is overdue for its sync. Start the scheduler with <code>npm run pipeline</code>.
      </>,
    );
  const worst = [...s.lag].sort((a, b) => b.behind - a.behind)[0];
  if (worst && worst.behind > 100)
    return problem(
      <>
        The {worst.group} consumer is <span className="fig">{count(worst.behind)}</span> events behind.
      </>,
    );
  if (s.pendingEmbeddings > 100)
    return problem(
      <>
        <span className="fig">{count(s.pendingEmbeddings)}</span> documents are waiting to be embedded.
      </>,
    );
  if (s.outbox > 100)
    return problem(
      <>
        <span className="fig">{count(s.outbox)}</span> events are waiting in the outbox.
      </>,
    );
  if (latency !== null)
    return {
      text: (
        <>
          New documents were searchable a median of <span className="fig">{duration(latency)}</span> after their sync.
        </>
      ),
      problem: false,
    };
  const caughtUp = !s.outbox && !s.pendingEmbeddings && s.lag.every((l) => !l.behind);
  if (synced)
    return {
      text: (
        <>
          <span className="fig">{count(synced)}</span> documents synced in the last 15 minutes{caughtUp ? ', and every stage has caught up' : ''}.
        </>
      ),
      problem: false,
    };
  return { text: 'Nothing has synced in the last 15 minutes.', problem: false };
}

function rowState(r: Row): { text: ReactNode; tone: string } {
  if (r.change === 'closed') return { text: 'Closed', tone: '' };
  if (r.searchableAfter !== undefined)
    return {
      text: (
        <>
          After <span className="fig">{duration(r.searchableAfter)}</span>
        </>
      ),
      tone: 'measured',
    };
  return r.document.searchable ? { text: 'Yes', tone: '' } : { text: 'Waiting', tone: 'waiting' };
}

function FeedRow({ row, now }: { row: Row; now: number }) {
  const state = rowState(row);
  const classes = [row.change === 'closed' && 'closed', row.fresh && 'fresh'].filter(Boolean).join(' ');
  const title = row.document.url ? (
    <a href={row.document.url} target="_blank" rel="noreferrer">
      {row.document.title}
    </a>
  ) : (
    <span>{row.document.title}</span>
  );
  return (
    <li className={classes || undefined}>
      <span className="when">
        <span className="fig">{ago(row.at, now)}</span>
        {row.change === 'changed' && <span className="change">edited</span>}
      </span>
      <span className="title">
        {title}
        {row.document.snippet && <span className="snippet">{row.document.snippet}</span>}
      </span>
      <span className="source">{feedName(row.document.source, row.document.feed)}</span>
      <span className={`state ${state.tone}`.trim()}>{state.text}</span>
    </li>
  );
}

type Stage = { label: ReactNode; value: ReactNode; tone?: 'warn' | 'bad'; detail?: string | null };

// The stages in the order a document passes through them: synced, written to the outbox, consumed
// from Kafka, embedded, searchable.
function PipelinePanel({ status: s }: { status: Status }) {
  const stages: Stage[] = [];
  if (s.syncs) {
    stages.push({
      label: 'Sync jobs',
      value: (
        <>
          <span className="fig">{s.syncs.scheduled}</span> scheduled, <span className="fig">{s.syncs.active}</span> syncing
          {s.syncs.waiting > 0 && (
            <>
              , <span className="fig">{s.syncs.waiting}</span> queued
            </>
          )}
        </>
      ),
    });
    if (s.syncs.failed) stages.push({ label: 'Failed syncs', value: <span className="fig">{count(s.syncs.failed)}</span>, tone: 'bad' });
  } else {
    stages.push({ label: 'Sync jobs', value: "Redis isn't reachable", tone: 'bad', detail: s.redisError });
  }
  stages.push({
    label: 'Outbox',
    value: s.outbox ? (
      <>
        <span className="fig">{count(s.outbox)}</span> events{s.oldestOutboxSeconds ? <>, oldest <span className="fig">{duration(s.oldestOutboxSeconds * 1_000)}</span></> : ''}
      </>
    ) : (
      'Empty'
    ),
    tone: s.outbox > 100 ? 'warn' : undefined,
  });
  if (s.lag) {
    for (const l of s.lag)
      stages.push({
        label: <code>{l.group}</code>,
        value: l.behind ? (
          <>
            <span className="fig">{count(l.behind)}</span> behind
          </>
        ) : (
          'Caught up'
        ),
        tone: l.behind > 100 ? 'warn' : undefined,
      });
  } else {
    stages.push({ label: 'Consumers', value: "Kafka isn't reachable", tone: 'bad', detail: s.kafkaError });
  }
  stages.push({
    label: 'Embeddings',
    value: s.pendingEmbeddings ? (
      <>
        <span className="fig">{count(s.pendingEmbeddings)}</span> waiting
      </>
    ) : (
      'Up to date'
    ),
    tone: s.pendingEmbeddings > 100 ? 'warn' : undefined,
  });
  if (s.lag)
    stages.push({
      label: 'Dead letters',
      value: s.deadLetters ? (
        <>
          <span className="fig">{count(s.deadLetters)}</span> not replayed
        </>
      ) : (
        'None'
      ),
      tone: s.deadLetters ? 'warn' : undefined,
    });
  stages.push({
    label: 'Index',
    value: (
      <>
        <span className="fig">{count(s.documents)}</span> documents, <span className="fig">{count(s.chunks)}</span> chunks
      </>
    ),
  });

  return (
    <section className="card pipeline" aria-labelledby="pipeline-title">
      <div className="card-head">
        <h2 id="pipeline-title">Pipeline</h2>
      </div>
      <dl className="stages">
        {stages.map((st, i) => {
          const last = i === stages.length - 1 ? ' last' : '';
          return (
            <Fragment key={i}>
              <dt className={last.trim() || undefined}>{st.label}</dt>
              <dd className={`${st.tone ?? ''}${last}`.trim() || undefined}>
                {st.value}
                {st.detail && <span className="detail">{st.detail}</span>}
              </dd>
            </Fragment>
          );
        })}
      </dl>
    </section>
  );
}

function AlertsPanel({ alerts, feeds, now }: { alerts: Alert[]; feeds: Feed[]; now: number }) {
  const watched = feeds.filter((f) => f.source === 'osv').map((f) => f.feed.replace(/^npm\//, ''));
  return (
    <section aria-labelledby="alerts-title">
      <div className="card-head">
        <h2 id="alerts-title">Security alerts</h2>
      </div>
      {alerts.length ? (
        <ul className="alerts">
          {alerts.map((a) => (
            <li key={a.id}>
              <span className="package">{a.package}</span>
              <span className="when fig">{ago(a.at, now)}</span>
              {a.url ? (
                <a className="advisory" href={a.url} target="_blank" rel="noreferrer" title={a.title}>
                  {a.title}
                </a>
              ) : (
                <span className="advisory">{a.title}</span>
              )}
            </li>
          ))}
        </ul>
      ) : (
        <>
          <p className="note">No advisories yet. One shows up here as soon as it is published for a package you track.</p>
          {watched.length > 0 ? (
            <p className="note">
              Watching {watched.slice(0, 5).join(', ')}
              {watched.length > 5 && ` and ${watched.length - 5} more`}.
            </p>
          ) : (
            <p className="note">
              Track a project&apos;s dependencies with <code>npm run track -- package.json</code>.
            </p>
          )}
        </>
      )}
    </section>
  );
}

const SHOWN_FEEDS = 6;

function FeedsPanel({ feeds, now }: { feeds: Feed[]; now: number }) {
  const next = (f: Feed) => (f.nextSyncAt ? Date.parse(f.nextSyncAt) : Infinity);
  const sorted = [...feeds].sort((a, b) => next(a) - next(b) || b.documents - a.documents);
  const late = (f: Feed) => f.nextSyncAt !== null && Date.parse(f.nextSyncAt) - now <= -60_000;
  // When every feed is late the scheduler is stopped, and the sentence under the title says so.
  const allLate = feeds.some((f) => f.nextSyncAt) && feeds.filter((f) => f.nextSyncAt).every(late);
  const row = (f: Feed) => {
    const due = f.nextSyncAt ? Date.parse(f.nextSyncAt) - now : null;
    const name = feedName(f.source, f.feed);
    return (
      <tr key={`${f.source} ${f.feed}`}>
        <td className="name">
          <span className="clip" title={name}>
            {name}
          </span>
          <span className="sub">
            <span className="fig">{count(f.documents)}</span> {f.documents === 1 ? 'document' : 'documents'}
          </span>
        </td>
        <td className="num">
          {ago(f.syncedAt, now)}
          <span className={`sub${late(f) && !allLate ? ' warn' : ''}`}>
            {due === null ? 'not scheduled' : due > 1_000 ? `next in ${roughly(due)}` : due > -60_000 ? 'next now' : 'overdue'}
          </span>
        </td>
      </tr>
    );
  };
  const head = (
    <thead>
      <tr>
        <th>Feed</th>
        <th className="num">Synced</th>
      </tr>
    </thead>
  );
  return (
    <section aria-labelledby="feeds-title">
      <div className="card-head">
        <h2 id="feeds-title">Feeds</h2>
      </div>
      <table className="feeds">
        {head}
        <tbody>{sorted.slice(0, SHOWN_FEEDS).map(row)}</tbody>
      </table>
      {sorted.length > SHOWN_FEEDS && (
        <details>
          <summary>All {sorted.length} feeds</summary>
          <table className="feeds">
            {head}
            <tbody>{sorted.slice(SHOWN_FEEDS).map(row)}</tbody>
          </table>
        </details>
      )}
    </section>
  );
}

function ModelsPanel({ usage }: { usage: ModelUsage[] }) {
  return (
    <section aria-labelledby="models-title">
      <div className="card-head">
        <h2 id="models-title">Models, last 7 days</h2>
      </div>
      {usage.length ? (
        <table className="models">
          <thead>
            <tr>
              <th>Model</th>
              <th className="num">Calls</th>
              <th className="num">p95</th>
              <th className="num">Tokens</th>
            </tr>
          </thead>
          <tbody>
            {usage.map((u) => (
              <tr key={`${u.provider} ${u.model}`}>
                <td className="name">
                  <span className="clip" title={u.model}>
                    {u.model}
                  </span>
                  <span className="sub">
                    {u.provider}
                    <span className="phone-only">
                      , {compact(u.inputTokens)} in, {compact(u.outputTokens)} out
                    </span>
                  </span>
                </td>
                <td className="num">
                  {count(u.ok)}
                  <span className={`sub${u.errors ? ' bad' : ''}`}>
                    {count(u.errors)} {u.errors === 1 ? 'error' : 'errors'}
                  </span>
                </td>
                <td className="num">{u.p95Ms === null ? '–' : duration(u.p95Ms)}</td>
                <td className="num">
                  {compact(u.inputTokens)} in
                  <span className="sub">{compact(u.outputTokens)} out</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="note">No model calls yet. Every answer from the Ask page is counted here.</p>
      )}
    </section>
  );
}
