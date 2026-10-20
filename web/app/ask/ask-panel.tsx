'use client';

import { useRef, useState, type FormEvent, type ReactNode } from 'react';
import { query, subscribe } from '../../lib/api';
import { kindOf, KINDS, type Kind } from '../../lib/sources';
import { count } from '../../lib/time';
import type { Feed } from '../../lib/types';

type Source = { n: number; title: string; url: string | null };
type Hit = { title: string; url: string | null; source: string; snippet: string };
type Step = { name: string; arguments: string };
type AskEvent =
  | { __typename: 'AnswerSources'; sources: Source[] }
  | { __typename: 'AnswerToken'; text: string }
  | { __typename: 'AnswerDone'; cited: number[]; provider: string | null; cached: boolean };
type AgentEvent = ({ __typename: 'AgentToolCall' } & Step) | { __typename: 'AgentToken'; text: string };

type Turn = {
  id: number;
  kind: 'ask' | 'agent' | 'search';
  question: string;
  answer: string;
  steps: Step[];
  sources?: Source[];
  done?: { cited: number[]; provider: string | null; cached: boolean };
  hits?: Hit[];
  error?: string;
  finished?: boolean;
};

const ASK = `subscription Ask($question: String!) {
  ask(question: $question) {
    __typename
    ... on AnswerSources { sources { n title url } }
    ... on AnswerToken { text }
    ... on AnswerDone { cited provider cached }
  }
}`;
const AGENT = `subscription Agent($question: String!, $thread: String) {
  agent(question: $question, thread: $thread) {
    __typename
    ... on AgentToolCall { name arguments }
    ... on AgentToken { text }
  }
}`;
const SEARCH = `query Search($query: String!) { search(query: $query, limit: 8) { title url source snippet } }`;

const EXAMPLES = [
  'How does Cloudflare D1 scale to millions of databases?',
  'What breaks if I upgrade express from 4.21.0 to the latest version?',
  'Which advisories affect lodash 4.17.20?',
];

// crypto.randomUUID only exists on https and localhost pages.
const newThread = () => (typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : Math.random().toString(36).slice(2));

const MODE_TAG = { ask: null, agent: 'Agent', search: 'Search' };
const KIND_ROWS: Record<Kind, string> = {
  hackernews: 'Hacker News',
  blog: 'Blogs and podcasts',
  release: 'Release notes',
  advisory: 'Security advisories',
  transcript: 'Transcripts',
};

export function AskPanel({ api, feeds }: { api: string; feeds: Feed[] | null }) {
  const [question, setQuestion] = useState('');
  const [turns, setTurns] = useState<Turn[]>([]);
  const [thread, setThread] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const nextId = useRef(0);

  const update = (id: number, change: (t: Turn) => Turn) => setTurns((ts) => ts.map((t) => (t.id === id ? change(t) : t)));

  async function run(kind: Turn['kind']) {
    const q = question.trim();
    if (!q || busy) return;
    const id = nextId.current++;
    setTurns((ts) => [...ts, { id, kind, question: q, answer: '', steps: [] }]);
    setQuestion('');
    setBusy(true);
    controller.current = new AbortController();
    const { signal } = controller.current;
    try {
      if (kind === 'search') {
        const { search } = await query<{ search: Hit[] }>(api, SEARCH, { query: q }, { signal });
        update(id, (t) => ({ ...t, hits: search }));
      } else if (kind === 'ask') {
        for await (const { ask: e } of subscribe<{ ask: AskEvent }>(api, ASK, { question: q }, { signal })) {
          if (e.__typename === 'AnswerSources') update(id, (t) => ({ ...t, sources: e.sources }));
          else if (e.__typename === 'AnswerToken') update(id, (t) => ({ ...t, answer: t.answer + e.text }));
          else update(id, (t) => ({ ...t, done: { cited: e.cited, provider: e.provider, cached: e.cached } }));
        }
      } else {
        const t = thread ?? newThread();
        setThread(t);
        for await (const { agent: e } of subscribe<{ agent: AgentEvent }>(api, AGENT, { question: q, thread: t }, { signal })) {
          if (e.__typename === 'AgentToolCall') update(id, (x) => ({ ...x, steps: [...x.steps, { name: e.name, arguments: e.arguments }] }));
          else update(id, (x) => ({ ...x, answer: x.answer + e.text }));
        }
      }
    } catch (err) {
      const message = signal.aborted ? 'Stopped.' : err instanceof Error ? err.message : String(err);
      update(id, (t) => ({ ...t, error: message }));
    } finally {
      update(id, (t) => ({ ...t, finished: true }));
      setBusy(false);
    }
  }

  const submit = (e: FormEvent) => {
    e.preventDefault();
    void run('ask');
  };

  return (
    <main>
      <div className="ask-grid">
        <div>
          <h1>Ask the sources</h1>
          <p className="finding">
            Answers come from the documents streamrag has synced{feeds?.length ? ` from ${count(feeds.length)} feeds` : ''}, with links to the ones they cite.
          </p>
          <form className="ask-form" onSubmit={submit}>
            <input
              aria-label="Question"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Ask about anything streamrag has synced"
              autoComplete="off"
            />
            <button type="submit" disabled={busy || !question.trim()}>
              Ask
            </button>
            <button type="button" className="secondary" disabled={busy || !question.trim()} onClick={() => void run('agent')}>
              Ask the agent
            </button>
            <button type="button" className="secondary" disabled={busy || !question.trim()} onClick={() => void run('search')}>
              Search
            </button>
            {busy && (
              <button type="button" className="text stop" onClick={() => controller.current?.abort()}>
                Stop
              </button>
            )}
          </form>

          {turns.length === 0 ? (
            <dl className="modes">
              <dt>Ask</dt>
              <dd>Writes one answer from the six best-matching documents and cites them.</dd>
              <dt>Ask the agent</dt>
              <dd>
                Can also read the breaking changes between two versions of an npm package and look up its security advisories. It remembers the
                conversation, so follow-up questions work.
              </dd>
              <dt>Search</dt>
              <dd>Lists the best-matching documents without writing an answer.</dd>
            </dl>
          ) : (
            <>
              {turns.map((t) => (
                <TurnView key={t.id} turn={t} />
              ))}
              {!busy && (
                <p className="after-turns">
                  {thread ? 'The agent keeps this conversation in mind. ' : ''}
                  <button
                    type="button"
                    className="text"
                    onClick={() => {
                      setTurns([]);
                      setThread(null);
                    }}
                  >
                    Start over
                  </button>
                </p>
              )}
            </>
          )}
        </div>

        <aside className="card rail">
          <section className="examples" aria-labelledby="examples-title">
            <h2 id="examples-title">Try asking</h2>
            <ul>
              {EXAMPLES.map((q) => (
                <li key={q}>
                  <button type="button" className="text" onClick={() => setQuestion(q)}>
                    {q}
                  </button>
                </li>
              ))}
            </ul>
          </section>
          {feeds && feeds.length > 0 && <IndexPanel feeds={feeds} />}
        </aside>
      </div>
    </main>
  );
}

// What there is to ask about: documents per kind of source.
function IndexPanel({ feeds }: { feeds: Feed[] }) {
  const byKind = new Map<Kind, { feeds: number; documents: number }>();
  for (const f of feeds) {
    const k = kindOf(f.source, f.feed);
    const entry = byKind.get(k) ?? { feeds: 0, documents: 0 };
    byKind.set(k, { feeds: entry.feeds + 1, documents: entry.documents + f.documents });
  }
  const rows = [...byKind].sort((a, b) => b[1].documents - a[1].documents);
  return (
    <section className="index" aria-labelledby="index-title">
      <h2 id="index-title">In the index</h2>
      <table>
        <thead>
          <tr>
            <th>Source</th>
            <th className="num">Feeds</th>
            <th className="num">Documents</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([k, v]) => (
            <tr key={k}>
              <td>{KIND_ROWS[k]}</td>
              <td className="num">{count(v.feeds)}</td>
              <td className="num">{count(v.documents)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function describe(step: Step) {
  let a: Record<string, unknown> = {};
  try {
    a = JSON.parse(step.arguments || '{}');
  } catch {
    // shown by name only
  }
  if (step.name === 'search') return `Searched for “${a.query ?? ''}”`;
  if (step.name === 'breaking_changes') return `Read the release notes of ${a.package} from ${a.from} to ${a.to ?? 'the latest version'}`;
  if (step.name === 'security_advisories') return `Looked up advisories for ${a.package}${a.version ? ` ${a.version}` : ''}`;
  return `Called ${step.name}`;
}

const URL_PATTERN = /(https?:\/\/[^\s)\]]+)/g;
const CITATION = /(\[\d+(?:\s*,\s*\d+)*\])/g;

// Turns [n] into links to source n, and bare URLs into links.
function linked(text: string, sources: Source[] = []): ReactNode[] {
  return text.split(CITATION).flatMap((part, i): ReactNode[] => {
    const cite = part.match(/^\[(\d+(?:\s*,\s*\d+)*)\]$/);
    if (cite && sources.length) {
      return cite[1].split(',').map((n) => {
        const s = sources[Number(n) - 1];
        return s?.url ? (
          <a key={`${i}-${n}`} className="cite" href={s.url} target="_blank" rel="noreferrer" title={s.title}>
            [{n.trim()}]
          </a>
        ) : (
          <span key={`${i}-${n}`} className="cite">
            [{n.trim()}]
          </span>
        );
      });
    }
    return part.split(URL_PATTERN).map((piece, j) =>
      j % 2 ? (
        <a key={`${i}-${j}`} href={piece} target="_blank" rel="noreferrer">
          {piece}
        </a>
      ) : (
        piece
      ),
    );
  });
}

function TurnView({ turn: t }: { turn: Turn }) {
  const waiting = !t.finished && !t.answer && !t.error;
  return (
    <article className="turn">
      <h2>
        {t.question}
        {MODE_TAG[t.kind] && <span className="tag">{MODE_TAG[t.kind]}</span>}
      </h2>
      {t.steps.length > 0 && (
        <ul className="steps">
          {t.steps.map((s, i) => (
            <li key={i}>{describe(s)}</li>
          ))}
        </ul>
      )}
      {t.kind === 'search' ? (
        t.hits && <Hits hits={t.hits} />
      ) : (
        <>
          {waiting && <p className="answer pending">{t.kind === 'agent' ? 'Working out what to look up…' : 'Reading the sources…'}</p>}
          {t.answer && <p className="answer">{linked(t.answer, t.sources)}</p>}
          {t.done?.provider && <p className="meta">{t.done.cached ? `Cached answer, first written by ${t.done.provider}` : `Answered by ${t.done.provider}`}</p>}
          {t.sources && t.sources.length > 0 && (
            <ol className="sources">
              {t.sources.map((s) => (
                <li key={s.n} className={t.done?.cited.includes(s.n) ? 'cited' : undefined}>
                  {s.url ? (
                    <a href={s.url} target="_blank" rel="noreferrer">
                      {s.title}
                    </a>
                  ) : (
                    s.title
                  )}
                </li>
              ))}
            </ol>
          )}
        </>
      )}
      {t.error && <p className="error">{t.error === 'Stopped.' ? t.error : `The answer stopped: ${t.error}`}</p>}
    </article>
  );
}

function Hits({ hits }: { hits: Hit[] }) {
  if (!hits.length) return <p className="answer pending">No matches. Try other words, or sync more sources.</p>;
  return (
    <ul className="hits">
      {hits.map((h, i) => {
        const kind = kindOf(h.source, '');
        return (
          <li key={i}>
            {h.url ? (
              <a href={h.url} target="_blank" rel="noreferrer">
                {h.title}
              </a>
            ) : (
              h.title
            )}
            <span className="kind">{KINDS[kind]}</span>
            <p>{h.snippet}</p>
          </li>
        );
      })}
    </ul>
  );
}
