import { createSchema, createYoga } from 'graphql-yoga';
import { askAgent, createAgent, type AgentEvent } from '../agent/graph.ts';
import type { Deps } from '../agent/tools.ts';
import { ask, type AnswerEvent } from '../core/answer.ts';
import { search, type Mode } from '../core/search.ts';
import { usage } from '../llm/calls.ts';
import { feeds, pipelineStatus } from '../pipeline/status.ts';
import { breakingChanges } from '../upgrade.ts';
import { documentActivity, listAlerts, pulse, type Activity, type Live } from './live.ts';

const typeDefs = /* GraphQL */ `
  enum Mode {
    keyword
    vector
    hybrid
    rerank
  }

  type Hit {
    title: String!
    url: String
    source: String!
    snippet: String!
  }

  type BreakingChange {
    change: String!
    migration: String!
  }

  type Release {
    version: String!
    url: String
    "null when the notes couldn't be found or fetched"
    changes: [BreakingChange!]
    error: String
  }

  type Upgrade {
    package: String!
    from: String!
    to: String!
    repo: String!
    releases: [Release!]!
  }

  type Alert {
    id: ID!
    at: String!
    package: String!
    title: String!
    url: String
  }

  type Lag {
    group: String!
    behind: Int!
  }

  type Syncs {
    scheduled: Int!
    waiting: Int!
    active: Int!
    delayed: Int!
    failed: Int!
  }

  type Status {
    documents: Int!
    chunks: Int!
    outbox: Int!
    "how long the oldest unpublished event has waited, in seconds"
    oldestOutboxSeconds: Int
    pendingEmbeddings: Int!
    alertsThisWeek: Int!
    "how far behind each consumer group is; null when Kafka is unreachable"
    lag: [Lag!]
    deadLetters: Int
    kafkaError: String
    "null when Redis is unreachable"
    syncs: Syncs
    redisError: String
  }

  type Feed {
    source: String!
    feed: String!
    documents: Int!
    syncedAt: String!
    "when the pipeline syncs it next; null when it isn't scheduled or Redis is unreachable"
    nextSyncAt: String
  }

  type ModelUsage {
    provider: String!
    model: String!
    ok: Int!
    errors: Int!
    skipped: Int!
    avgMs: Int
    p95Ms: Int
    inputTokens: Int!
    outputTokens: Int!
  }

  type Document {
    id: ID!
    source: String!
    feed: String!
    title: String!
    url: String
    "the start of its text"
    snippet: String!
    "whether its current version is embedded, and so findable by search"
    searchable: Boolean!
  }

  enum Change {
    added
    changed
    closed
  }

  type DocumentActivity {
    id: ID!
    at: String!
    change: Change!
    "Milliseconds from this change's commit to the commit of its chunks, once this version is searchable."
    searchableAfterMs: Float
    document: Document!
  }
  type Searchable {
    at: String!
    documentIds: [ID!]!
  }
  type NewAlert {
    alert: Alert!
  }
  union Activity = DocumentActivity | Searchable | NewAlert

  type PulseBucket {
    "start of a 10-second bucket"
    at: String!
    source: String!
    documents: Int!
  }

  type Source {
    n: Int!
    title: String!
    url: String
  }
  type AnswerSources {
    sources: [Source!]!
  }
  type AnswerToken {
    text: String!
  }
  type AnswerDone {
    answer: String!
    cited: [Int!]!
    provider: String
    cached: Boolean!
  }
  union AnswerEvent = AnswerSources | AnswerToken | AnswerDone

  type AgentToolCall {
    name: String!
    arguments: String!
  }
  type AgentToken {
    text: String!
  }
  union AgentEvent = AgentToolCall | AgentToken

  type Query {
    search(query: String!, mode: Mode = rerank, limit: Int = 10): [Hit!]!
    "What breaks between two versions of an npm package; to defaults to the latest."
    upgrade(package: String!, from: String!, to: String): Upgrade!
    alerts(limit: Int = 20): [Alert!]!
    status: Status!
    "Documents added, changed or closed lately, newest first."
    recent(limit: Int = 50): [DocumentActivity!]!
    "Documents added or changed per source, in 10-second buckets."
    pulse(minutes: Int = 15): [PulseBucket!]!
    feeds: [Feed!]!
    "Model calls over the last 7 days."
    usage: [ModelUsage!]!
  }

  type Subscription {
    "An answer from the synced sources: the numbered sources, then the text as it is written, then what it cited."
    ask(question: String!): AnswerEvent!
    "The agent's tool calls as it makes them, then its answer. Pass the same thread to ask a follow-up."
    agent(question: String!, thread: String): AgentEvent!
    "Every document a sync adds, changes or closes as it commits, when documents become searchable, and new security alerts."
    activity: Activity!
  }
`;

// A chunk's text after its title line, cut at a word boundary near 280 characters.
const snippet = (text: string) => {
  const body = text.slice(text.indexOf('\n') + 1).replace(/\s+/g, ' ').trim();
  if (body.length <= 280) return body;
  const end = body.lastIndexOf(' ', 280);
  return `${body.slice(0, end > 200 ? end : 280).replace(/[\s,;:.-]+$/, '')}…`;
};
const clamp = (n: number, max: number) => Math.min(Math.max(Math.trunc(n), 1), max);

// GraphQL over HTTP, with GraphiQL at /graphql. Subscriptions stream over server-sent events, which
// Yoga speaks out of the box, so a plain fetch or EventSource can read an answer as it's written.
// Yoga allows any origin by default, which lets the dashboard on another port call it.
export function createApi(deps: Deps, live: Live) {
  const agent = createAgent(deps);
  const passThrough = <T>(event: T) => event;
  return createYoga({
    graphqlEndpoint: '/graphql',
    graphiql: { title: 'streamrag' },
    schema: createSchema({
      typeDefs,
      resolvers: {
        Query: {
          search: async (_: unknown, a: { query: string; mode: Mode; limit: number }) =>
            (await search(deps.db, a.query, { mode: a.mode, limit: clamp(a.limit, 50), embed: deps.embed, rerank: deps.rerank })).map((h) => ({
              ...h,
              snippet: snippet(h.text),
            })),
          upgrade: async (_: unknown, a: { package: string; from: string; to?: string }) => ({
            package: a.package,
            from: a.from,
            ...(await breakingChanges(deps.db, deps.gateway, a.package, a.from, a.to)),
          }),
          alerts: (_: unknown, a: { limit: number }) => listAlerts(deps.db, { limit: clamp(a.limit, 500) }),
          status: async () => {
            const { kafka, syncs, ...s } = await pipelineStatus(deps.db);
            const k = 'unreachable' in kafka ? null : kafka;
            return {
              ...s,
              lag: k && (['embedder', 'alerts', 'transcriber'] as const).map((group) => ({ group, behind: k[group] })),
              deadLetters: k?.deadLetters ?? null,
              kafkaError: 'unreachable' in kafka ? kafka.unreachable : null,
              syncs: 'unreachable' in syncs ? null : syncs,
              redisError: 'unreachable' in syncs ? syncs.unreachable : null,
            };
          },
          recent: (_: unknown, a: { limit: number }) => documentActivity(deps.db, { limit: clamp(a.limit, 500) }),
          pulse: (_: unknown, a: { minutes: number }) => pulse(deps.db, clamp(a.minutes, 24 * 60)),
          feeds: () => feeds(deps.db),
          usage: () => usage(deps.db),
        },
        Subscription: {
          ask: { subscribe: (_: unknown, a: { question: string }) => ask(deps.db, a.question, deps), resolve: passThrough },
          agent: { subscribe: (_: unknown, a: { question: string; thread?: string }) => askAgent(agent, a.question, a.thread), resolve: passThrough },
          activity: { subscribe: () => live.subscribe(), resolve: passThrough },
        },
        AnswerEvent: { __resolveType: (e: AnswerEvent) => ({ sources: 'AnswerSources', token: 'AnswerToken', done: 'AnswerDone' })[e.type] },
        AnswerSources: { sources: (e: Extract<AnswerEvent, { type: 'sources' }>) => e.sources.map((s, i) => ({ n: i + 1, title: s.title, url: s.url })) },
        AgentEvent: { __resolveType: (e: AgentEvent) => (e.type === 'tool' ? 'AgentToolCall' : 'AgentToken') },
        Activity: { __resolveType: (e: Activity) => ({ document: 'DocumentActivity', searchable: 'Searchable', alert: 'NewAlert' })[e.type] },
      },
    }),
  });
}
