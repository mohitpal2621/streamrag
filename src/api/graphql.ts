import { createSchema, createYoga } from 'graphql-yoga';
import { askAgent, createAgent, type AgentEvent } from '../agent/graph.ts';
import type { Deps } from '../agent/tools.ts';
import { ask, type AnswerEvent } from '../core/answer.ts';
import { search, type Mode } from '../core/search.ts';
import { pipelineStatus } from '../pipeline/status.ts';
import { breakingChanges } from '../upgrade.ts';

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
    at: String!
    package: String!
    title: String!
    url: String
  }

  type Status {
    outbox: Int!
    pendingEmbeddings: Int!
    alertsThisWeek: Int!
    "null when Kafka is unreachable"
    embedderLag: Int
    deadLetters: Int
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
  }

  type Subscription {
    "An answer from the synced sources: the numbered sources, then the text as it is written, then what it cited."
    ask(question: String!): AnswerEvent!
    "The agent's tool calls as it makes them, then its answer. Pass the same thread to ask a follow-up."
    agent(question: String!, thread: String): AgentEvent!
  }
`;

const snippet = (text: string) => text.slice(text.indexOf('\n') + 1, 300).replace(/\s+/g, ' ');

// GraphQL over HTTP, with GraphiQL at /graphql. Subscriptions stream over server-sent events, which
// Yoga speaks out of the box, so a plain fetch or EventSource can read an answer as it's written.
export function createApi(deps: Deps) {
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
            (await search(deps.db, a.query, { mode: a.mode, limit: a.limit, embed: deps.embed, rerank: deps.rerank })).map((h) => ({
              ...h,
              snippet: snippet(h.text),
            })),
          upgrade: async (_: unknown, a: { package: string; from: string; to?: string }) => ({
            package: a.package,
            from: a.from,
            ...(await breakingChanges(deps.db, deps.gateway, a.package, a.from, a.to)),
          }),
          alerts: async (_: unknown, a: { limit: number }) =>
            (
              await deps.db.query(
                `select a.created_at as at, d.feed, d.title, d.url from alerts a join documents d on d.id = a.document_id
                 order by a.id desc limit $1`,
                [a.limit],
              )
            ).rows.map((r) => ({ at: r.at.toISOString(), package: r.feed.replace(/^npm\//, ''), title: r.title, url: r.url })),
          status: async () => {
            const s = await pipelineStatus(deps.db);
            const k = 'unreachable' in s.kafka ? null : s.kafka;
            return { ...s, embedderLag: k?.embedder ?? null, deadLetters: k?.deadLetters ?? null };
          },
        },
        Subscription: {
          ask: { subscribe: (_: unknown, a: { question: string }) => ask(deps.db, a.question, deps), resolve: passThrough },
          agent: { subscribe: (_: unknown, a: { question: string; thread?: string }) => askAgent(agent, a.question, a.thread), resolve: passThrough },
        },
        AnswerEvent: { __resolveType: (e: AnswerEvent) => ({ sources: 'AnswerSources', token: 'AnswerToken', done: 'AnswerDone' })[e.type] },
        AnswerSources: { sources: (e: Extract<AnswerEvent, { type: 'sources' }>) => e.sources.map((s, i) => ({ n: i + 1, title: s.title, url: s.url })) },
        AgentEvent: { __resolveType: (e: AgentEvent) => (e.type === 'tool' ? 'AgentToolCall' : 'AgentToken') },
      },
    }),
  });
}
