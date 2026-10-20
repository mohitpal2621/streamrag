export type Document = { id: string; source: string; feed: string; title: string; url: string | null; snippet: string; searchable: boolean };
export type DocumentActivity = { id: string; at: string; change: 'added' | 'changed' | 'closed'; document: Document };
export type Alert = { id: string; at: string; package: string; title: string; url: string | null };
export type Activity =
  | ({ __typename: 'DocumentActivity' } & DocumentActivity)
  | { __typename: 'Searchable'; at: string; documentIds: string[] }
  | { __typename: 'NewAlert'; alert: Alert };
export type PulseBucket = { at: string; source: string; documents: number };
export type Status = {
  documents: number;
  chunks: number;
  outbox: number;
  oldestOutboxSeconds: number | null;
  pendingEmbeddings: number;
  alertsThisWeek: number;
  lag: { group: string; behind: number }[] | null;
  deadLetters: number | null;
  kafkaError: string | null;
  syncs: { scheduled: number; waiting: number; active: number; delayed: number; failed: number } | null;
  redisError: string | null;
};
export type Feed = { source: string; feed: string; documents: number; syncedAt: string; nextSyncAt: string | null };
export type ModelUsage = {
  provider: string;
  model: string;
  ok: number;
  errors: number;
  skipped: number;
  avgMs: number | null;
  p95Ms: number | null;
  inputTokens: number;
  outputTokens: number;
};
export type Snapshot = { status: Status; recent: DocumentActivity[]; pulse: PulseBucket[]; feeds: Feed[]; usage: ModelUsage[]; alerts: Alert[] };

export const FEED_ROWS = 40;

const DOCUMENT = 'id source feed title url snippet searchable';
export const ACTIVITY_FIELDS = `id at change document { ${DOCUMENT} }`;
export const ALERT_FIELDS = 'id at package title url';
export const STATUS_QUERY = `status {
  documents chunks outbox oldestOutboxSeconds pendingEmbeddings alertsThisWeek
  lag { group behind } deadLetters kafkaError syncs { scheduled waiting active delayed failed } redisError
}`;
export const FEEDS_QUERY = 'feeds { source feed documents syncedAt nextSyncAt }';
export const USAGE_QUERY = 'usage { provider model ok errors skipped avgMs p95Ms inputTokens outputTokens }';
export const SNAPSHOT_QUERY = `{
  ${STATUS_QUERY}
  recent(limit: ${FEED_ROWS}) { ${ACTIVITY_FIELDS} }
  pulse(minutes: 15) { at source documents }
  ${FEEDS_QUERY}
  ${USAGE_QUERY}
  alerts(limit: 8) { ${ALERT_FIELDS} }
}`;
export const ACTIVITY_SUBSCRIPTION = `subscription {
  activity {
    __typename
    ... on DocumentActivity { ${ACTIVITY_FIELDS} }
    ... on Searchable { at documentIds }
    ... on NewAlert { alert { ${ALERT_FIELDS} } }
  }
}`;
