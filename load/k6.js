// Load test for the GraphQL API: k6 run load/k6.js, or with Docker (see README).
// SCENARIO picks what to load: search (MODE=keyword|vector|hybrid|rerank), dashboard (the queries the
// live page polls) or cached (a repeated question the semantic cache answers). RATE is requests a
// second, held for DURATION after a 10 second ramp.
import http from 'k6/http';
import { check } from 'k6';

const API = __ENV.API_URL || 'http://localhost:4000/graphql';
const SCENARIO = __ENV.SCENARIO || 'search';
const MODE = __ENV.MODE || 'hybrid';
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '60s';

// Questions a developer might search for, phrased differently from any one document.
const QUERIES = [
  'how do teams avoid cache stampedes',
  'postgres connection pooling at scale',
  'kafka consumer lag after a deploy',
  'rust async runtime internals',
  'react server components data fetching',
  'prototype pollution in lodash',
  'zero downtime database migrations',
  'how does cloudflare d1 scale to millions of databases',
  'node.js security release',
  'running sqlite in production',
  'deterministic simulation testing',
  'typescript native compiler speed',
];

export const options = {
  scenarios: {
    [SCENARIO]: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: Math.max(10, RATE * 2),
      maxVUs: Math.max(50, RATE * 10),
      stages: [
        { target: RATE, duration: '10s' },
        { target: RATE, duration: DURATION },
      ],
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

const post = (query, variables) =>
  http.post(API, JSON.stringify({ query, variables }), { headers: { 'content-type': 'application/json', accept: 'application/json' } });

const pick = (list) => list[Math.floor(Math.random() * list.length)];

export default function () {
  let res;
  if (SCENARIO === 'search') {
    res = post('query Search($q: String!, $mode: Mode) { search(query: $q, mode: $mode, limit: 10) { title url } }', { q: pick(QUERIES), mode: MODE });
  } else if (SCENARIO === 'dashboard') {
    res = post('{ status { documents outbox pendingEmbeddings lag { group behind } syncs { active failed } } recent(limit: 40) { id at change document { title searchable } } }');
  } else if (SCENARIO === 'cached') {
    // The same question every time; the first request writes the answer, the rest come from the cache.
    res = http.post(API, JSON.stringify({ query: 'subscription { ask(question: "How does deterministic simulation testing find bugs?") { __typename ... on AnswerDone { cached } } }' }), {
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    });
    check(res, { 'served from the cache': (r) => r.body.includes('"cached":true') });
  } else {
    throw new Error(`unknown SCENARIO ${SCENARIO}`);
  }
  check(res, { 'status 200': (r) => r.status === 200, 'no GraphQL errors': (r) => !r.body.includes('"errors"') });
}
