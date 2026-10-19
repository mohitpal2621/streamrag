# streamrag

A real-time RAG engine. It keeps a constantly changing knowledge base in sync and searchable, and later phases add hybrid search, cited answers and agents on top.

It ships with three kinds of sources: engineering blogs and release notes over RSS, Hacker News as a live stream, and a dependency radar that follows the release notes and security advisories of every package in your `package.json`.

Everything runs locally on free, open-source tools.

## What works today (phase 1)

- Connectors for RSS and Atom feeds, Hacker News (through its Algolia API) and OSV.dev security advisories. The default `sources.json` follows 30 engineering and release blogs plus the latest Hacker News stories and comments, about 5,600 documents on a first run.
- Dependency radar. `npm run track -- path/to/package.json` adds each dependency's security advisories and, when the package lives on GitHub, its release notes.
- Incremental sync. A content hash skips unchanged documents and catches edited ones. Snapshot sources list everything that exists, so whatever they stop listing gets closed. Append-only feeds like RSS and Hacker News keep items open after they roll off. A source that fails to fetch is skipped and left as it was.
- Full-text search in Postgres: a weighted `tsvector`, a GIN index, and ranked results with highlighted snippets.

## Quick start

Needs Node 22.18+ and Docker.

```bash
npm install
docker compose up -d
npm run migrate
npm run ingest                            # about 45 seconds for the default sources
npm run search -- "rate limiting redis"
npm run track -- ../my-app/package.json   # then run npm run ingest again
```

Run `npm run ingest` again later and only the changes are written.

## How it's built

```
src/
  core/         source-agnostic storage, sync and search (Postgres)
  connectors/   one file per source type: rss, hackernews, osv
  track.ts      turns a package.json into sources
  cli.ts
db/schema.sql
```

- Every source lands in one `documents` table, keyed by `(source, feed, external_id)`.
- A sync is one batch upsert (`unnest` + `ON CONFLICT`) in one transaction. `now()` is fixed for the whole transaction, so the rows the sync touched show up as `= now()`, and a snapshot sync closes everything else in that same transaction.
- Search ranks first and builds snippets only for the rows that make the cut, because `ts_headline` is slow.
- Fetching is polite: one request a second, an identifying User-Agent, public endpoints only.
- The tests run against a real Postgres through Testcontainers, locally and in GitHub Actions.

## Roadmap

1. Core sync, full-text search, RSS, Hacker News and the dependency radar (done)
2. Embeddings with pgvector, hybrid keyword + vector search, reranking, and an eval set that measures retrieval quality
3. LLM layer: a gateway across Gemini, Groq and local Ollama with rate limiting, retries, fallback and a semantic cache, plus answers with citations, such as "what breaks if I upgrade Next.js?"
4. Event pipeline: Kafka (Redpanda) with Hacker News as the live stream, the outbox pattern, idempotent consumers, a dead-letter topic, BullMQ schedules and Redis locks
5. Media connector: FFmpeg and Whisper turn dev podcasts and conference talks into timestamped, searchable text, and answers link to the exact moment
6. APIs: GraphQL, streamed answers, a LangGraph agent and an MCP server, so coding assistants can ask about your dependencies
7. Web dashboard (Next.js) with live updates
8. Observability and load testing: OpenTelemetry, Prometheus, Grafana, k6 and Playwright

## Data

Posts, release notes, advisories and Hacker News items come from public feeds and APIs. streamrag stores them for search and links back to each original. It doesn't republish them.
