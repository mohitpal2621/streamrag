# streamrag

A real-time RAG engine. It keeps a constantly changing set of documents indexed and searchable, and (in later phases) answers questions over them with cited sources.

The first use case is the job market. streamrag pulls live postings from public job boards and matches them against a resume, showing which jobs fit and which skills you're missing.

Everything runs locally on free, open-source tools.

## What works today (phase 1)

- Connectors for the public job-board APIs of Greenhouse, Lever and Ashby. The default `sources.json` covers 49 companies, about 9,500 open postings.
- Idempotent sync. Each run writes a feed's full listing in one transaction. A content hash skips unchanged postings, edited ones get updated, and postings that disappear are closed (and reopened if they come back). A feed that fails to fetch is skipped, so a network error never wipes it.
- Full-text search in Postgres: weighted `tsvector`, a GIN index, ranked results with highlighted snippets.
- Resume matching, the jobs app. It ranks open jobs by the F1 overlap between your skills and the job's, then lists the skills you're missing most often.

## Quick start

Needs Node 22.18+ and Docker.

```bash
npm install
docker compose up -d
npm run migrate
npm run ingest                                        # about 90 seconds for all 49 feeds
npm run search -- "kafka exactly once"
npm run match -- resume.txt --location "india|remote" # resume.txt: your resume as plain text
```

Run `npm run ingest` again later and only the changes are written.

## How it's built

```
src/
  core/         source-agnostic storage, sync and search (Postgres)
  connectors/   one file per source, each turning an API response into documents
  apps/jobs/    the first use case: skill extraction and resume matching
  cli.ts
db/schema.sql
```

- Every source lands in one `documents` table, keyed by `(source, feed, external_id)`.
- Syncs are a batch upsert (`unnest` + `ON CONFLICT`). `now()` is fixed for the whole transaction, so rows the sync touched show up as `= now()`, and anything it didn't see is closed in the same transaction.
- Search ranks first and builds snippets only for the rows that make the cut, because `ts_headline` is slow.
- Crawling is polite: one request per second, an identifying User-Agent, official public endpoints only.
- The tests run against a real Postgres through Testcontainers, locally and in GitHub Actions.

## Roadmap

1. Core sync, full-text search and job matching (done)
2. Embeddings with pgvector, hybrid keyword + vector search, reranking, and an eval set that measures retrieval quality
3. LLM layer: a gateway across Gemini, Groq and local Ollama with rate limiting, retries, fallback and a semantic cache; structured extraction; answers with citations
4. Event pipeline: Kafka (Redpanda), the outbox pattern, idempotent consumers, a dead-letter topic, BullMQ schedules and Redis locks
5. Media connector: FFmpeg and Whisper turn video and audio into timestamped, searchable text, and answers link to the exact clip
6. APIs: GraphQL, streamed answers, a LangGraph agent and an MCP server
7. Web dashboard (Next.js) with live updates
8. Observability and load testing: OpenTelemetry, Prometheus, Grafana, k6 and Playwright

## Data

Job postings come from the public APIs that Greenhouse, Lever and Ashby run for companies' careers pages. streamrag stores them for search and links back to each original posting. It doesn't republish them.
