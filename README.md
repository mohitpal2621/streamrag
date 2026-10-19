# streamrag

A real-time RAG engine. It keeps a constantly changing knowledge base in sync, searches it with hybrid keyword and vector search plus reranking, and answers questions from it with citations. Later phases add an event pipeline and agents on top.

It ships with three kinds of sources: engineering blogs and release notes over RSS, Hacker News as a live stream, and a dependency radar that follows the release notes and security advisories of every package in your `package.json`.

Everything runs on free, open-source tools, including the models. Answers come from a model on your own machine through Ollama, and the free tiers of Gemini and Groq can be added as fallbacks.

## What works today (phases 1 to 3)

- Connectors for RSS and Atom feeds, Hacker News (through its Algolia API) and OSV.dev security advisories. The default `sources.json` follows 30 engineering and release blogs plus the latest Hacker News stories and comments, about 5,600 documents on a first run.
- Dependency radar. `npm run track -- path/to/package.json` adds each dependency's security advisories and, when the package lives on GitHub, its release notes.
- Incremental sync. A content hash skips unchanged documents and catches edited ones. Snapshot sources list everything that exists, so whatever they stop listing gets closed. Append-only feeds like RSS and Hacker News keep items open after they roll off. A source that fails to fetch is skipped and left as it was.
- Incremental embedding. Only new or changed documents get re-chunked and re-embedded, a batch at a time, so an interrupted run resumes where it stopped.
- Hybrid search with reranking, measured by an eval set (below).
- Answers with citations. `npm run ask` streams an answer written from the top six search results and lists the sources it cites.
- An LLM gateway with retries, fallback across providers, a circuit breaker and per-minute rate limits. Every model call is logged, and `npm run usage` sums them up.
- A semantic cache. The same question asked again, even lightly reworded, gets the earlier answer back in under a second, as long as the documents it cites haven't changed.

## Quick start

Needs Node 22.18+ and Docker, plus [Ollama](https://ollama.com) for answers.

```bash
npm install
docker compose up -d
npm run migrate
npm run ingest                            # sync, then embed what changed
npm run search -- "how do teams avoid cache stampedes"
ollama pull llama3.2:3b                   # the default answer model, about 2 GB
npm run ask -- "how does Cloudflare D1 scale to millions of databases?"
npm run usage                             # model calls, errors, latency and tokens
npm run track -- ../my-app/package.json   # then run npm run ingest again
npm run eval                              # score every search mode
```

The first `ingest` downloads two small models (about 56 MB, kept in `node_modules`) and embeds every document once. For the default sources that's about 15,000 chunks, which took around half an hour on a 2-core machine and goes faster with more cores. After that, each run only embeds what changed.

`npm run search` takes `--mode keyword|vector|hybrid|rerank` (default `rerank`) and `-n` for the number of results.

Settings live in an optional `.env` file, and `.env.example` lists them: another Ollama model or address, Gemini and Groq keys, and the order to try providers in.

## How search works

1. Each document is split into overlapping ~200-word chunks, and every chunk starts with the document's title, so it still says what it's about when retrieved alone.
2. Every chunk gets a 384-dimension embedding from `bge-small-en-v1.5`, stored in pgvector with an HNSW index, and a `tsvector` with a GIN index.
3. A query runs both legs and fuses their rankings with reciprocal rank fusion in one SQL statement. Each leg contributes `1 / (60 + rank)`, so a chunk both legs agree on wins, and raw scores from the two legs, which aren't comparable, never get mixed.
4. A cross-encoder (`ms-marco-MiniLM-L-6-v2`) reads the query together with each of the top 30 chunks and reorders them. It is slower than comparing embeddings but much better at judging relevance.
5. Results come back as documents, each with its best chunk.

Both models run on the CPU through transformers.js, with no API keys.

## How answers work

1. The question goes through the same reranked search. The top six results, one chunk from each document, become numbered sources in the prompt, around 1,700 tokens in all.
2. The model is told to cite every claim as `[n]` and to say so when the sources don't answer the question. The answer streams to the terminal as it's written, and citations to numbers that weren't in the prompt are dropped and reported.

A gateway sits between `ask` and the models:

- It tries providers in the order of `LLM_PROVIDERS`: Ollama first by default, then Gemini and Groq, each only when its key is set.
- A 429, a 5xx or a reset connection gets up to three attempts, with exponential backoff and full jitter, waiting at least as long as `Retry-After` asks. After a bad key, a refused connection or a `Retry-After` over 10 seconds (a free quota that's used up), it moves on to the next provider at once.
- Switching provider is only safe before the first token. Once text has streamed, a failure ends the answer with an error, because a second provider would start the answer over.
- Three failed calls in a row take a provider out of rotation for 30 seconds.
- A sliding one-minute window keeps each free tier under its request limit. A request that would wait more than 2 seconds for a slot goes to the next provider instead.
- Every call, error and skip lands in the `llm_calls` table, which `npm run usage` sums over the last week.

The cache embeds each question and looks for one asked in the last day within cosine distance 0.05. A hit only counts while every document the answer cites is still open with the same content hash, so an edited release note or a withdrawn advisory forces a fresh answer. Answers that cite nothing aren't cached, because "the sources don't say" can stop being true after the next sync.

On the live corpus, a reworded question ("how does cloudflare's D1 scale to millions of databases") was 0.012 away from the cached one and hit. Questions about something else, like "How does Cloudflare D1 handle backups?" or "How does Turso scale to millions of databases?", were 0.18 or more away and missed. A heavier rewording at 0.11 missed too. The threshold is tight on purpose, since a miss costs one model call and a wrong hit serves a confident answer to a different question.

On 2 CPU cores with no GPU, Llama 3.2 3B took about a minute per answer, around 50 seconds of it spent reading the prompt, and a cached repeat took under a second. In test questions it cited its sources whenever it answered from them, and said plainly when they didn't cover the question. Qwen 2.5 0.5B, tried for comparison, never cited anything, so none of its answers could be cached.

Gemini and Groq keys are free to create. Leave billing off on both accounts and they can't charge you. When a free quota runs out, the gateway moves on to the next provider. Google may use what you send to the free Gemini API to improve its products, which is fine for public blog posts but worth knowing.

## Eval

`npm run eval` starts its own Postgres in Docker, pulls the current OSV advisories for 40 popular npm packages, embeds them, and asks 56 questions in every mode. The questions are written the way a developer would ask them, mostly without the advisory's own wording, and each lists the advisories that answer it. A few are bare CVE IDs, which keyword search is good at and embeddings are not.

Results on 557 advisories, October 2026:

| mode | recall@1 | recall@5 | recall@10 | MRR@10 |
|---|---|---|---|---|
| keyword | 0.11 | 0.14 | 0.14 | 0.13 |
| vector | 0.88 | 0.91 | 0.95 | 0.89 |
| hybrid | 0.88 | 0.96 | 1.00 | 0.91 |
| rerank | 0.88 | 0.98 | 1.00 | 0.92 |

Recall@k is the share of questions whose answer shows up in the top k results. MRR rewards putting it first. The corpus grows as new advisories are published, so the numbers move a little over time.

Keyword search on its own needs every word of a question in one chunk, so it rarely finds an answer to a natural question. Inside hybrid search the strictness helps: it fires on exact names and IDs, which the vectors miss, and lifts recall@10 from 0.95 to 1.00.

Two changes the eval decided:

- Matching any word instead of all of them lifted keyword search alone from 0.14 to 0.77 recall@10, but dropped hybrid from 1.00 to 0.93 and its MRR from 0.91 to 0.74, because fusion trusts a noisy leg as much as a precise one. It was reverted.
- The reranker pushed bare CVE IDs out of the top 10, since it was trained on natural questions. Queries that look like an identifier now keep the hybrid order, which brought rerank's recall@10 from 0.96 to 1.00. Those IDs still land at ranks 2 to 4 rather than first.

## How it's built

```
src/
  core/         storage, sync, chunking, embedding, search and answers (Postgres + pgvector)
  connectors/   one file per source type: rss, hackernews, osv
  llm/          the gateway, a streaming client for OpenAI-compatible APIs, provider settings
  track.ts      turns a package.json into sources
  cli.ts
  eval.ts       the retrieval eval
db/schema.sql
eval/queries.json
```

- Every source lands in one `documents` table, keyed by `(source, feed, external_id)`. Retrieval works on a `chunks` table.
- A sync is one batch upsert (`unnest` + `ON CONFLICT`) in one transaction. `now()` is fixed for the whole transaction, so the rows the sync touched show up as `= now()`, and a snapshot sync closes everything else in that same transaction.
- `embedded_hash` records which version of a document its chunks came from, so changed documents get picked up again and nothing else does.
- Vector search uses pgvector 0.8's iterative index scans, so filtering out closed documents can't leave a result list short.
- The cache is a table too. Each row of `answers` keeps the question's embedding (HNSW-indexed), the numbered sources with their content hashes, and which of them the answer cited.
- Ollama, Gemini and Groq all offer an OpenAI-compatible chat API, so one small client (`fetch` plus a server-sent events parser) covers all three without an SDK.
- Fetching is polite: one request a second, an identifying User-Agent, public endpoints only.
- The tests run against a real Postgres through Testcontainers, locally and in GitHub Actions, with stand-in models and a scripted gateway, so CI downloads nothing and calls no model.

## Roadmap

1. Core sync, full-text search, RSS, Hacker News and the dependency radar (done)
2. Embeddings with pgvector, hybrid keyword + vector search, reranking, and an eval set that measures retrieval quality (done)
3. LLM layer: a gateway across local Ollama, Gemini and Groq with rate limits, retries, fallback and a circuit breaker, a semantic cache, and streamed answers with citations (done). Still to come: pulling breaking changes out of release notes as structured data, for questions like "what breaks if I upgrade Next.js?", and an answer-quality eval with promptfoo
4. Event pipeline: Kafka (Redpanda) with Hacker News as the live stream, the outbox pattern, idempotent consumers, a dead-letter topic, BullMQ schedules and Redis locks
5. Media connector: FFmpeg and Whisper turn dev podcasts and conference talks into timestamped, searchable text, and answers link to the exact moment
6. APIs: GraphQL, streamed answers, a LangGraph agent and an MCP server, so coding assistants can ask about your dependencies
7. Web dashboard (Next.js) with live updates
8. Observability and load testing: OpenTelemetry, Langfuse, Prometheus, Grafana, k6 and Playwright

## Data

Posts, release notes, advisories and Hacker News items come from public feeds and APIs. streamrag stores them for search and links back to each original. It doesn't republish them.
