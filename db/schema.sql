create extension if not exists vector;

-- Every source (feeds and APIs today, media later) lands in this one table.
create table if not exists documents (
  id            bigserial primary key,
  source        text not null,          -- connector name, e.g. rss
  feed          text not null,          -- one stream within a source, e.g. a blog's feed URL
  external_id   text not null,
  title         text not null,
  url           text,
  body          text not null,
  meta          jsonb not null default '{}',
  content_hash  text not null,
  embedded_hash text,                                -- content_hash the current chunks were made from
  extracted_hash text,                               -- content_hash its breaking changes were read from
  first_seen_at timestamptz not null default now(),
  updated_at    timestamptz not null default now(),  -- last time the content changed
  last_seen_at  timestamptz not null default now(),
  closed_at     timestamptz,                         -- set when the feed stops listing it
  unique (source, feed, external_id)
);

-- Upgrades a phase 1 database: search moved from whole documents to chunks.
alter table documents add column if not exists embedded_hash text;
alter table documents drop column if exists tsv;
alter table documents add column if not exists extracted_hash text;

create index if not exists documents_open_idx on documents (source, feed) where closed_at is null;

-- Retrieval works on chunks: overlapping windows of a document, each starting with its title.
create table if not exists chunks (
  id          bigserial primary key,
  document_id bigint not null references documents (id) on delete cascade,
  ord         int not null,
  text        text not null,
  embedding   vector(384) not null,
  tsv         tsvector generated always as (to_tsvector('english', text)) stored,
  unique (document_id, ord)
);

create index if not exists chunks_embedding_idx on chunks using hnsw (embedding vector_cosine_ops);
create index if not exists chunks_tsv_idx on chunks using gin (tsv);

-- Semantic cache: a new question close enough to an earlier one reuses its answer, as long as
-- every document that answer cites is unchanged.
create table if not exists answers (
  id         bigserial primary key,
  question   text not null,
  embedding  vector(384) not null,
  answer     text not null,
  sources    jsonb not null,  -- the numbered sources in prompt order: documentId, title, url, hash
  cited      int[] not null,  -- which of those numbers the answer cites
  provider   text not null,
  created_at timestamptz not null default now()
);

create index if not exists answers_embedding_idx on answers using hnsw (embedding vector_cosine_ops);

-- Breaking changes a model read out of release notes, replaced whenever the notes change.
create table if not exists breaking_changes (
  document_id bigint not null references documents (id) on delete cascade,
  ord         int not null,
  change      text not null,
  migration   text not null,  -- what to do instead; empty when the notes don't say
  primary key (document_id, ord)
);

-- One row per model call or skipped provider, for latency and token usage.
create table if not exists llm_calls (
  id            bigserial primary key,
  provider      text not null,
  model         text not null,
  status        text not null,  -- ok, error or skipped
  error         text,
  latency_ms    int,
  input_tokens  int,
  output_tokens int,
  created_at    timestamptz not null default now()
);
