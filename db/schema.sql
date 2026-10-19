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
  first_seen_at timestamptz not null default now(),
  updated_at    timestamptz not null default now(),  -- last time the content changed
  last_seen_at  timestamptz not null default now(),
  closed_at     timestamptz,                         -- set when the feed stops listing it
  tsv           tsvector generated always as (
                  setweight(to_tsvector('english', title), 'A') ||
                  setweight(to_tsvector('english', body), 'B')
                ) stored,
  unique (source, feed, external_id)
);

create index if not exists documents_tsv_idx on documents using gin (tsv);
create index if not exists documents_open_idx on documents (source, feed) where closed_at is null;
