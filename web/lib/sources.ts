// How each source and feed reads on screen, and which color stands for it.
export type Kind = 'hackernews' | 'blog' | 'release' | 'advisory' | 'transcript';

export const KINDS: Record<Kind, string> = {
  hackernews: 'Hacker News',
  blog: 'Blog',
  release: 'Release notes',
  advisory: 'Advisory',
  transcript: 'Transcript',
};

// The pulse stacks by source rather than kind, since that is what the outbox records.
export const SOURCES: Record<string, { label: string; kind: Kind }> = {
  hackernews: { label: 'Hacker News', kind: 'hackernews' },
  rss: { label: 'Blogs, podcasts and releases', kind: 'blog' },
  github: { label: 'Release notes', kind: 'release' },
  osv: { label: 'Security advisories', kind: 'advisory' },
  transcript: { label: 'Transcripts', kind: 'transcript' },
};

const releases = /^https:\/\/github\.com\/([^/]+\/[^/]+)\/releases\.atom$/;
const host = (url: string) => {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return url;
  }
};

export function kindOf(source: string, feed: string): Kind {
  if (source === 'rss' && releases.test(feed)) return 'release';
  return SOURCES[source]?.kind ?? 'blog';
}

export function feedName(source: string, feed: string): string {
  if (source === 'hackernews') return `Hacker News ${feed === 'comment' ? 'comments' : feed === 'story' ? 'stories' : feed}`;
  if (source === 'osv') return `${feed.replace(/^npm\//, '')} advisories`;
  if (source === 'github') return `${feed} releases`;
  if (source === 'transcript') return `${host(feed)} transcripts`;
  const repo = feed.match(releases)?.[1];
  return repo ? `${repo} releases` : host(feed);
}
