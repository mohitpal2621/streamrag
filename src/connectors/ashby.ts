import type { Doc } from '../core/store.ts';

type Job = { id: string; title: string; jobUrl?: string; descriptionPlain?: string; location?: string; isListed?: boolean };

export const url = (feed: string) => `https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(feed)}`;

export function normalize(data: { jobs?: Job[] }): Doc[] {
  if (!Array.isArray(data?.jobs)) throw new Error('unexpected Ashby response');
  return data.jobs
    .filter((j) => j.isListed !== false)
    .map((j) => ({
      externalId: j.id,
      title: j.title,
      url: j.jobUrl ?? null,
      body: j.descriptionPlain ?? '',
      meta: { location: j.location ?? null },
    }));
}
