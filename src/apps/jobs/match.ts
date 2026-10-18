import { extractSkills } from './skills.ts';

export const JOB_SOURCES = ['greenhouse', 'lever', 'ashby'];

type Job = { title: string; feed: string; url: string | null; body: string; meta: { location?: string | null } };

export function rankJobs(resume: string, jobs: Job[], limit: number) {
  const mine = new Set(extractSkills(resume));
  if (!mine.size) throw new Error('found no known skills in the resume');

  const ranked = jobs
    .map((job) => {
      const wanted = extractSkills(`${job.title}\n${job.body}`);
      const have = wanted.filter((s) => mine.has(s));
      const missing = wanted.filter((s) => !mine.has(s));
      // F1 of the two skill sets: high only when you cover most of the job AND the job uses much of
      // your stack, so a sales role that mentions AWS and Redis can't outrank a real backend role.
      const score = (2 * have.length) / (wanted.length + mine.size);
      return { title: job.title, feed: job.feed, url: job.url, location: job.meta.location ?? null, have, missing, score };
    })
    .filter((j) => j.have.length > 0)
    .sort((a, b) => b.score - a.score || b.have.length - a.have.length)
    .slice(0, limit);

  const gaps = new Map<string, number>();
  for (const job of ranked) for (const skill of job.missing) gaps.set(skill, (gaps.get(skill) ?? 0) + 1);

  return { skills: [...mine], jobs: ranked, gaps: [...gaps].sort((a, b) => b[1] - a[1]) };
}
