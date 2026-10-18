import type { Doc } from '../core/store.ts';
import { decodeEntities, htmlToText } from './html.ts';

type Job = { id: number; title: string; absolute_url?: string; content?: string; location?: { name?: string } };

export const url = (feed: string) =>
  `https://boards-api.greenhouse.io/v1/boards/${encodeURIComponent(feed)}/jobs?content=true`;

export function normalize(data: { jobs?: Job[] }): Doc[] {
  if (!Array.isArray(data?.jobs)) throw new Error('unexpected Greenhouse response');
  return data.jobs.map((j) => ({
    externalId: String(j.id),
    title: j.title,
    url: j.absolute_url ?? null,
    body: htmlToText(decodeEntities(j.content ?? '')), // Greenhouse sends its HTML entity-escaped
    meta: { location: j.location?.name ?? null },
  }));
}
