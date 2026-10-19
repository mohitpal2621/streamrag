import type { Doc } from '../core/store.ts';
import { htmlToText } from './html.ts';
import { get } from './http.ts';

export const snapshot = false;

type Hit = {
  objectID: string;
  title?: string | null;
  url?: string | null;
  story_text?: string | null;
  comment_text?: string | null;
  story_title?: string | null;
  author?: string;
  points?: number | null;
  created_at?: string;
};

export function normalize(data: { hits?: Hit[] }): Doc[] {
  if (!Array.isArray(data?.hits)) throw new Error('unexpected Hacker News response');
  return data.hits.map((h) => ({
    externalId: h.objectID,
    title: h.title ?? `Re: ${h.story_title ?? 'Hacker News'}`,
    url: `https://news.ycombinator.com/item?id=${h.objectID}`,
    body: htmlToText(h.story_text ?? h.comment_text ?? ''),
    meta: { author: h.author ?? null, points: h.points ?? null, link: h.url ?? null, published: h.created_at ?? null },
  }));
}

// The feed is an Algolia tag: story, comment, show_hn, ask_hn or front_page.
export const fetchFeed = async (tag: string) =>
  normalize(await (await get(`https://hn.algolia.com/api/v1/search_by_date?tags=${encodeURIComponent(tag)}&hitsPerPage=1000`)).json() as { hits?: Hit[] });
