import type { Doc } from '../core/store.ts';
import { htmlToText } from './html.ts';

type Posting = {
  id: string;
  text: string;
  hostedUrl?: string;
  descriptionPlain?: string;
  additionalPlain?: string;
  lists?: { text: string; content: string }[];
  categories?: { location?: string };
};

export const url = (feed: string) => `https://api.lever.co/v0/postings/${encodeURIComponent(feed)}?mode=json`;

export function normalize(data: Posting[]): Doc[] {
  if (!Array.isArray(data)) throw new Error('unexpected Lever response');
  return data.map((p) => ({
    externalId: p.id,
    title: p.text,
    url: p.hostedUrl ?? null,
    // The requirements usually sit in `lists`, not in the description.
    body: [p.descriptionPlain, ...(p.lists ?? []).map((l) => `${l.text}\n${htmlToText(l.content)}`), p.additionalPlain]
      .filter(Boolean)
      .join('\n\n')
      .trim(),
    meta: { location: p.categories?.location ?? null },
  }));
}
