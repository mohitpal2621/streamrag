import { XMLParser } from 'fast-xml-parser';
import type { Doc } from '../core/store.ts';
import { decodeEntities, htmlToText } from './html.ts';
import { get } from './http.ts';

export const snapshot = false; // feeds only show their latest items; older ones still exist

// Text-bearing tags stay raw, because they come three ways: entity-escaped HTML, CDATA, or inline XHTML.
const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@',
  parseTagValue: false, // keep "2024" a string, not a number
  isArray: (name) => name === 'item' || name === 'entry' || name === 'link',
  stopNodes: ['*.title', '*.description', '*.content', '*.summary', '*.content:encoded'],
});

// The parser returns untyped data. A tag with attributes becomes { '#text', '@attr' }, a plain one a string.
const raw = (v: any): string => (typeof v === 'string' ? v : (v?.['#text'] ?? ''));

// Atom's type attribute says what a tag holds: text, html or inline xhtml. RSS has no attribute:
// its titles are text and its descriptions are HTML, which is what the fallback covers.
function toText(v: any, fallbackType: 'text' | 'html'): string {
  const type = v?.['@type'] ?? fallbackType;
  const xml = raw(v);
  if (type === 'xhtml') return htmlToText(xml);
  const unwrapped = xml
    .split(/(<!\[CDATA\[[\s\S]*?\]\]>)/)
    .map((part) => (part.startsWith('<![CDATA[') ? part.slice(9, -3) : decodeEntities(part)))
    .join('');
  // A text title like "<script> tags" must keep its brackets, so only HTML gets its tags stripped.
  return type === 'text' ? decodeEntities(unwrapped).replace(/\s+/g, ' ').trim() : htmlToText(unwrapped);
}

// RSS has <link>url</link>; Atom has <link rel="alternate" href="url"/>.
function linkOf(links: any[] = []): string | null {
  const plain = links.find((l) => typeof l === 'string');
  if (plain) return plain;
  return (links.find((l) => !l['@rel'] || l['@rel'] === 'alternate') ?? links[0])?.['@href'] ?? null;
}

export function normalize(xml: string): Doc[] {
  const doc = parser.parse(xml);
  const items = doc.rss?.channel ? (doc.rss.channel.item ?? []) : doc.feed ? (doc.feed.entry ?? []) : null;
  if (!items) throw new Error('not an RSS or Atom feed');
  return items.flatMap((item: any) => {
    const url = linkOf(item.link);
    const id = raw(item.guid) || raw(item.id) || url;
    if (!id) return [];
    return [{
      externalId: id,
      title: toText(item.title, 'text'),
      url,
      body: toText(item['content:encoded'] || item.description || item.content || item.summary, 'html'),
      meta: { published: raw(item.pubDate) || raw(item.published) || raw(item.updated) || null },
    }];
  });
}

export const fetchFeed = async (url: string) => normalize(await (await get(url)).text());
