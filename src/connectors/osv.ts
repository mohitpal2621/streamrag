import type { Doc } from '../core/store.ts';
import { get } from './http.ts';

export const snapshot = true; // OSV returns every advisory for the package, so a missing one was withdrawn

type Vuln = {
  id: string;
  summary?: string;
  details?: string;
  aliases?: string[];
  published?: string;
  modified?: string;
  withdrawn?: string;
  database_specific?: { severity?: string };
  affected?: { package?: { name?: string }; ranges?: { type?: string; events?: { introduced?: string; fixed?: string }[] }[] }[];
};

export function normalize(vulns: Vuln[], ecosystem: string, name: string): Doc[] {
  return vulns
    .filter((v) => !v.withdrawn)
    .map((v) => {
      // Only this package's version ranges; GIT ranges list commit hashes, not versions.
      const ranges = v.affected?.filter((a) => a.package?.name === name).flatMap((a) => a.ranges ?? []) ?? [];
      const fixed = [...new Set(ranges.filter((r) => r.type !== 'GIT').flatMap((r) => r.events?.flatMap((e) => e.fixed ?? []) ?? []))];
      return {
        externalId: v.id,
        title: `${name}: ${v.summary ?? v.id}`,
        url: `https://osv.dev/vulnerability/${v.id}`,
        body: [v.details, fixed.length && `Fixed in: ${fixed.join(', ')}`, v.aliases?.length && `Also known as: ${v.aliases.join(', ')}`]
          .filter(Boolean)
          .join('\n\n'),
        meta: { ecosystem, package: name, severity: v.database_specific?.severity ?? null, published: v.published ?? null },
      };
    });
}

// The feed is "<ecosystem>/<package>", e.g. npm/next or npm/@nestjs/core.
export async function fetchFeed(feed: string) {
  const slash = feed.indexOf('/');
  const [ecosystem, name] = [feed.slice(0, slash), feed.slice(slash + 1)];
  if (slash < 1 || !name) throw new Error(`osv feed must look like npm/<package>, got "${feed}"`);
  const vulns: Vuln[] = [];
  let pageToken: string | undefined;
  do {
    const res = await get('https://api.osv.dev/v1/query', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ package: { ecosystem, name }, page_token: pageToken }),
    });
    const page = (await res.json()) as { vulns?: Vuln[]; next_page_token?: string };
    vulns.push(...(page.vulns ?? []));
    pageToken = page.next_page_token;
  } while (pageToken);
  return normalize(vulns, ecosystem, name);
}
