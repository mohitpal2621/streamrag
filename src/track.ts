import { readFile } from 'node:fs/promises';
import { get } from './connectors/http.ts';
import type { Source } from './core/ingest.ts';

// npm's repository field comes as git+https://github.com/o/r.git, git://…, git@github.com:o/r, github:o/r or o/r.
export function githubRepo(repository: string | { url?: string } | undefined): string | null {
  const url = typeof repository === 'string' ? repository : repository?.url;
  const m = url?.match(/github\.com[/:]([\w.-]+)\/([\w.-]+?)(?:\.git)?(?:[#/].*)?$/) ?? url?.match(/^(?:github:)?([\w.-]+)\/([\w.-]+)$/);
  return m ? `${m[1]}/${m[2]}` : null;
}

// For every dependency: its security advisories, plus its GitHub release notes when the repo is known.
export async function sourcesForPackageJson(path: string) {
  const pkg = JSON.parse(await readFile(path, 'utf8'));
  const sources: Source[] = [];
  const noRepo: string[] = [];
  for (const name of Object.keys({ ...pkg.dependencies, ...pkg.devDependencies })) {
    sources.push({ connector: 'osv', feed: `npm/${name}` });
    const repo = await get(`https://registry.npmjs.org/${name}/latest`)
      .then(async (res) => githubRepo(((await res.json()) as { repository?: string | { url?: string } }).repository))
      .catch(() => null); // private and local packages aren't on the registry
    if (repo) sources.push({ connector: 'rss', feed: `https://github.com/${repo}/releases.atom` });
    else noRepo.push(name);
  }
  return { sources, noRepo };
}

// Packages in one monorepo share a releases feed, so duplicates are common.
export function mergeSources(current: Source[], incoming: Source[]) {
  const key = (s: Source) => `${s.connector} ${s.feed}`;
  const seen = new Set(current.map(key));
  const added: Source[] = [];
  for (const s of incoming) {
    if (seen.has(key(s))) continue;
    seen.add(key(s));
    added.push(s);
  }
  return added;
}
