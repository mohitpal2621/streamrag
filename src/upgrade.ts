import type pg from 'pg';
import { connectors } from './connectors/index.ts';
import { get } from './connectors/http.ts';
import { extractPending, type Breaking } from './core/extract.ts';
import { ingest } from './core/ingest.ts';
import { syncFeed } from './core/store.ts';
import type { Gateway } from './llm/gateway.ts';
import { githubRepo } from './track.ts';

type Version = [number, number, number];

// Takes 4, 4.2, 4.2.1 or v4.2.1. Prereleases like 5.0.0-beta.1 don't parse, so they are never picked.
export function parseVersion(v: string): Version | null {
  const m = v.trim().match(/^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?$/);
  return m ? [Number(m[1]), Number(m[2] ?? 0), Number(m[3] ?? 0)] : null;
}

const compare = (a: Version, b: Version) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2];

// The releases an upgrade crosses: each stable major and minor after `from`, up to and including `to`.
// shortcut: patch releases are skipped, since semver keeps breaking changes out of them and a big
// project ships dozens; read them too if a package is known to break things in patches.
export function releasesToCheck(versions: string[], from: string, to: string): string[] {
  const lo = parseVersion(from);
  const hi = parseVersion(to);
  if (!lo || !hi) throw new Error(`versions look like 4, 4.2 or 4.2.1, not "${lo ? to : from}"`);
  return versions
    .filter((v) => /^\d+\.\d+\.0$/.test(v))
    .map((v) => ({ v, p: v.split('.').map(Number) as Version }))
    .filter(({ p }) => compare(p, lo) > 0 && compare(p, hi) <= 0)
    .sort((a, b) => compare(a.p, b.p))
    .map(({ v }) => v);
}

type Release = { tag_name: string; name: string | null; body: string | null; html_url: string; published_at: string | null };

// Tries each way a project might tag the version until one exists. Without a token GitHub allows
// 60 API requests an hour, which is plenty here since a release is only ever fetched once.
async function releaseByTag(repo: string, tags: string[]): Promise<Release | null> {
  const token = process.env.GITHUB_TOKEN;
  const headers = { accept: 'application/vnd.github+json', ...(token ? { authorization: `Bearer ${token}` } : {}) };
  for (const tag of tags) {
    try {
      return (await (await get(`https://api.github.com/repos/${repo}/releases/tags/${encodeURIComponent(tag)}`, { headers })).json()) as Release;
    } catch (err) {
      const res = (err as { res?: Response }).res;
      if (res?.status === 404) continue;
      if (res?.status === 401) throw new Error('GitHub turned down the GITHUB_TOKEN (HTTP 401); fix it or unset it');
      if (res?.headers.get('x-ratelimit-remaining') === '0') {
        const reset = new Date(Number(res.headers.get('x-ratelimit-reset')) * 1000).toLocaleTimeString();
        throw new Error(`GitHub's API limit for this hour is used up (it resets at ${reset}); a GITHUB_TOKEN raises it`);
      }
      throw err;
    }
  }
  return null;
}

const findByUrl = async (db: pg.Pool, urls: string[]) =>
  (await db.query<{ id: string }>('select id from documents where url = any($1::text[]) order by id limit 1', [urls])).rows[0]?.id;

export type ReleaseCheck = { version: string; url?: string; short?: boolean; changes?: Breaking[]; error?: string };

// What breaks when `name` goes from one version to another, read from the release notes of every
// major and minor in between.
export async function breakingChanges(db: pg.Pool, gateway: Gateway, name: string, from: string, to?: string, onRead?: (title: string) => void) {
  const npm = `https://registry.npmjs.org/${name.replace('/', '%2F')}`;
  // The abbreviated document lists every version at a fraction of the full one's size, but has no repo.
  const res = await get(npm, { headers: { accept: 'application/vnd.npm.install-v1+json' } }).catch((err) => {
    throw (err as { res?: Response }).res?.status === 404 ? new Error(`npm has no package called ${name}`) : err;
  });
  const info = (await res.json()) as { versions: Record<string, unknown>; 'dist-tags': { latest: string } };
  const repo = githubRepo(((await (await get(`${npm}/latest`)).json()) as { repository?: string | { url?: string } }).repository);
  if (!repo) throw new Error(`${name} lists no GitHub repository on npm, so there are no release notes to read`);
  const target = to ?? info['dist-tags'].latest;
  const versions = releasesToCheck(Object.keys(info.versions), from, target);

  // The releases feed has the newest ten for one request; anything older comes from the API by tag.
  await ingest(db, [{ connector: 'rss', feed: `https://github.com/${repo}/releases.atom` }], connectors, 0);
  const ids = new Map<string, string>();
  const failed = new Map<string, string>();
  let apiError: string | undefined; // once the API fails, later releases get the same reason without asking again
  for (const version of versions) {
    const tags = [`v${version}`, version, `${name}@${version}`];
    let id = await findByUrl(db, tags.map((t) => `https://github.com/${repo}/releases/tag/${t}`));
    if (!id && apiError) failed.set(version, apiError);
    else if (!id) {
      try {
        const r = await releaseByTag(repo, tags);
        if (r) {
          const doc = { externalId: r.tag_name, title: r.name || r.tag_name, url: r.html_url, body: r.body ?? '', meta: { published: r.published_at } };
          await syncFeed(db, 'github', repo, [doc], false);
          id = await findByUrl(db, [r.html_url]);
        }
      } catch (err) {
        apiError = err instanceof Error ? err.message : String(err);
        failed.set(version, apiError);
      }
    }
    if (id) ids.set(version, id);
  }

  await extractPending(db, gateway, [...ids.values()], onRead);
  const { rows } = await db.query<{ id: string; url: string; short: boolean; changes: Breaking[] }>(
    `select d.id, d.url, length(d.body) < 300 as short,
            coalesce(json_agg(json_build_object('change', b.change, 'migration', b.migration) order by b.ord)
                     filter (where b.ord is not null), '[]') as changes
     from documents d left join breaking_changes b on b.document_id = d.id
     where d.id = any($1::bigint[]) group by d.id`,
    [[...ids.values()]],
  );
  const byId = new Map(rows.map((r) => [r.id, r]));
  const releases: ReleaseCheck[] = versions.map((version) => {
    const r = byId.get(ids.get(version) ?? '');
    if (r) return { version, url: r.url, short: r.short, changes: r.changes };
    return failed.has(version) ? { version, error: failed.get(version) } : { version };
  });
  return { repo, to: target, releases };
}
