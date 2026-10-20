import type pg from 'pg';
import { z } from 'zod';
import { advisories } from '../connectors/osv.ts';
import type { Embed, Rerank } from '../core/models.ts';
import { search } from '../core/search.ts';
import type { Gateway } from '../llm/gateway.ts';
import { breakingChanges } from '../upgrade.ts';

export type Deps = { db: pg.Pool; embed: Embed; rerank: Rerank; gateway: Gateway };

type Tool<S extends z.ZodRawShape> = { description: string; input: S; run: (args: z.infer<z.ZodObject<S>>) => Promise<string> };
const tool = <S extends z.ZodRawShape>(t: Tool<S>) => t;

// What a coding assistant asks about a project's dependencies and its stack, as tools that the MCP
// server and the LangGraph agent share. Each returns plain text a model can quote, links included.
export function tools(d: Deps) {
  return {
    search: tool({
      description:
        'Search what streamrag has synced: engineering blogs, release notes, Hacker News, security advisories and podcast transcripts. Returns the best matches with links.',
      input: {
        query: z.string().describe('what to look for, in plain words'),
        // Small models often send numbers as strings, so this one is coerced.
        limit: z.coerce.number().int().min(1).max(10).optional().describe('how many results, 5 by default'),
      },
      run: async ({ query, limit }) => {
        const hits = await search(d.db, query, { mode: 'rerank', limit: limit ?? 5, embed: d.embed, rerank: d.rerank });
        if (!hits.length) return 'nothing found';
        return hits.map((h, i) => `[${i + 1}] ${h.title}\n${h.url ?? ''}\n${h.text.slice(h.text.indexOf('\n') + 1, 700)}`).join('\n\n');
      },
    }),
    breaking_changes: tool({
      description:
        'What breaks when an npm package is upgraded: the breaking changes in the release notes of every major and minor release between two versions.',
      input: {
        package: z.string().describe('npm package name, like next or @nestjs/core'),
        from: z.string().describe('the version in use, like 4.21.0'),
        to: z.string().optional().describe('the version to upgrade to, or latest; the latest if left out'),
      },
      run: async ({ package: name, from, to }) => {
        const r = await breakingChanges(d.db, d.gateway, name, from, to);
        if (!r.releases.length) return `${name} has no major or minor release between ${from} and ${r.to}; patch releases are not checked.`;
        const lines = r.releases.map((rel) => {
          if (!rel.changes) return `${rel.version}: ${rel.error ? `could not fetch the notes (${rel.error})` : 'no release notes on GitHub'}`;
          if (!rel.changes.length) return `${rel.version} (${rel.url}): nothing breaking listed${rel.short ? ', but the notes are short and may only link to a changelog' : ''}`;
          return `${rel.version} (${rel.url}):\n${rel.changes.map((c) => `- ${c.change}${c.migration ? ` -> ${c.migration}` : ''}`).join('\n')}`;
        });
        // A one-line verdict first, which a small model can repeat without misreading the details.
        const total = r.releases.reduce((n, rel) => n + (rel.changes?.length ?? 0), 0);
        const verdict = `Upgrading ${name} from ${from} to ${r.to}: ${total ? `${total} breaking changes` : 'no breaking changes'} listed in the notes of ${r.releases.map((rel) => rel.version).join(', ')}.`;
        return `${verdict}\n${lines.join('\n')}`;
      },
    }),
    security_advisories: tool({
      description: 'Known security advisories for an npm package from OSV, with the versions that fix them. Give a version to see only those that affect it.',
      input: {
        package: z.string().describe('npm package name'),
        version: z.string().optional().describe('the version in use; leave out to list every advisory'),
      },
      run: async ({ package: name, version }) => {
        const found = await advisories('npm', name, version);
        if (!found.length) return `no known advisories for ${name}${version ? ` ${version}` : ''}`;
        return found
          .map((a) => {
            const fixed = a.body.match(/Fixed in: (.+)/)?.[1];
            return `- ${a.title} (${a.meta.severity ?? 'severity not given'})${fixed ? `, fixed in ${fixed}` : ''}\n  ${a.url}`;
          })
          .join('\n');
      },
    }),
  };
}

export type Tools = ReturnType<typeof tools>;
