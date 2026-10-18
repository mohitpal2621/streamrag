import type { Connector } from '../core/ingest.ts';
import type { Doc } from '../core/store.ts';
import * as ashby from './ashby.ts';
import * as greenhouse from './greenhouse.ts';
import * as lever from './lever.ts';

// `any`: the JSON comes straight off the network, and each normalize checks its shape.
const jsonConnector =
  (c: { url: (feed: string) => string; normalize: (data: any) => Doc[] }): Connector =>
  async (feed) => {
    const res = await fetch(c.url(feed), {
      headers: { 'user-agent': 'streamrag (+https://github.com/mohitpal2621/streamrag)' },
      signal: AbortSignal.timeout(30_000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return c.normalize(await res.json());
  };

export const connectors: Record<string, Connector> = {
  greenhouse: jsonConnector(greenhouse),
  lever: jsonConnector(lever),
  ashby: jsonConnector(ashby),
};
