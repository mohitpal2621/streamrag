import type { Connector } from '../core/ingest.ts';
import * as hackernews from './hackernews.ts';
import * as osv from './osv.ts';
import * as rss from './rss.ts';

export const connectors: Record<string, Connector> = { rss, hackernews, osv };
