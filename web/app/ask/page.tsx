import type { Metadata } from 'next';
import { query } from '../../lib/api';
import { apiUrl } from '../../lib/config';
import { FEEDS_QUERY, type Feed } from '../../lib/types';
import { AskPanel } from './ask-panel';

export const dynamic = 'force-dynamic';
export const metadata: Metadata = { title: 'Ask | streamrag' };

// The feeds only fill the side column; the page still works without them if the API is slow or down.
export default async function AskPage() {
  const api = apiUrl();
  const feeds = await query<{ feeds: Feed[] }>(api, `{ ${FEEDS_QUERY} }`, {}, { cache: 'no-store', signal: AbortSignal.timeout(5_000) }).then(
    (d) => d.feeds,
    () => null,
  );
  return <AskPanel api={api} feeds={feeds} />;
}
