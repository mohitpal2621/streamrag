import { query } from '../lib/api';
import { apiUrl } from '../lib/config';
import { SNAPSHOT_QUERY, type Snapshot } from '../lib/types';
import { LiveDashboard } from './live-dashboard';
import { ApiDown } from './api-down';

export const dynamic = 'force-dynamic';

// Rendered on the server with everything as it is now; the client then follows the activity stream.
export default async function LivePage() {
  const api = apiUrl();
  let snapshot: Snapshot;
  try {
    snapshot = await query<Snapshot>(api, SNAPSHOT_QUERY, {}, { cache: 'no-store', signal: AbortSignal.timeout(15_000) });
  } catch (err) {
    return <ApiDown api={api} error={err} />;
  }
  return <LiveDashboard api={api} initial={snapshot} renderedAt={Date.now()} />;
}
