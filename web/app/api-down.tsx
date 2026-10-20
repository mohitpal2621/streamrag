export function ApiDown({ api, error }: { api: string; error: unknown }) {
  const reason = error instanceof Error ? (error.cause instanceof Error ? error.cause.message : error.message) : String(error);
  return (
    <main>
      <div className="card notice">
        <h1>The streamrag API isn&apos;t answering</h1>
        <p>
          The dashboard reads from <code>{api}</code>, which said: <span className="reason">{reason}</span>.
        </p>
        <p>
          Start it with <code>npm run serve</code> in the streamrag folder, then reload this page. To use another address, set{' '}
          <code>API_URL</code> when starting the dashboard.
        </p>
      </div>
    </main>
  );
}
