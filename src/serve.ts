// npm run serve: the GraphQL API, with GraphiQL at http://localhost:4000/graphql.
import { createServer } from 'node:http';
import { createApi } from './api/graphql.ts';
import { startLive } from './api/live.ts';
import { embed, rerank } from './core/models.ts';
import { connect } from './core/store.ts';
import { loggedGateway } from './llm/calls.ts';

const port = Number(process.env.PORT ?? 4000);
const db = connect();
db.on('error', (err) => console.error(`database: ${err.message}`)); // an idle connection dropped; the pool opens a new one
const live = await startLive(db);
const server = createServer(createApi({ db, embed, rerank, gateway: loggedGateway(db).gateway }, live));
server.listen(port, () => console.log(`GraphQL at http://localhost:${port}/graphql`));

const stop = () => {
  server.closeAllConnections(); // the dashboard keeps its activity stream open
  server.close(async () => {
    await live.close();
    await db.end();
  });
};
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
