// npm run mcp: an MCP server over stdio, for coding assistants. stdout carries the protocol, so
// nothing else may print there.
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { createMcp } from './api/mcp.ts';
import { embed, rerank } from './core/models.ts';
import { connect } from './core/store.ts';
import { loggedGateway } from './llm/calls.ts';

const db = connect();
await createMcp({ db, embed, rerank, gateway: loggedGateway(db).gateway }).connect(new StdioServerTransport());
