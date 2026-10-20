import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { tools, type Deps } from '../agent/tools.ts';

// The same tools the agent uses, offered to any MCP client: Claude Code, Claude Desktop, Cursor and
// the like. Their own model does the reasoning; streamrag answers what it knows about the code's
// dependencies and the engineering world around them.
export function createMcp(deps: Deps) {
  const server = new McpServer({ name: 'streamrag', version: '0.1.0' });
  for (const [name, t] of Object.entries(tools(deps))) {
    server.registerTool(name, { description: t.description, inputSchema: t.input }, async (args: unknown) => {
      try {
        return { content: [{ type: 'text' as const, text: await (t.run as (a: unknown) => Promise<string>)(args) }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `error: ${err instanceof Error ? err.message : err}` }], isError: true };
      }
    });
  }
  return server;
}
