import { Annotation, END, MemorySaver, START, StateGraph, type LangGraphRunnableConfig } from '@langchain/langgraph';
import { z } from 'zod';
import type { Message, ToolCall } from '../llm/openai.ts';
import { llmTracer, traced, tracedStream } from '../tracing.ts';
import { tools, type Deps } from './tools.ts';

const SYSTEM = `You help developers with their dependencies and their stack. Look things up with the tools before answering: breaking_changes for upgrades, security_advisories for known vulnerabilities, search for anything else. Once a tool has returned, answer the question from its result: say what it found, include the links it gave, and say plainly when it doesn't cover the question.`;

const State = Annotation.Root({
  messages: Annotation<Message[]>({ reducer: (all, added) => all.concat(added), default: () => [] }),
});
type AgentState = typeof State.State;

export type AgentEvent = { type: 'tool'; name: string; arguments: string } | { type: 'token'; text: string };

// A tool-calling loop as a LangGraph graph: the model either calls tools, whose results go back to it,
// or answers. Two nodes and one conditional edge. After maxRounds of tool calls for one question the
// tools are taken away, so a small model that keeps calling them still has to answer. The checkpointer keeps each
// thread's messages, so a follow-up question in the same thread sees the earlier ones.
export function createAgent(deps: Deps, { maxRounds = 3 } = {}) {
  const defs = tools(deps);
  const schemas = Object.entries(defs).map(([name, t]) => ({
    type: 'function',
    function: { name, description: t.description, parameters: z.toJSONSchema(z.object(t.input)) },
  }));

  const agent = async (state: AgentState, config: LangGraphRunnableConfig) => {
    const question = state.messages.findLastIndex((m) => m.role === 'user');
    const rounds = state.messages.slice(question + 1).filter((m) => m.role === 'assistant' && m.tool_calls?.length).length;
    let text = '';
    let calls: ToolCall[] = [];
    const offer = rounds < maxRounds ? { tools: schemas } : {};
    for await (const chunk of deps.gateway.stream([{ role: 'system', content: SYSTEM }, ...state.messages], offer)) {
      if (chunk.calls) calls = chunk.calls;
      text += chunk.text;
      if (chunk.text) config.writer?.({ type: 'token', text: chunk.text } satisfies AgentEvent);
    }
    return { messages: [{ role: 'assistant', content: text, ...(calls.length ? { tool_calls: calls } : {}) } satisfies Message] };
  };

  // Runs every call from the last reply. A bad call becomes an error message the model can read and
  // correct, instead of ending the run.
  const runTools = async (state: AgentState, config: LangGraphRunnableConfig) => {
    const last = state.messages.at(-1);
    const calls = last?.role === 'assistant' ? (last.tool_calls ?? []) : [];
    const results = await Promise.all(
      calls.map(async (call): Promise<Message> => {
        config.writer?.({ type: 'tool', name: call.function.name, arguments: call.function.arguments } satisfies AgentEvent);
        const attributes = { 'gen_ai.operation.name': 'execute_tool', 'gen_ai.tool.name': call.function.name, 'gen_ai.tool.call.arguments': call.function.arguments };
        const content = await traced(llmTracer, `tool ${call.function.name}`, attributes, async (span) => {
          let result: string;
          try {
            const def = defs[call.function.name as keyof typeof defs];
            if (!def) throw new Error(`there is no tool called ${call.function.name}`);
            const args = z.object(def.input).parse(JSON.parse(call.function.arguments || '{}'));
            result = await (def.run as (a: unknown) => Promise<string>)(args);
          } catch (err) {
            result = `error: ${err instanceof z.ZodError ? z.prettifyError(err) : err instanceof Error ? err.message : err}`;
          }
          span.setAttribute('gen_ai.tool.call.result', result);
          return result;
        });
        return { role: 'tool', tool_call_id: call.id, content };
      }),
    );
    return { messages: results };
  };

  return new StateGraph(State)
    .addNode('agent', agent)
    .addNode('tools', runTools)
    .addEdge(START, 'agent')
    .addConditionalEdges('agent', (s) => {
      const last = s.messages.at(-1);
      return last?.role === 'assistant' && last.tool_calls?.length ? 'tools' : END;
    })
    .addEdge('tools', 'agent')
    .compile({ checkpointer: new MemorySaver() });
}

export type Agent = ReturnType<typeof createAgent>;

// Asks the agent one question in a thread and yields what happens: each tool call, then the answer's
// tokens as they are written.
export function askAgent(agent: Agent, question: string, thread: string = crypto.randomUUID()): AsyncGenerator<AgentEvent> {
  return tracedStream(llmTracer, 'agent', { 'input.value': question, 'session.id': thread }, async function* (span) {
    const stream = await agent.stream(
      { messages: [{ role: 'user', content: question }] },
      { configurable: { thread_id: thread }, streamMode: 'custom', recursionLimit: 12 },
    );
    let answer = '';
    for await (const event of stream as AsyncIterable<AgentEvent>) {
      if (event.type === 'token') answer += event.text;
      yield event;
    }
    span.setAttribute('output.value', answer);
  });
}
