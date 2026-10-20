import { fork, type ChildProcess } from 'node:child_process';

export type Embed = (texts: string[], kind: 'query' | 'passage') => Promise<number[][]>;
export type Rerank = (query: string, texts: string[]) => Promise<number[]>;

// Model inference blocks the thread it runs on: a rerank of 30 chunks held the event loop for over a
// second on 2 cores, which stalled every other request in the API and would starve Kafka's
// heartbeats in the pipeline. So each model runs in a child process of its own (models-worker.ts),
// and a query's embedding never waits behind a rerank. (Worker threads would be lighter, but
// onnxruntime-node crashes when two threads of one process use it.)
function childProcess(model: 'embed' | 'rerank') {
  let child: ChildProcess | undefined;
  let next = 0;
  const pending = new Map<number, { resolve: (value: never) => void; reject: (err: Error) => void }>();
  // An idle model mustn't keep a finished command running.
  const hold = (on: boolean) => {
    for (const handle of [child, child?.channel]) on ? handle?.ref() : handle?.unref();
  };
  const fail = (err: Error) => {
    for (const p of pending.values()) p.reject(err);
    pending.clear();
    child = undefined;
  };
  return (...args: unknown[]) => {
    if (!child) {
      // Without the parent's flags: the model process needs no telemetry preload of its own.
      child = fork(new URL('./models-worker.ts', import.meta.url), [model], { execArgv: [], serialization: 'advanced' });
      child.on('message', ({ id, result, error }: { id: number; result: never; error?: string }) => {
        const p = pending.get(id);
        pending.delete(id);
        if (error) p?.reject(new Error(error));
        else p?.resolve(result);
        if (!pending.size) hold(false);
      });
      child.on('error', fail);
      child.on('exit', (code, signal) => fail(new Error(`the ${model} model's process exited (${signal ?? code})`)));
    }
    hold(true);
    const id = next++;
    child.send({ id, args });
    return new Promise<never>((resolve, reject) => pending.set(id, { resolve, reject }));
  };
}

export const embed: Embed = childProcess('embed');
export const rerank: Rerank = childProcess('rerank');
