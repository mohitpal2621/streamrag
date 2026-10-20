// Runs one of the two local models in a process of its own, for models.ts.
import { AutoModelForSequenceClassification, AutoTokenizer, pipeline } from '@huggingface/transformers';

// Small free models that run on the CPU. They download once from Hugging Face (about 56 MB)
// into node_modules, then work offline.
const EMBED_MODEL = 'Xenova/bge-small-en-v1.5'; // 384 dimensions, matches vector(384) in the schema
const RERANK_MODEL = 'Xenova/ms-marco-MiniLM-L-6-v2';

// One text per model call. A batch is padded to its longest text, and on the CPU that cost far more
// than batching saved: 64 chunks took 8.6 s to embed in batches of 16 and 2.2 s one at a time, and
// reranking 30 chunks took 3.1 s and 2 GB as one batch, 0.6 s and 220 MB one at a time.

let extractor: ReturnType<typeof loadExtractor> | undefined;
const loadExtractor = () => pipeline('feature-extraction', EMBED_MODEL, { dtype: 'q8' });

async function embed(texts: string[], kind: 'query' | 'passage') {
  extractor ??= loadExtractor();
  const model = await extractor;
  // bge expects this instruction on the query only, never on the passages being searched.
  const input = kind === 'query' ? texts.map((t) => `Represent this sentence for searching relevant passages: ${t}`) : texts;
  const vectors: number[][] = [];
  for (const text of input) vectors.push(...(await model(text, { pooling: 'cls', normalize: true })).tolist());
  return vectors;
}

let reranker: ReturnType<typeof loadReranker> | undefined;
const loadReranker = () =>
  Promise.all([AutoTokenizer.from_pretrained(RERANK_MODEL), AutoModelForSequenceClassification.from_pretrained(RERANK_MODEL, { dtype: 'q8' })]);

// A cross-encoder reads the query and the passage together, which is slower than comparing
// embeddings but much better at judging relevance, so it only sees the top few candidates.
async function rerank(query: string, texts: string[]) {
  reranker ??= loadReranker();
  const [tokenizer, model] = await reranker;
  const scores: number[] = [];
  for (const text of texts) {
    const { logits } = await model(tokenizer(query, { text_pair: text, truncation: true }));
    scores.push(logits.tolist()[0][0]);
  }
  return scores;
}

const run = process.argv[2] === 'embed' ? embed : rerank;
process.on('message', async ({ id, args }: { id: number; args: [never, never] }) => {
  try {
    process.send!({ id, result: await run(...args) });
  } catch (err) {
    process.send!({ id, error: err instanceof Error ? err.message : String(err) });
  }
});
process.on('disconnect', () => process.exit()); // the parent is gone
