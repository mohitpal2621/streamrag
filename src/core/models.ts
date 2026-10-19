import { AutoModelForSequenceClassification, AutoTokenizer, pipeline } from '@huggingface/transformers';

export type Embed = (texts: string[], kind: 'query' | 'passage') => Promise<number[][]>;
export type Rerank = (query: string, texts: string[]) => Promise<number[]>;

// Small free models that run on the CPU. They download once from Hugging Face (about 56 MB)
// into node_modules, then work offline.
const EMBED_MODEL = 'Xenova/bge-small-en-v1.5'; // 384 dimensions, matches vector(384) in the schema
const RERANK_MODEL = 'Xenova/ms-marco-MiniLM-L-6-v2';

// Texts per model call. Memory grows with batch size times sequence length squared, and one call
// over a few hundred long chunks needed 6 GB before this cap.
const BATCH = 16;

let extractor: ReturnType<typeof loadExtractor> | undefined;
const loadExtractor = () => pipeline('feature-extraction', EMBED_MODEL, { dtype: 'q8' });

export const embed: Embed = async (texts, kind) => {
  extractor ??= loadExtractor();
  const model = await extractor;
  // bge expects this instruction on the query only, never on the passages being searched.
  const input = kind === 'query' ? texts.map((t) => `Represent this sentence for searching relevant passages: ${t}`) : texts;
  const vectors: number[][] = [];
  for (let i = 0; i < input.length; i += BATCH) {
    const output = await model(input.slice(i, i + BATCH), { pooling: 'cls', normalize: true });
    vectors.push(...output.tolist());
  }
  return vectors;
};

let reranker: ReturnType<typeof loadReranker> | undefined;
const loadReranker = () =>
  Promise.all([AutoTokenizer.from_pretrained(RERANK_MODEL), AutoModelForSequenceClassification.from_pretrained(RERANK_MODEL, { dtype: 'q8' })]);

// A cross-encoder reads the query and the passage together, which is slower than comparing
// embeddings but much better at judging relevance, so it only sees the top few candidates.
export const rerank: Rerank = async (query, texts) => {
  reranker ??= loadReranker();
  const [tokenizer, model] = await reranker;
  const inputs = tokenizer(texts.map(() => query), { text_pair: texts, padding: true, truncation: true });
  const { logits } = await model(inputs);
  return logits.tolist().map((row: number[]) => row[0]);
};
