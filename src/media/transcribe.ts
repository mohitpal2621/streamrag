import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { pipeline, type AutomaticSpeechRecognitionPipeline } from '@huggingface/transformers';
import type pg from 'pg';
import { syncFeed } from '../core/store.ts';

const RATE = 16_000; // Whisper listens at 16 kHz, mono
export const WHISPER_MODEL = process.env.WHISPER_MODEL ?? 'Xenova/whisper-base.en';

export type Segment = { start: number; text: string };
// Turns samples into timed text, with start times relative to the samples given.
export type Recognize = (samples: Float32Array) => Promise<Segment[]>;

// The library's own return type for this task is too large for TypeScript to compute, so the call
// goes through a plain signature.
const load = pipeline as (task: string, model: string, options: object) => Promise<unknown>;
let asr: Promise<AutomaticSpeechRecognitionPipeline> | undefined;
export const whisper: Recognize = async (samples) => {
  asr ??= load('automatic-speech-recognition', WHISPER_MODEL, { dtype: 'q8' }) as Promise<AutomaticSpeechRecognitionPipeline>;
  const out = await (await asr)(samples, { chunk_length_s: 30, stride_length_s: 5, return_timestamps: true });
  const chunks = (Array.isArray(out) ? out[0] : out).chunks ?? [];
  return chunks.map((c) => ({ start: c.timestamp[0], text: c.text.trim() })).filter((s) => s.text);
};

const toSamples = (bytes: Buffer) => {
  const samples = new Float32Array(bytes.length / 4);
  new Uint8Array(samples.buffer).set(bytes); // copied, since a Float32Array needs 4-byte alignment
  return samples;
};

// FFmpeg reads the episode straight from its URL, whatever the container or codec, and writes 32-bit
// float samples at 16 kHz mono to stdout, which is what Whisper takes. They come out in blocks of a
// few minutes, so a three-hour episode never sits in memory whole; -t stops the download early.
export async function* decode(url: string, { maxSeconds, blockSeconds = 300 }: { maxSeconds?: number; blockSeconds?: number } = {}) {
  const args = ['-nostdin', '-loglevel', 'error', ...(maxSeconds ? ['-t', String(maxSeconds)] : []), '-i', url];
  args.push('-vn', '-ac', '1', '-ar', String(RATE), '-f', 'f32le', 'pipe:1');
  const ff = spawn(process.env.FFMPEG_PATH ?? 'ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
  try {
    await once(ff, 'spawn');
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
    throw new Error('ffmpeg is not installed (winget install Gyan.FFmpeg, brew install ffmpeg or apt install ffmpeg), or set FFMPEG_PATH');
  }
  let stderr = '';
  ff.stderr.on('data', (c) => (stderr += c));
  const exited = once(ff, 'close') as Promise<[number | null]>;
  const blockBytes = blockSeconds * RATE * 4;
  try {
    let parts: Buffer[] = [];
    let size = 0;
    for await (const chunk of ff.stdout as AsyncIterable<Buffer>) {
      parts.push(chunk);
      size += chunk.length;
      if (size < blockBytes) continue;
      const all = Buffer.concat(parts, size);
      yield toSamples(all.subarray(0, blockBytes));
      parts = [all.subarray(blockBytes)];
      size = parts[0].length;
    }
    const [code] = await exited;
    if (code !== 0) throw new Error(`ffmpeg exited with ${code}: ${stderr.trim().slice(0, 300)}`);
    if (size >= 4) yield toSamples(Buffer.concat(parts, size).subarray(0, size - (size % 4)));
  } finally {
    if (ff.exitCode === null) ff.kill();
  }
}

export async function transcribe(url: string, opts: { maxSeconds?: number; blockSeconds?: number; recognize?: Recognize; onProgress?: (seconds: number) => void } = {}) {
  const recognize = opts.recognize ?? whisper;
  const segments: Segment[] = [];
  let seconds = 0;
  for await (const block of decode(url, opts)) {
    for (const s of await recognize(block)) segments.push({ start: seconds + s.start, text: s.text });
    seconds += block.length / RATE;
    opts.onProgress?.(seconds);
  }
  return { segments, seconds };
}

export const clock = (s: number) => {
  const [h, m, sec] = [Math.floor(s / 3600), Math.floor((s % 3600) / 60), Math.floor(s % 60)];
  return h ? `${h}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}` : `${m}:${String(sec).padStart(2, '0')}`;
};

// Whisper's segments are a few seconds each. They are merged into paragraphs of about 30 seconds,
// each opening with its start time, so any chunk of a transcript says roughly where it is.
export function paragraphs(segments: Segment[], seconds = 30) {
  const out: string[] = [];
  let start = 0;
  for (const s of segments) {
    if (!out.length || s.start - start >= seconds) {
      out.push(`[${clock(s.start)}] ${s.text}`);
      start = s.start;
    } else {
      out[out.length - 1] += ` ${s.text}`;
    }
  }
  return out.join('\n');
}

type Episode = { id: string; feed: string; external_id: string; title: string; audio: string; published: string | null };

// Podcast episodes (RSS items with an audio enclosure) that have no transcript yet, newest first.
// `ids` narrows it to particular documents.
export async function untranscribed(db: pg.Pool, limit: number, ids?: string[]) {
  const { rows } = await db.query<Episode>(
    `select d.id, d.feed, d.external_id, d.title, d.meta->>'audio' as audio, d.meta->>'published' as published
     from documents d
     where d.meta ? 'audio' and d.closed_at is null and ($1::bigint[] is null or d.id = any($1))
       and not exists (select 1 from documents t where t.source = 'transcript' and t.feed = d.feed and t.external_id = d.external_id)`,
    [ids ?? null],
  );
  // Feeds date items in RFC 822 or ISO 8601, which Date.parse reads and Postgres doesn't always.
  return rows.sort((a, b) => (Date.parse(b.published ?? '') || 0) - (Date.parse(a.published ?? '') || 0)).slice(0, limit);
}

// A transcript is a document of its own, keyed like its episode, so the embedder and search take it
// as it is. Its URL is the audio file, so a link can jump to a moment with #t=seconds.
export async function transcribeEpisode(db: pg.Pool, episode: Episode, opts: Parameters<typeof transcribe>[1] = {}) {
  const { segments, seconds } = await transcribe(episode.audio, opts);
  const body = paragraphs(segments);
  await syncFeed(db, 'transcript', episode.feed, [
    { externalId: episode.external_id, title: `${episode.title} (transcript)`, url: episode.audio, body, meta: { episode: episode.id, seconds: Math.round(seconds), model: WHISPER_MODEL } },
  ], false);
  return { seconds, words: body.split(/\s+/).length };
}
