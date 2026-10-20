import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
import { clock, decode, paragraphs, transcribe } from '../src/media/transcribe.ts';

// Three seconds of a 440 Hz tone, as a stereo 44.1 kHz WAV that FFmpeg has to resample and downmix.
const dir = mkdtempSync(join(tmpdir(), 'streamrag-media-'));
const wav = join(dir, 'tone.wav');
execFileSync('ffmpeg', ['-nostdin', '-loglevel', 'error', '-f', 'lavfi', '-i', 'sine=frequency=440:duration=3', '-ac', '2', '-ar', '44100', wav]);
after(() => rmSync(dir, { recursive: true, force: true }));

test('ffmpeg decodes any input to 16 kHz mono samples, in blocks', async () => {
  const blocks: number[] = [];
  for await (const b of decode(wav, { blockSeconds: 1 })) blocks.push(b.length);
  assert.deepEqual(blocks, [16_000, 16_000, 16_000]);
  const short: number[] = [];
  for await (const b of decode(wav, { maxSeconds: 1.5 })) short.push(b.length);
  assert.deepEqual(short, [24_000]); // -t stopped reading after a second and a half
});

test('transcript times count from the start of the episode, not the block', async () => {
  const blockStarts: number[] = [];
  const { segments, seconds } = await transcribe(wav, {
    blockSeconds: 1,
    recognize: async (samples) => [{ start: 0.25, text: `${samples.length} samples` }],
    onProgress: (s) => blockStarts.push(s),
  });
  assert.deepEqual(segments.map((s) => s.start), [0.25, 1.25, 2.25]);
  assert.deepEqual([seconds, blockStarts], [3, [1, 2, 3]]);
});

test('segments merge into timed paragraphs of about 30 seconds', () => {
  const text = paragraphs([
    { start: 0, text: 'Welcome back.' },
    { start: 12.4, text: 'Today: Kafka.' },
    { start: 31, text: 'First, partitions.' },
    { start: 3_725, text: 'Thanks for listening.' },
  ]);
  assert.equal(text, '[0:00] Welcome back. Today: Kafka.\n[0:31] First, partitions.\n[1:02:05] Thanks for listening.');
  assert.deepEqual([clock(59.9), clock(600), clock(3_600)], ['0:59', '10:00', '1:00:00']);
});

test('a missing ffmpeg says how to install it', async () => {
  const saved = process.env.FFMPEG_PATH;
  process.env.FFMPEG_PATH = join(dir, 'no-such-ffmpeg');
  try {
    await assert.rejects(transcribe(wav, { recognize: async () => [] }), /ffmpeg is not installed \(winget install Gyan\.FFmpeg/);
  } finally {
    if (saved === undefined) delete process.env.FFMPEG_PATH;
    else process.env.FFMPEG_PATH = saved;
  }
});

test('a file ffmpeg cannot read fails with its reason', async () => {
  await assert.rejects(transcribe(join(dir, 'missing.mp3'), { recognize: async () => [] }), /ffmpeg exited with \d+: .*No such file/);
});
