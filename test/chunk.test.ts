import assert from 'node:assert/strict';
import { test } from 'node:test';
import { chunk } from '../src/core/chunk.ts';

const words = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => `w${from + i}`).join(' ');

test('a short document is one chunk that starts with its title', () => {
  assert.deepEqual(chunk('Title', 'just a few words'), ['Title\njust a few words']);
});

test('a document with no body still gets a chunk, so its title is searchable', () => {
  assert.deepEqual(chunk('Node.js 24.22.0 (LTS)', ''), ['Node.js 24.22.0 (LTS)']);
});

test('long documents split into overlapping windows', () => {
  const chunks = chunk('T', words(0, 450), 200, 40);
  assert.deepEqual(chunks, [`T\n${words(0, 200)}`, `T\n${words(160, 360)}`, `T\n${words(320, 450)}`]);
});

test('a document that ends exactly on a window boundary adds no empty chunk', () => {
  assert.equal(chunk('T', words(0, 200), 200, 40).length, 1);
});
