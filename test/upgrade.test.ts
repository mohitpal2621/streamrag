import assert from 'node:assert/strict';
import { test } from 'node:test';
import { extractBreaking, markedBreaking, pieces } from '../src/core/extract.ts';
import type { ChatOptions, Message } from '../src/llm/openai.ts';
import { parseVersion, releasesToCheck } from '../src/upgrade.ts';

test('versions parse with or without a v and missing parts, prereleases do not', () => {
  assert.deepEqual([parseVersion('4'), parseVersion('v4.2.1'), parseVersion('4.2'), parseVersion('5.0.0-beta.1')], [[4, 0, 0], [4, 2, 1], [4, 2, 0], null]);
});

test('an upgrade reads the stable majors and minors it crosses, in order', () => {
  const versions = ['15.1.0', '14.1.0', '14.2.0', '14.2.5', '15.0.0-canary.3', '15.0.0', '15.0.1', '15.2.0'];
  assert.deepEqual(releasesToCheck(versions, '14.2.0', '15.1.3'), ['15.0.0', '15.1.0']);
  assert.deepEqual(releasesToCheck(versions, '14', '15'), ['14.1.0', '14.2.0', '15.0.0']);
  assert.deepEqual(releasesToCheck(versions, '15.2.0', '15.2.0'), []);
  assert.throws(() => releasesToCheck(versions, 'latest', '15'), /not "latest"/);
});

test('long notes are split between lines, without link targets', () => {
  const line = (n: number) => `* [\`abc${n}\`](https://github.com/o/r/commit/abc${n}) feat!: change number ${n}`;
  const notes = Array.from({ length: 40 }, (_, i) => line(i)).join('\n');
  const parts = pieces(notes, 300);
  assert.ok(parts.length > 1 && parts.every((p) => p.length <= 300));
  assert.ok(!parts.join('').includes('https://'));
  assert.equal(parts.join('\n').split('\n').length, 40); // every line kept, none split
  assert.deepEqual(pieces('   \n  '), []);
});

// Replies from a script, one reply per call, and records what it was asked.
function scripted(replies: string[]) {
  const g = {
    asked: [] as { messages: Message[]; options?: ChatOptions }[],
    async *stream(messages: Message[], options?: ChatOptions) {
      g.asked.push({ messages, options });
      yield { provider: 'fake', text: replies.shift() ?? '' };
    },
  };
  return g;
}

test('extraction asks for JSON, merges the pieces and drops repeats', async () => {
  const g = scripted([
    '{"breaking": [{"change": "Drops Node 16", "migration": "Use Node 18"}, {"change": "  "}]}',
    '```json\n{"breaking": [{"change": "drops node 16", "migration": ""}, {"change": "Removes app.del()"}]}\n```',
  ]);
  const found = await extractBreaking(g, 'v2.0.0', `${'a'.repeat(3_000)}\n${'b'.repeat(3_000)}`);
  assert.deepEqual(found, [
    { change: 'Drops Node 16', migration: 'Use Node 18' },
    { change: 'Removes app.del()', migration: '' },
  ]);
  assert.equal(g.asked.length, 2);
  assert.equal((g.asked[0].options?.response_format as { type: string }).type, 'json_schema');
  assert.equal(g.asked[0].options?.temperature, 0);
});

test('a reply that is not the JSON asked for gets one retry', async () => {
  const g = scripted(['Sure! Here are the breaking changes:', '{"breaking": []}']);
  assert.deepEqual(await extractBreaking(g, 'v1.1.0', 'Fixes a typo.'), []);
  assert.equal(g.asked.length, 2);
  await assert.rejects(extractBreaking(scripted(['nope', '{"changes": []}']), 'v1.2.0', 'notes'), /did not reply with the JSON asked for/);
});

test('conventional-commit breaking markers are read without the model', async () => {
  const notes = [
    '* [`53f0f47`](https://github.com/o/r/commit/53f0f47) feat!: Remove CodePath#currentSegments (#17756) (Milos Djermanovic)',
    '* [`19f9a89`](https://github.com/o/r/commit/19f9a89) chore: Update dependencies (#18275) (Nicholas C. Zakas)',
    '- fix(cli)!: exit with 2 on a config error',
    'BREAKING CHANGE: the cache file moved to .cache/',
    'feat: add a --json flag',
  ].join('\n');
  const { marked, rest } = markedBreaking(notes);
  assert.deepEqual(marked.map((b) => b.change), ['Remove CodePath#currentSegments', 'exit with 2 on a config error', 'the cache file moved to .cache/']);
  assert.equal(rest, '* `19f9a89` chore: Update dependencies (#18275) (Nicholas C. Zakas)\nfeat: add a --json flag');

  const g = scripted(['{"breaking": [{"change": "Drops the --legacy flag", "migration": ""}]}']);
  const found = await extractBreaking(g, 'v9.0.0', notes);
  assert.equal(found.length, 4);
  assert.ok(!g.asked[0].messages[1].content.includes('feat!:')); // the model only saw the unmarked lines
});
