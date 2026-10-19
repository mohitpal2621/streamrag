import assert from 'node:assert/strict';
import { test } from 'node:test';
import { githubRepo, mergeSources } from '../src/track.ts';

test('githubRepo understands the ways npm packages name their repo', () => {
  assert.equal(githubRepo({ url: 'git+https://github.com/vercel/next.js.git' }), 'vercel/next.js');
  assert.equal(githubRepo({ url: 'git://github.com/brianc/node-postgres.git' }), 'brianc/node-postgres');
  assert.equal(githubRepo({ url: 'git+ssh://git@github.com/stevemao/left-pad.git' }), 'stevemao/left-pad');
  assert.equal(githubRepo('git@github.com:o/r.git'), 'o/r');
  assert.equal(githubRepo('https://github.com/o/r/tree/main/packages/x'), 'o/r');
  assert.equal(githubRepo('github:o/r'), 'o/r');
  assert.equal(githubRepo('o/r'), 'o/r');
  assert.equal(githubRepo({ url: 'https://gitlab.com/o/r' }), null);
  assert.equal(githubRepo(undefined), null);
});

test('mergeSources adds only feeds it does not already have', () => {
  const current = [{ connector: 'osv', feed: 'npm/next' }];
  const incoming = [
    { connector: 'osv', feed: 'npm/next' },
    { connector: 'rss', feed: 'https://github.com/nestjs/nest/releases.atom' },
    { connector: 'rss', feed: 'https://github.com/nestjs/nest/releases.atom' },
  ];
  assert.deepEqual(mergeSources(current, incoming), [{ connector: 'rss', feed: 'https://github.com/nestjs/nest/releases.atom' }]);
});
