import assert from 'node:assert/strict';
import { test } from 'node:test';
import { extractSkills } from '../src/apps/jobs/skills.ts';

test('keeps look-alike names apart', () => {
  assert.deepEqual(extractSkills('We use JavaScript'), ['JavaScript']);
  assert.deepEqual(extractSkills('Java and JavaScript'), ['JavaScript', 'Java']);
  assert.deepEqual(extractSkills('React Native apps'), ['React Native']);
  assert.deepEqual(extractSkills('React and React Native'), ['React', 'React Native']);
});

test('counts everyday words only when they mean the tech', () => {
  assert.deepEqual(extractSkills('Own our go-to-market tooling. Go live fast.'), []);
  assert.deepEqual(extractSkills('Services written in Go, Python and Rust'), ['Python', 'Go', 'Rust']);
  assert.deepEqual(extractSkills('the rest of the team'), []);
  assert.deepEqual(extractSkills('Design RESTful APIs'), ['REST']);
});

test('handles symbols in names', () => {
  assert.deepEqual(extractSkills('C++, C# and .NET; Node.js'), ['C++', 'C#', '.NET', 'Node.js']);
  assert.deepEqual(extractSkills('visit example.net'), []);
});
