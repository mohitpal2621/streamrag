import assert from 'node:assert/strict';
import { test } from 'node:test';
import { rankJobs } from '../src/apps/jobs/match.ts';

const job = (title: string, body: string) => ({ title, feed: 'acme', url: null, body, meta: { location: 'Remote' } });

test('ranks jobs that use more of your stack above short lists you happen to cover', () => {
  const { skills, jobs, gaps } = rankJobs(
    'Node.js, TypeScript, PostgreSQL, Redis, AWS',
    [
      job('Data Scientist', 'PyTorch and Python'),
      job('Sales Engineer', 'AWS and Redis'),
      job('Backend Engineer', 'Node.js, TypeScript, PostgreSQL, Redis, Kafka, Go, Python, Kubernetes'),
    ],
    10,
  );
  assert.deepEqual(skills, ['TypeScript', 'Node.js', 'PostgreSQL', 'Redis', 'AWS']);
  assert.deepEqual(jobs.map((j) => j.title), ['Backend Engineer', 'Sales Engineer']);
  assert.deepEqual(jobs[0].missing, ['Python', 'Go', 'Kafka', 'Kubernetes']);
  assert.deepEqual(gaps, [['Python', 1], ['Go', 1], ['Kafka', 1], ['Kubernetes', 1]]);
});

test('rejects a resume with no known skills', () => {
  assert.throws(() => rankJobs('I enjoy painting', [], 10), /no known skills/);
});
