import assert from 'node:assert/strict';
import { test } from 'node:test';
import * as ashby from '../src/connectors/ashby.ts';
import * as greenhouse from '../src/connectors/greenhouse.ts';
import { htmlToText } from '../src/connectors/html.ts';
import * as lever from '../src/connectors/lever.ts';

test('htmlToText keeps line breaks and decodes entities', () => {
  assert.equal(
    htmlToText('<p>Build&nbsp;APIs &amp; tools</p><ul><li>Node.js</li><li>Go &#8211; or Rust&#x2019;s</li></ul>'),
    'Build APIs & tools\nNode.js\nGo – or Rust’s',
  );
});

test('greenhouse unescapes its HTML before stripping it', () => {
  const docs = greenhouse.normalize({
    jobs: [{ id: 42, title: 'Backend Engineer', absolute_url: 'https://x/42', content: '&lt;p&gt;Kafka &amp;amp; Go&lt;/p&gt;', location: { name: 'Remote' } }],
  });
  assert.deepEqual(docs, [{ externalId: '42', title: 'Backend Engineer', url: 'https://x/42', body: 'Kafka & Go', meta: { location: 'Remote' } }]);
});

test('lever keeps the requirement lists in the body', () => {
  const [doc] = lever.normalize([
    { id: 'abc', text: 'SRE', descriptionPlain: 'About us', lists: [{ text: 'Requirements', content: '<li>Kubernetes</li><li>Terraform</li>' }] },
  ]);
  assert.equal(doc.body, 'About us\n\nRequirements\nKubernetes\nTerraform');
  assert.equal(doc.url, null);
  assert.deepEqual(doc.meta, { location: null });
});

test('ashby skips unlisted jobs', () => {
  const docs = ashby.normalize({ jobs: [{ id: '1', title: 'A', descriptionPlain: 'x' }, { id: '2', title: 'B', isListed: false }] });
  assert.deepEqual(docs.map((d) => d.externalId), ['1']);
});

test('connectors reject responses of the wrong shape', () => {
  const notFound = JSON.parse('{"ok":false,"error":"Document not found"}');
  assert.throws(() => greenhouse.normalize(notFound), /unexpected Greenhouse response/);
  assert.throws(() => lever.normalize(notFound), /unexpected Lever response/);
  assert.throws(() => ashby.normalize(notFound), /unexpected Ashby response/);
});
