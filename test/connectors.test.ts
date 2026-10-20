import assert from 'node:assert/strict';
import { test } from 'node:test';
import * as hackernews from '../src/connectors/hackernews.ts';
import { htmlToText } from '../src/connectors/html.ts';
import * as osv from '../src/connectors/osv.ts';
import * as rss from '../src/connectors/rss.ts';

test('htmlToText keeps line breaks and decodes entities', () => {
  assert.equal(
    htmlToText('<p>Build&nbsp;APIs &amp; tools</p><ul><li>Node.js</li><li>Go &#8211; or Rust&#x2019;s</li></ul>'),
    'Build APIs & tools\nNode.js\nGo – or Rust’s',
  );
});

test('rss reads RSS 2.0 items, preferring the full content', () => {
  const docs = rss.normalize(`<?xml version="1.0"?>
    <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><title>Eng</title>
      <item>
        <title>Scaling Postgres</title><link>https://blog.example/pg</link>
        <guid isPermaLink="false">post-1</guid><pubDate>Mon, 19 Oct 2026 10:00:00 GMT</pubDate>
        <description>&lt;p&gt;Short&lt;/p&gt;</description>
        <content:encoded><![CDATA[<p>We moved to <b>pgvector</b> &amp; partitioning.</p>]]></content:encoded>
      </item>
      <item><title>2024</title><link>https://blog.example/2024</link></item>
    </channel></rss>`);
  assert.deepEqual(docs, [
    { externalId: 'post-1', title: 'Scaling Postgres', url: 'https://blog.example/pg', body: 'We moved to pgvector & partitioning.', meta: { published: 'Mon, 19 Oct 2026 10:00:00 GMT' } },
    { externalId: 'https://blog.example/2024', title: '2024', url: 'https://blog.example/2024', body: '', meta: { published: null } },
  ]);
});

test('rss marks podcast episodes by their audio enclosure and length', () => {
  const docs = rss.normalize(`<?xml version="1.0"?>
    <rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"><channel><title>Dev Show</title>
      <item><title>Ep 1</title><guid>ep-1</guid><itunes:duration>47:05</itunes:duration>
        <enclosure url="https://cdn.example/ep1.mp3" length="1" type="audio/mpeg"/></item>
      <item><title>Ep 2</title><guid>ep-2</guid><itunes:duration>3725</itunes:duration>
        <enclosure url="https://cdn.example/ep2.m4a?x=1" length="1"/></item>
      <item><title>Slides</title><guid>slides</guid><enclosure url="https://cdn.example/deck.pdf" type="application/pdf"/></item>
    </channel></rss>`);
  assert.deepEqual(
    docs.map((d) => d.meta),
    [
      { published: null, audio: 'https://cdn.example/ep1.mp3', duration: 2_825 },
      { published: null, audio: 'https://cdn.example/ep2.m4a?x=1', duration: 3_725 },
      { published: null },
    ],
  );
});

test('rss reads Atom entries like GitHub release feeds', () => {
  const docs = rss.normalize(`<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"><title>Releases</title>
      <entry>
        <id>tag:github.com,2008:Repository/1/v16.4.2</id><updated>2026-10-18T12:00:00Z</updated>
        <link rel="alternate" type="text/html" href="https://github.com/o/r/releases/tag/v16.4.2"/>
        <title>v16.4.2</title>
        <content type="html">&lt;h2&gt;Fixes&lt;/h2&gt;&lt;ul&gt;&lt;li&gt;Fix cache &amp;amp; headers&lt;/li&gt;&lt;/ul&gt;</content>
      </entry>
    </feed>`);
  assert.deepEqual(docs, [
    {
      externalId: 'tag:github.com,2008:Repository/1/v16.4.2',
      title: 'v16.4.2',
      url: 'https://github.com/o/r/releases/tag/v16.4.2',
      body: 'Fixes\nFix cache & headers',
      meta: { published: '2026-10-18T12:00:00Z' },
    },
  ]);
});

test('rss reads inline XHTML content and keeps brackets in text titles', () => {
  const [first, second] = rss.normalize(`<feed xmlns="http://www.w3.org/2005/Atom">
    <entry>
      <id>https://vercel.example/changelog/1</id>
      <title><![CDATA[Logs now show sessions & caching]]></title>
      <link href="https://vercel.example/changelog/1"/>
      <content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p>Open a <a href="/s">session</a> &amp; see it.</p><p>Shows &lt;code&gt; as text.</p></div></content>
    </entry>
    <entry>
      <id>https://vercel.example/blog/2</id>
      <title>&lt;script type="text/llms.txt"&gt;</title>
    </entry>
  </feed>`);
  assert.equal(first.title, 'Logs now show sessions & caching');
  assert.equal(first.url, 'https://vercel.example/changelog/1');
  assert.equal(first.body, 'Open a session & see it.\nShows <code> as text.');
  assert.equal(second.title, '<script type="text/llms.txt">');
});

test('rss accepts an empty feed and rejects a page that is not a feed', () => {
  assert.deepEqual(rss.normalize('<rss><channel><title>quiet</title></channel></rss>'), []);
  assert.throws(() => rss.normalize('<html><body>hi</body></html>'), /not an RSS or Atom feed/);
});

test('hackernews turns stories and comments into documents', () => {
  const docs = hackernews.normalize({
    hits: [
      { objectID: '1', title: 'Show HN: streamrag', url: 'https://github.com/x', story_text: null, author: 'pg', points: 42, created_at: '2026-10-19T00:00:00Z' },
      { objectID: '2', comment_text: 'I&#x27;d use &lt;b&gt;this&lt;/b&gt;<p>Second para', story_title: 'Show HN: streamrag', author: 'dang', created_at: '2026-10-19T00:01:00Z' },
    ],
  });
  assert.deepEqual(docs, [
    { externalId: '1', title: 'Show HN: streamrag', url: 'https://news.ycombinator.com/item?id=1', body: '', meta: { author: 'pg', points: 42, link: 'https://github.com/x', published: '2026-10-19T00:00:00Z' } },
    { externalId: '2', title: 'Re: Show HN: streamrag', url: 'https://news.ycombinator.com/item?id=2', body: "I'd use <b>this</b>\nSecond para", meta: { author: 'dang', points: null, link: null, published: '2026-10-19T00:01:00Z' } },
  ]);
  assert.throws(() => hackernews.normalize(JSON.parse('{"message":"rate limited"}')), /unexpected Hacker News response/);
});

test('osv keeps live advisories with the versions that fix this package', () => {
  const docs = osv.normalize(
    [
      {
        id: 'GHSA-1',
        summary: 'Cache poisoning',
        details: 'Bad cache.',
        aliases: ['CVE-2026-1'],
        database_specific: { severity: 'HIGH' },
        published: '2026-01-01T00:00:00Z',
        affected: [
          {
            package: { name: 'next' },
            ranges: [
              { type: 'SEMVER', events: [{ introduced: '13.0.0' }, { fixed: '14.2.1' }] },
              { type: 'GIT', events: [{ introduced: '0' }, { fixed: 'abc123' }] },
            ],
          },
          { package: { name: '@next/other' }, ranges: [{ type: 'SEMVER', events: [{ fixed: '9.9.9' }] }] },
        ],
      },
      { id: 'GHSA-2', withdrawn: '2026-02-01T00:00:00Z' },
    ],
    'npm',
    'next',
  );
  assert.deepEqual(docs, [
    {
      externalId: 'GHSA-1',
      title: 'next: Cache poisoning',
      url: 'https://osv.dev/vulnerability/GHSA-1',
      body: 'Bad cache.\n\nFixed in: 14.2.1\n\nAlso known as: CVE-2026-1',
      meta: { ecosystem: 'npm', package: 'next', severity: 'HIGH', published: '2026-01-01T00:00:00Z' },
    },
  ]);
});
