import type pg from 'pg';
import type { Gateway } from '../llm/gateway.ts';
import type { Message } from '../llm/openai.ts';

export type Breaking = { change: string; migration: string };

const SYSTEM = `You read software release notes and list the breaking changes in them: anything that can make code, configuration or a build that worked on the previous version fail or behave differently after upgrading. That covers removed or renamed APIs and options, changed defaults, stricter validation and dropped runtime versions. Leave out new features, bug fixes, security fixes, dependency bumps and deprecations that still work, unless the notes say they change behavior.
Reply with JSON only, shaped like {"breaking": [{"change": "...", "migration": "..."}]}. "migration" says what to do instead, or is "" when the notes don't say. Reply {"breaking": []} when nothing breaks.`;

// Ollama, Gemini, Groq and llama.cpp all constrain the reply to this schema. It still gets checked,
// since an older server may ignore it.
const RESPONSE_FORMAT = {
  type: 'json_schema',
  json_schema: {
    name: 'breaking_changes',
    schema: {
      type: 'object',
      properties: {
        breaking: {
          type: 'array',
          items: { type: 'object', properties: { change: { type: 'string' }, migration: { type: 'string' } }, required: ['change', 'migration'] },
        },
      },
      required: ['breaking'],
    },
  },
};

// In a changelog, commit URLs are most of the tokens and say nothing about what changed.
const withoutLinks = (notes: string) => notes.replace(/\[([^\]]*)\]\([^)]*\)/g, '$1');

// Conventional commits mark a breaking change with "!" after the type (feat!: …) or a
// "BREAKING CHANGE:" line. Those lines are taken as they are, minus the PR number and author; the
// model only judges the rest, since a small one copies long lists badly.
const MARKED = /^\s*(?:[-*]\s+)?(?:`?[0-9a-f]{7,40}`?\s+)?(?:\w+(?:\([^)]*\))?!:|BREAKING[ -]CHANGES?:)\s*(.+)$/i;
export function markedBreaking(notes: string) {
  const marked: Breaking[] = [];
  const rest: string[] = [];
  for (const line of withoutLinks(notes).split('\n')) {
    const m = line.match(MARKED);
    if (m) marked.push({ change: m[1].replace(/\s*\(#\d+\)(?:\s*\([^)]*\))?\s*$/, '').trim(), migration: '' });
    else rest.push(line);
  }
  return { marked, rest: rest.join('\n') };
}

// Long notes go to the model in pieces split between lines, so a small local model with a 4K-token
// context still reads each piece whole.
export function pieces(notes: string, maxChars = 4_000): string[] {
  const out: string[] = [];
  let current = '';
  for (const line of withoutLinks(notes).split('\n')) {
    if (current && current.length + line.length + 1 > maxChars) {
      out.push(current);
      current = '';
    }
    current += current ? `\n${line}` : line;
  }
  if (current.trim()) out.push(current);
  return out;
}

function parse(reply: string): Breaking[] | null {
  try {
    const { breaking } = JSON.parse(reply.replace(/^\s*```(?:json)?|```\s*$/g, ''));
    if (!Array.isArray(breaking)) return null;
    return breaking.flatMap((b) =>
      typeof b?.change === 'string' && b.change.trim()
        ? [{ change: b.change.trim(), migration: typeof b.migration === 'string' ? b.migration.trim() : '' }]
        : [],
    );
  } catch {
    return null;
  }
}

export async function extractBreaking(gateway: Gateway, title: string, notes: string): Promise<Breaking[]> {
  const { marked, rest } = markedBreaking(notes);
  const found = new Map(marked.map((b) => [b.change.toLowerCase(), b]));
  for (const piece of pieces(rest)) {
    const messages: Message[] = [
      { role: 'system', content: SYSTEM },
      { role: 'user', content: `Release notes: ${title}\n\n${piece}` },
    ];
    let items: Breaking[] | null = null;
    // A reply that isn't the JSON asked for gets one more try before the release is given up on.
    for (let attempt = 0; attempt < 2 && !items; attempt++) {
      let reply = '';
      for await (const t of gateway.stream(messages, { response_format: RESPONSE_FORMAT, temperature: 0 })) reply += t.text;
      items = parse(reply);
    }
    if (!items) throw new Error(`the model did not reply with the JSON asked for, twice, for "${title}"`);
    for (const b of items) if (!found.has(b.change.toLowerCase())) found.set(b.change.toLowerCase(), b);
  }
  return [...found.values()];
}

// Extracts the breaking changes of the given documents, skipping any whose current version was
// already done. Each document's rows are replaced in one transaction, like its chunks.
export async function extractPending(db: pg.Pool, gateway: Gateway, ids: string[], onRead?: (title: string) => void) {
  const { rows: docs } = await db.query<{ id: string; title: string; body: string; content_hash: string }>(
    `select id, title, body, content_hash from documents
     where id = any($1::bigint[]) and extracted_hash is distinct from content_hash order by id`,
    [ids],
  );
  for (const d of docs) {
    onRead?.(d.title);
    const found = await extractBreaking(gateway, d.title, d.body);
    const client = await db.connect();
    try {
      await client.query('begin');
      await client.query('delete from breaking_changes where document_id = $1', [d.id]);
      await client.query(
        `insert into breaking_changes (document_id, ord, change, migration)
         select $1, t.ord, t.change, t.migration from unnest($2::text[], $3::text[]) with ordinality t(change, migration, ord)`,
        [d.id, found.map((b) => b.change), found.map((b) => b.migration)],
      );
      await client.query('update documents set extracted_hash = $2 where id = $1', [d.id, d.content_hash]);
      await client.query('commit');
    } catch (err) {
      await client.query('rollback');
      throw err;
    } finally {
      client.release();
    }
  }
  return docs.length;
}
