// shortcut: the common named entities only; swap in a full HTML entity table if rarer ones start to matter.
const NAMED: Record<string, string> = {
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ',
  lsquo: '‘', rsquo: '’', ldquo: '“', rdquo: '”', ndash: '–', mdash: '—', hellip: '…', bull: '•', middot: '·',
  rarr: '→', larr: '←', times: '×', copy: '©', pound: '£', euro: '€', eacute: 'é',
};

export const decodeEntities = (s: string) =>
  s.replace(/&(?:#(\d+)|#x([\da-f]+)|([a-z]+));/gi, (m, dec?: string, hex?: string, name?: string) => {
    if (name) return NAMED[name.toLowerCase()] ?? m;
    const code = Number(dec ?? `0x${hex}`);
    return code <= 0x10ffff ? String.fromCodePoint(code) : m;
  });

export const htmlToText = (html: string) =>
  decodeEntities(html.replace(/<(?:br|p|\/p|\/div|\/li|\/h[1-6])\b[^>]*>/gi, '\n').replace(/<[^>]*>/g, ' '))
    .replace(/[ \t ]+/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .trim();
