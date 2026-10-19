// Splits a document into ~200-word windows that overlap by 40 words, so a sentence cut at one
// boundary is whole in the next. Each chunk starts with the title, so it still says what it is
// about when it is retrieved on its own. 200 words stays well inside the embedder's 512 tokens.
export function chunk(title: string, body: string, size = 200, overlap = 40): string[] {
  const words = body.split(/\s+/).filter(Boolean);
  if (!words.length) return [title];
  const chunks: string[] = [];
  for (let start = 0; ; start += size - overlap) {
    chunks.push(`${title}\n${words.slice(start, start + size).join(' ')}`);
    if (start + size >= words.length) return chunks;
  }
}
