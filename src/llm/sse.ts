// Yields the data payload of each server-sent event. Network chunks can end mid-line (or mid-character),
// so text is buffered until a full line is in.
export async function* sseData(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = '';
  for await (const bytes of body) {
    buffer += decoder.decode(bytes, { stream: true });
    for (let end = buffer.indexOf('\n'); end >= 0; end = buffer.indexOf('\n')) {
      const line = buffer.slice(0, end).replace(/\r$/, '');
      buffer = buffer.slice(end + 1);
      if (line.startsWith('data:')) yield line.slice(5).trimStart();
    }
  }
  buffer += decoder.decode();
  if (buffer.startsWith('data:')) yield buffer.slice(5).trimStart();
}
