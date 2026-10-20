import '@fontsource-variable/recursive/mono.css';
import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import { apiUrl } from '../lib/config';
import { Nav } from './nav';
import './globals.css';

export const metadata: Metadata = {
  title: 'streamrag',
  description: 'Watch the knowledge base sync, and ask it questions.',
};

const host = (url: string) => {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
};

// The mark: a few bars of the pulse, the newest in the colour of documents that just turned searchable.
function Mark() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" aria-hidden="true">
      <rect className="mark-line" x="2" y="9" width="3" height="9" rx="1" />
      <rect className="mark-line" x="6.5" y="3" width="3" height="15" rx="1" />
      <rect className="mark-line" x="11" y="11" width="3" height="7" rx="1" />
      <rect className="mark-new" x="15.5" y="6" width="3" height="12" rx="1" />
    </svg>
  );
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="bar">
          <span className="brand">
            <Mark />
            streamrag
          </span>
          <Nav />
          <span className="scope">
            API<span className="fig">{host(apiUrl())}</span>
          </span>
        </header>
        {children}
      </body>
    </html>
  );
}
