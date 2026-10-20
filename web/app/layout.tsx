import '@fontsource-variable/recursive';
import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import { Nav } from './nav';
import './globals.css';

export const metadata: Metadata = {
  title: 'streamrag',
  description: 'Watch the knowledge base sync, and ask it questions.',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="masthead">
          <span className="wordmark">streamrag</span>
          <Nav />
        </header>
        {children}
      </body>
    </html>
  );
}
