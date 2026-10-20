'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const LINKS = [
  { href: '/', label: 'Live' },
  { href: '/ask', label: 'Ask' },
];

export function Nav() {
  const path = usePathname();
  return (
    <nav aria-label="Pages">
      {LINKS.map((l) => (
        <Link key={l.href} href={l.href} aria-current={path === l.href ? 'page' : undefined}>
          {l.label}
        </Link>
      ))}
    </nav>
  );
}
