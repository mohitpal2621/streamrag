import type { Metadata } from 'next';
import { apiUrl } from '../../lib/config';
import { AskPanel } from './ask-panel';

export const dynamic = 'force-dynamic';
export const metadata: Metadata = { title: 'Ask | streamrag' };

export default function AskPage() {
  return <AskPanel api={apiUrl()} />;
}
