// promptfoo ships SDKs for dozens of model providers as optional packages, over 1 GB in all, and this
// eval needs none of them. So it installs without them, plus the one native SQLite binding promptfoo
// won't start without, for whichever platform this is.
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';

const dir = import.meta.dirname;
const npm = (...args: string[]) =>
  execFileSync('npm', [...args, '--omit=optional', '--no-audit', '--no-fund'], { cwd: dir, stdio: 'inherit', shell: process.platform === 'win32' });

if (!existsSync(`${dir}/node_modules/promptfoo`)) {
  npm('install');
  const { version } = JSON.parse(readFileSync(`${dir}/node_modules/libsql/package.json`, 'utf8'));
  const platform = { linux: `linux-${process.arch}-gnu`, darwin: `darwin-${process.arch}`, win32: `win32-${process.arch}-msvc` }[process.platform as string];
  if (!platform) throw new Error(`no SQLite binding known for ${process.platform}; run npm install in eval/llm instead`);
  npm('install', '--no-save', `@libsql/${platform}@${version}`);
}
