import type { NextConfig } from 'next';

const config: NextConfig = {
  // The repo root has its own lockfile; this app is its own project.
  turbopack: { root: __dirname },
  // next dev writes an AGENTS.md into the app otherwise.
  agentRules: false,
};

export default config;
