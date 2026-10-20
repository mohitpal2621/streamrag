import type { NextConfig } from 'next';

const config: NextConfig = {
  // The repo root has its own lockfile; this app is its own project.
  turbopack: { root: __dirname },
};

export default config;
