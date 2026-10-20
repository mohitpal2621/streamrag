import { defineConfig, devices } from '@playwright/test';

// npm test starts the e2e stack (the real API on a Testcontainers Postgres, with stand-in models), then
// builds and starts the dashboard against it. One worker, since a test stops and restarts the API.
export default defineConfig({
  testDir: 'e2e',
  timeout: 30_000,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['list'], ['github']] : 'list',
  use: {
    baseURL: 'http://localhost:3100',
    trace: 'retain-on-failure',
    ...devices['Desktop Chrome'],
    // A Chromium that is already installed, when Playwright's own download isn't available
    launchOptions: { executablePath: process.env.CHROMIUM_PATH || undefined },
  },
  webServer: [
    { command: 'node e2e/stack.mts', url: 'http://localhost:4101/ready', timeout: 180_000, stdout: 'pipe' },
    { command: 'npx next build && npx next start -p 3100', url: 'http://localhost:3100/ask', timeout: 300_000, env: { API_URL: 'http://localhost:4100/graphql' } },
  ],
});
