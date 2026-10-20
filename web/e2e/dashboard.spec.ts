import { expect, test } from '@playwright/test';

const control = async (path: string, body: object) => {
  const res = await fetch(`http://localhost:4101${path}`, { method: 'POST', body: JSON.stringify(body) });
  expect(res.ok, await res.text()).toBe(true);
};

test.describe('live page', () => {
  test('shows what synced, the pipeline, alerts, feeds and model usage', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Live sync' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Synced in the last 15 minutes' })).toBeVisible();
    await expect(page.getByRole('status')).toHaveText('Live');
    await expect(page.locator('.finding')).toContainText("can't reach Kafka");
    await expect(page.locator('.stages')).toContainText(/\d+ documents, \d+ chunks/);
    // Embedded before the page opened, so the API supplies its time to searchable.
    await expect(page.locator('.feed li').filter({ hasText: 'Vacuum settings' }).locator('.state')).toHaveText(/^After \d/);
    await expect(page.locator('.changes .card-head')).toContainText('Searchable a median of');
    await expect(page.locator('.pulse .by-source')).toContainText('Blogs, podcasts and releases');
    await expect(page.getByText("Kafka isn't reachable")).toBeVisible();
    await expect(page.locator('table').filter({ hasText: 'Feed' }).first()).toContainText('lodash advisories');
    await expect(page.locator('table').filter({ hasText: 'Feed' }).first()).toContainText('kb.example');
    await expect(page.locator('section').filter({ has: page.getByRole('heading', { name: 'Models, last 7 days' }) })).toContainText('llama3.2:3b');
  });

  test('a sync shows up as it commits and turns searchable when it is embedded', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('status')).toHaveText('Live');
    await control('/sync', { title: 'a comment the test just posted' });
    const row = page.locator('.feed li').filter({ hasText: 'a comment the test just posted' });
    await expect(row).toBeVisible();
    await expect(row.locator('.state')).toHaveText(/^After \d/);
    await expect(row).toHaveClass(/fresh/);
    await expect(page.locator('.changes .card-head')).toContainText('Searchable a median of');
  });

  test('a new security alert shows up live', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('status')).toHaveText('Live');
    await control('/alert', { id: 'GHSA-live' });
    const alert = page.locator('.alerts li').first();
    await expect(alert).toContainText('express');
    await expect(alert.getByRole('link')).toHaveAttribute('href', 'https://osv.dev/GHSA-live');
  });

  test('says how to start the API while it is down, and reconnects when it is back', async ({ page, context }) => {
    await page.goto('/');
    await expect(page.getByRole('status')).toHaveText('Live');
    await control('/api/stop', {});
    await expect(page.getByRole('status')).toHaveText('Reconnecting');

    const fresh = await context.newPage();
    await fresh.goto('/');
    await expect(fresh.getByRole('heading', { name: "The streamrag API isn't answering" })).toBeVisible();
    await expect(fresh.locator('.notice')).toContainText('npm run serve');
    await fresh.close();

    await control('/api/start', {});
    await expect(page.getByRole('status')).toHaveText('Live', { timeout: 20_000 });
    await control('/sync', { title: 'posted after the restart' });
    await expect(page.locator('.feed li').filter({ hasText: 'posted after the restart' })).toBeVisible();
  });
});

test.describe('ask page', () => {
  test('streams an answer whose citations link to the sources', async ({ page }) => {
    await page.goto('/ask');
    await page.getByLabel('Question').fill('how do I keep postgres tables small?');
    await page.getByRole('button', { name: 'Ask', exact: true }).click();
    const turn = page.locator('.turn').first();
    await expect(turn.locator('.answer')).toHaveText('Vacuum reclaims dead rows, which keeps tables small [1].');
    await expect(turn.locator('.answer .cite')).toHaveAttribute('href', 'https://kb.example/vacuum');
    await expect(turn.locator('.sources li.cited')).toHaveText('Vacuum settings');
    await expect(turn.locator('.meta')).toHaveText('Answered by stand-in');
  });

  test('the agent shows its tool calls and keeps the conversation for a follow-up', async ({ page }) => {
    await page.goto('/ask');
    await page.getByLabel('Question').fill('what keeps tables small?');
    await page.getByRole('button', { name: 'Ask the agent' }).click();
    const first = page.locator('.turn').first();
    await expect(first.locator('.steps')).toHaveText('Searched for “what keeps tables small?”');
    await expect(first.locator('.answer')).toContainText('This is question 1 in this conversation.');

    await page.getByLabel('Question').fill('and what else?');
    await page.getByRole('button', { name: 'Ask the agent' }).click();
    await expect(page.locator('.turn').nth(1).locator('.answer')).toContainText('This is question 2 in this conversation.');

    await page.getByRole('button', { name: 'Start over' }).click();
    await expect(page.locator('.turn h2')).toHaveCount(0);
  });

  test('search lists matches with links', async ({ page }) => {
    await page.goto('/ask');
    await page.getByLabel('Question').fill('vacuum');
    await page.getByRole('button', { name: 'Search' }).click();
    await expect(page.locator('.hits li').first().getByRole('link')).toHaveText('Vacuum settings');
  });

  test('an example question fills the box', async ({ page }) => {
    await page.goto('/ask');
    await page.getByRole('button', { name: 'Which advisories affect lodash 4.17.20?' }).click();
    await expect(page.getByLabel('Question')).toHaveValue('Which advisories affect lodash 4.17.20?');
  });
});
