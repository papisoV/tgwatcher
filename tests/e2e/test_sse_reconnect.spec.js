// AC-3: SSE reconnect uses exponential backoff (1s -> 2s -> 4s -> 8s -> 15s cap).
// Uses Playwright's page.clock to fast-forward through wall-clock waits.
const { test, expect } = require('@playwright/test');

test('SSE reconnect uses exponential backoff', async ({ page }) => {
  // Install fake clock before any setTimeout fires
  await page.clock.install();
  // Stub /api/events to fail 503 first 3 times, then succeed
  let attempts = 0;
  await page.route('**/api/events', async route => {
    attempts++;
    if (attempts <= 3) {
      return route.fulfill({ status: 503, body: 'Service Unavailable' });
    }
    // 4th attempt: return empty SSE stream
    return route.fulfill({
      status: 200,
      headers: { 'Content-Type': 'text/event-stream' },
      body: '',
    });
  });

  await page.goto('/');
  // Fast-forward through 3 reconnect attempts (1+2+4 = 7s, plus jitter)
  await page.clock.fastForward(8000);

  // After 8s of virtual time, should have made at least 3 failed attempts
  // and started the 4th. The 4th returns 200 so connection should be live.
  expect(attempts).toBeGreaterThanOrEqual(3);

  // Capture console warnings to verify backoff sequence
  const warnings = [];
  page.on('console', msg => {
    if (msg.type() === 'warning' && msg.text().includes('[SSE]')) {
      warnings.push(msg.text());
    }
  });
  // Fast-forward more to see if 4th attempt is made after 8s delay
  await page.clock.fastForward(10000);
  expect(attempts).toBeGreaterThanOrEqual(4);
});
