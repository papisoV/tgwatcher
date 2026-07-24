// AC-2: loadMessages request sequencing — rapid chat switches must show
// the final chat's messages, not a stale response from an earlier request.
const { test, expect } = require('@playwright/test');

test('rapid chat switches show final chat messages', async ({ page }) => {
  await page.goto('/');
  // Wait for initial load
  await page.waitForSelector('#msgBody');
  await page.waitForTimeout(1500);

  // Collect all /api/messages responses
  const responses = [];
  page.on('response', async resp => {
    if (resp.url().includes('/api/messages')) {
      try {
        const data = await resp.json();
        responses.push({ url: resp.url(), data });
      } catch (e) { /* non-json, skip */ }
    }
  });

  // Click 3 different chat rows rapidly (if sidebar has multiple)
  const chatItems = await page.$$('#chatList .chat-item');
  test.skip(chatItems.length < 3, 'Need at least 3 chats for race test');

  for (let i = 0; i < 3; i++) {
    await chatItems[i].click();
  }
  // Wait for last fetch to settle
  await page.waitForTimeout(2000);

  // The last successful response should be the currently selected chat
  const last = responses[responses.length - 1];
  expect(last).toBeTruthy();
  // Verify the message list shown matches the last response
  const visibleSenders = await page.$$eval('#msgBody tr', rows =>
    rows.map(r => r.querySelector('.col-sender')?.textContent || '')
  );
  // All non-empty senders should come from the last response data
  const lastSenders = (last.data.messages || []).map(m => m.sender_name || m.sender_username || '');
  for (const vs of visibleSenders.filter(s => s && s !== '-')) {
    expect(lastSenders.some(ls => ls.includes(vs) || vs.includes(ls))).toBeTruthy();
  }
});
