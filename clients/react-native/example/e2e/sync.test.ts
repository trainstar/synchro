import { by, device, element, expect, waitFor } from 'detox';

// Scroll until button is visible, then tap it
async function scrollToAndTap(buttonId: string) {
  // Use Detox's scroll-until-visible pattern
  try {
    await waitFor(element(by.id(buttonId)))
      .toBeVisible()
      .whileElement(by.id('test-scroll'))
      .scroll(200, 'down');
  } catch {
    // Fallback: manual swipe with larger magnitude
    for (let i = 0; i < 12; i++) {
      try {
        await expect(element(by.id(buttonId))).toBeVisible();
        break;
      } catch {
        await element(by.id('test-scroll')).swipe('up', 'slow', 0.5);
        await new Promise((r) => setTimeout(r, 200));
      }
    }
  }
  // Small delay to ensure scroll has settled before tapping
  await new Promise((r) => setTimeout(r, 150));
  await element(by.id(buttonId)).tap();
}

describe('Synchro RN E2E', () => {
  beforeAll(async () => {
    await device.launchApp({ newInstance: true });
    // Disable Detox synchronization — the app uses setInterval (usePendingChanges)
    // which prevents Detox from ever seeing the app as idle.
    // We use explicit waitFor timeouts instead.
    await device.disableSynchronization();
  });

  afterAll(async () => {
    await device.enableSynchronization();
  });

  it('shows the test harness', async () => {
    await waitFor(element(by.id('header')))
      .toBeVisible()
      .withTimeout(5000);
  });

  // -- Initialize and start (High) --

  it('initializes successfully', async () => {
    await scrollToAndTap('btn-init');
    await waitFor(element(by.id('badge-init')))
      .toHaveText('PASS')
      .withTimeout(10000);
  });

  // -- Query and execute (High) --

  it('executes a query', async () => {
    await scrollToAndTap('btn-query');
    await waitFor(element(by.id('badge-query')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  it('executes a write', async () => {
    await scrollToAndTap('btn-execute');
    await waitFor(element(by.id('badge-execute')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  // -- Write transaction commit (Highest) --

  it('write transaction commit', async () => {
    await scrollToAndTap('btn-writeTx');
    await waitFor(element(by.id('badge-writeTx')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  // -- Write transaction rollback (Highest) --

  it('write transaction rollback', async () => {
    await scrollToAndTap('btn-rollbackTx');
    await waitFor(element(by.id('badge-rollbackTx')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  // -- Read transaction (High) --

  it('read transaction', async () => {
    await scrollToAndTap('btn-readTx');
    await waitFor(element(by.id('badge-readTx')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  // -- Transaction timeout (Highest) --

  it('transaction timeout triggers rollback', async () => {
    await scrollToAndTap('btn-txTimeout');
    await waitFor(element(by.id('badge-txTimeout')))
      .toHaveText('PASS')
      .withTimeout(15000);
  });

  // -- Transaction error recovery (Highest) --

  it('transaction error recovery — write lock released after timeout', async () => {
    await scrollToAndTap('btn-txRecovery');
    await waitFor(element(by.id('badge-txRecovery')))
      .toHaveText('PASS')
      .withTimeout(20000);
  });

  // -- Sync lifecycle --

  it('starts sync', async () => {
    await scrollToAndTap('btn-start');
    await waitFor(element(by.id('badge-start')))
      .toHaveText('PASS')
      .withTimeout(10000);
  });

  // -- Push/pull round trip (High) --

  it('push/pull round trip — pending changes drain after sync', async () => {
    await scrollToAndTap('btn-pushPull');
    await waitFor(element(by.id('badge-pushPull')))
      .toHaveText('PASS')
      .withTimeout(20000);
  });

  // -- Conflict resolution (High) --

  it('conflict resolution — detects server-side conflict', async () => {
    await scrollToAndTap('btn-conflict');
    await waitFor(element(by.id('badge-conflict')))
      .toHaveText('PASS')
      .withTimeout(30000);
  });

  // -- Multi-user isolation (Medium) --

  it('multi-user isolation — user 2 cannot see user 1 data', async () => {
    await scrollToAndTap('btn-multiUser');
    await waitFor(element(by.id('badge-multiUser')))
      .toHaveText('PASS')
      .withTimeout(20000);
  });

  // -- Status change events (Medium) --

  it('shows sync status', async () => {
    // Scroll back to top
    for (let i = 0; i < 5; i++) {
      try {
        await expect(element(by.id('sync-status'))).toBeVisible();
        break;
      } catch {
        await element(by.id('test-scroll')).swipe('down', 'slow', 0.3);
      }
    }
    await waitFor(element(by.id('sync-status')))
      .toBeVisible()
      .withTimeout(3000);
  });

  it('stops sync', async () => {
    await scrollToAndTap('btn-stop');
    await waitFor(element(by.id('badge-stop')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });

  // -- Error mapping (Medium) --

  it('maps native errors to typed JS errors', async () => {
    await scrollToAndTap('btn-errorMap');
    await waitFor(element(by.id('badge-errorMap')))
      .toHaveText('PASS')
      .withTimeout(5000);
  });
});
