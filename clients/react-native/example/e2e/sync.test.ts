import { by, device, element, expect, waitFor } from 'detox';

async function scrollToAndTap(buttonId: string) {
  try {
    await waitFor(element(by.id(buttonId)))
      .toBeVisible()
      .whileElement(by.id('test-scroll'))
      .scroll(200, 'down');
  } catch {
    for (let i = 0; i < 10; i += 1) {
      try {
        await expect(element(by.id(buttonId))).toBeVisible();
        break;
      } catch {
        await element(by.id('test-scroll')).swipe('up', 'slow', 0.5);
      }
    }
  }

  await waitFor(element(by.id(buttonId))).toBeVisible().withTimeout(3000);
  await element(by.id(buttonId)).tap();
}

async function scrollToBadge(label: string) {
  const badge = element(by.id(`badge-${label}`));
  for (let i = 0; i < 8; i += 1) {
    try {
      await expect(badge).toBeVisible();
      return;
    } catch {
      await element(by.id('test-scroll')).swipe('down', 'slow', 0.5);
    }
  }
}

async function scrollToTopAndTap(buttonId: string) {
  for (let i = 0; i < 10; i += 1) {
    try {
      await expect(element(by.id(buttonId))).toBeVisible();
      break;
    } catch {
      await element(by.id('test-scroll')).swipe('down', 'slow', 0.75);
    }
  }

  await waitFor(element(by.id(buttonId))).toBeVisible().withTimeout(5000);
  await element(by.id(buttonId)).tap();
}

async function expectBadge(label: string, text = 'PASS', timeout = 5000) {
  await waitFor(element(by.id('last-result-key')))
    .toHaveText(label)
    .withTimeout(timeout);
  await waitFor(element(by.id('last-result-status')))
    .toHaveText(text)
    .withTimeout(timeout);
}

async function runAction(label: string, timeout = 5000) {
  await scrollToAndTap(`btn-${label}`);
  await expectBadge(label, 'PASS', timeout);
}

describe('Synchro RN E2E', () => {
  beforeAll(async () => {
    if (device.getPlatform() === 'android') {
      await device.launchApp({ newInstance: true, delete: true });
    }
  });

  beforeEach(async () => {
    if (device.getPlatform() === 'ios') {
      await device.launchApp({ newInstance: true, delete: true });
      await waitFor(element(by.id('status-value')))
        .toHaveText('idle')
        .withTimeout(5000);
      await waitFor(element(by.id('btn-reset'))).toBeVisible().withTimeout(5000);
    } else {
      await scrollToTopAndTap('btn-reset');
      await waitFor(element(by.id('status-value')))
        .toHaveText('idle')
        .withTimeout(15000);
      await waitFor(element(by.id('sync-status')))
        .toBeVisible()
        .whileElement(by.id('test-scroll'))
        .scroll(400, 'up');
    }
  });

  it('shows the test harness', async () => {
    await expect(element(by.id('sync-status'))).toBeVisible();
    await expect(element(by.id('btn-reset'))).toBeVisible();
  });

  it('initializes successfully', async () => {
    await runAction('init', 10000);
  });

  it('executes a query', async () => {
    await runAction('query', 10000);
  });

  it('executes a write', async () => {
    await runAction('execute', 10000);
  });

  it('write transaction commit', async () => {
    await runAction('writeTx', 10000);
  });

  it('write transaction rollback', async () => {
    await runAction('rollbackTx', 10000);
  });

  it('read transaction', async () => {
    await runAction('readTx', 10000);
  });

  it('transaction timeout triggers rollback', async () => {
    await runAction('txTimeout', 15000);
  });

  it('transaction error recovery — write lock released after timeout', async () => {
    await runAction('txRecovery', 20000);
  });

  it('starts sync', async () => {
    await runAction('start', 15000);
  });

  it('push/pull round trip — pending changes drain after sync', async () => {
    await runAction('pushPull', 25000);
  });

  it('conflict resolution — detects server-side conflict', async () => {
    await runAction('conflict', 30000);
  });

  it('multi-user isolation — user 2 cannot see user 1 data', async () => {
    await runAction('multiUser', 25000);
  });

  it('stops sync', async () => {
    await runAction('stop', 20000);
  });

  it('maps native errors to typed JS errors', async () => {
    await runAction('errorMap', 10000);
  });

  it('seed database initializes offline with schema and CDC triggers', async () => {
    await runAction('seedInit', 10000);
  });
});
