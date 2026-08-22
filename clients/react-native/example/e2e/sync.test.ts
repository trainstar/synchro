import { by, device, element, expect, waitFor } from 'detox';
import { loadScenario } from './scenarioLoader';
import { assertUserIsolation, prepareConflict } from './serverSetup';

async function scrollToAndTap(buttonId: string) {
  await waitFor(element(by.id(buttonId)))
    .toBeVisible()
    .whileElement(by.id('test-scroll'))
    .scroll(200, 'down');
  await waitFor(element(by.id(buttonId))).toBeVisible().withTimeout(3000);
  await element(by.id(buttonId)).tap();
}

async function expectBadge(label: string, text = 'PASS', timeout = 5000) {
  await waitFor(element(by.id('last-result-key')))
    .toHaveText(label)
    .withTimeout(timeout);
  try {
    await waitFor(element(by.id('last-result-status')))
      .toHaveText(text)
      .withTimeout(timeout);
  } catch (error) {
    const step = await element(by.id('step-value')).getAttributes();
    const detail = await element(by.id('error-value')).getAttributes();
    throw new Error(
      `badge ${label} did not reach ${text}; step=${String(step.text)} error=${String(detail.text)} original=${String(error)}`
    );
  }
}

describe('shared scenario catalog', () => {
  it('loads an authored scenario by ID', () => {
    const scenario = loadScenario('SCN-SCHEMA-QUEUED-MUTATION-001');
    if (scenario.id !== 'SCN-SCHEMA-QUEUED-MUTATION-001') {
      throw new Error('loaded scenario identity changed');
    }
  });
});

async function runAction(label: string, timeout = 5000) {
  await scrollToAndTap(`btn-${label}`);
  await expectBadge(label, 'PASS', timeout);
}

async function readPendingRecord(testID: string): Promise<string> {
  const attributes = await element(by.id(testID)).getAttributes();
  const recordID = String(attributes.text ?? '');
  if (!recordID || recordID === 'none') {
    throw new Error(`${testID} did not expose a pending record ID`);
  }
  return recordID;
}

async function runConflictAction(timeout = 30000) {
  await scrollToAndTap('btn-conflict');
  try {
    await waitFor(element(by.id('step-value')))
      .toHaveText('conflict:awaiting-server')
      .withTimeout(15000);
  } catch (error) {
    const step = await element(by.id('step-value')).getAttributes();
    const detail = await element(by.id('error-value')).getAttributes();
    throw new Error(
      `conflict setup did not reach server handoff; step=${String(step.text)} error=${String(detail.text)} original=${String(error)}`
    );
  }
  await prepareConflict(await readPendingRecord('conflict-record-id'));
  await scrollToAndTap('btn-conflict');
  await expectBadge('conflict', 'PASS', timeout);
}

async function runMultiUserAction(timeout = 25000) {
  await scrollToAndTap('btn-multiUser');
  await waitFor(element(by.id('step-value')))
    .toHaveText('multiUser:awaiting-server')
    .withTimeout(15000);
  await assertUserIsolation(await readPendingRecord('multi-user-record-id'));
  await scrollToAndTap('btn-multiUser');
  await expectBadge('multiUser', 'PASS', timeout);
}

async function waitForUninitializedStatus(timeout = 15000) {
  await waitFor(element(by.id('status-value')))
    .toHaveText('uninitialized')
    .withTimeout(timeout);
  await waitFor(element(by.id('sync-status')))
    .toBeVisible()
    .whileElement(by.id('test-scroll'))
    .scroll(400, 'up');
}

async function relaunchToIdle() {
  await device.launchApp({ newInstance: true, delete: false });
  await waitForUninitializedStatus(10000);
  await waitFor(element(by.id('btn-reset'))).toBeVisible().withTimeout(5000);
}

async function resetHarnessForTest() {
  await waitFor(element(by.id('btn-reset'))).toBeVisible().withTimeout(5000);
  await element(by.id('btn-reset')).tap();
  try {
    await waitFor(element(by.id('step-value')))
      .toHaveText('reset:complete')
      .withTimeout(30000);
  } catch (error) {
    const step = await element(by.id('step-value')).getAttributes();
    const detail = await element(by.id('error-value')).getAttributes();
    throw new Error(
      `reset did not complete; step=${String(step.text)} error=${String(detail.text)} original=${String(error)}`
    );
  }
  if (device.getPlatform() !== 'ios') {
    await waitForUninitializedStatus(15000);
    return;
  }

  try {
    await waitForUninitializedStatus(4000);
  } catch {
    await relaunchToIdle();
  }
}

describe('Synchro RN E2E', () => {
  beforeAll(async () => {
    await relaunchToIdle();
  });

  beforeEach(async () => {
    await resetHarnessForTest();
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

  it('close and reinitialize roll back active writes and release the lock', async () => {
    await runAction('txRecovery', 20000);
  });

  it('starts sync', async () => {
    await runAction('start', 15000);
  });

  it('awaits native background, foreground, and stop lifecycle transitions', async () => {
    await runAction('lifecycle', 30000);
  });

  it('push/pull round trip, pending changes drain after sync', async () => {
    await runAction('pushPull', 25000);
  });

  it('conflict resolution, detects server-side conflict', async () => {
    await runConflictAction(30000);
  });

  it('multi-user isolation, user 2 cannot see user 1 data', async () => {
    await runMultiUserAction(25000);
  });

  it('stops sync', async () => {
    await runAction('stop', 20000);
  });

  it('maps native errors to typed JS errors', async () => {
    await runAction('errorMap', 10000);
  });

  it('preserves offline writes before first connect and reconciles them on first sync', async () => {
    await runAction('offlineFirst', 20000);
  });

  it('seed database initializes offline with schema and CDC triggers', async () => {
    await runAction('seedInit', 10000);
  });

  it('seed database resumes incrementally without rebuilding shared scope', async () => {
    await runAction('seedResume', 15000);
  });

  it('rejects a corrupt seed without publishing partial state', async () => {
    await runAction('seedCorrupt', 20000);
  });
});
