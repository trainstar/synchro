import { by, device, element, waitFor } from 'detox';

type LaunchOptions = Parameters<typeof device.launchApp>[0];

export async function runCorpusCommandLoop(execute: () => Promise<void>): Promise<void> {
  try {
    return await execute();
  } finally {
    await device.enableSynchronization();
  }
}

export async function launchCorpusApp(options: LaunchOptions): Promise<void> {
  await device.launchApp(options);
  await device.disableSynchronization();
  await waitFor(element(by.id('conformance-harness'))).toBeVisible().withTimeout(30000);
}

export async function submitCorpusCommand(serialized: string): Promise<void> {
  const input = element(by.id('conformance-command-input'));
  const state = element(by.id('conformance-command-state'));
  await input.replaceText('');
  await waitFor(state).toHaveText('idle').withTimeout(30000);
  await input.replaceText(serialized);
  await waitFor(state).toHaveText('ready').withTimeout(30000);
  const attributes = await input.getAttributes();
  if (attributes.text !== serialized) {
    throw new Error('React Native conformance command input changed');
  }
  await input.tapReturnKey();
  await element(by.id('btn-conformance-execute')).tap();
}
