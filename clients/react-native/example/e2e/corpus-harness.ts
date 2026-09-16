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
