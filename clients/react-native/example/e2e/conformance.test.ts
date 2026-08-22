import { by, device, element, expect, waitFor } from 'detox';

describe('React Native conformance host', () => {
  beforeAll(async () => {
    await device.launchApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });
  });

  it('emits the strict error envelope for an invalid command', async () => {
    await expect(element(by.id('conformance-harness'))).toBeVisible();
    await element(by.id('conformance-command-input')).replaceText('{');
    await element(by.id('btn-conformance-execute')).tap();
    await waitFor(element(by.id('conformance-result')))
      .toHaveText('{"schema_version":1,"outcome":"error","result":null,"error_code":"invalid_command"}')
      .withTimeout(5000);
  });
});
