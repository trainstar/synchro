import { by, device, element, expect, waitFor } from 'detox';

describe('reset acknowledgement diagnostic', () => {
  for (let iteration = 0; iteration < 30; iteration += 1) {
    it(`resets after fresh-process initialization ${iteration}`, async () => {
      await device.launchApp({ newInstance: true, delete: false });
      await waitFor(element(by.id('status-value')))
        .toHaveText('uninitialized')
        .withTimeout(10000);
      await element(by.id('btn-reset')).tap();
      await waitFor(element(by.id('step-value')))
        .toHaveText('reset:complete')
        .withTimeout(30000);
      await element(by.id('btn-init')).tap();
      await waitFor(element(by.id('last-result-key')))
        .toHaveText('init')
        .withTimeout(10000);
      await expect(element(by.id('last-result-status'))).toHaveText('PASS');
      await element(by.id('btn-reset')).tap();
      try {
        await waitFor(element(by.id('step-value')))
          .toHaveText('reset:complete')
          .withTimeout(30000);
      } catch (error) {
        const step = await element(by.id('step-value')).getAttributes();
        const status = await element(by.id('status-value')).getAttributes();
        throw new Error(`reset ${iteration}: step=${String(step.text)} status=${String(status.text)} cause=${String(error)}`);
      }
    });
  }
});
