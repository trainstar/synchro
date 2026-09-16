jest.mock('detox', () => ({
  device: { terminateApp: jest.fn().mockResolvedValue(undefined) },
}), { virtual: true });

import { runCorpusCommandLoop } from '../example/e2e/corpus-harness';

it('terminates a faulted corpus app without waiting for network idleness', async () => {
  const { device } = jest.requireMock<{ device: { terminateApp: jest.Mock } }>('detox');
  const failure = new Error('held request did not complete');
  await expect(runCorpusCommandLoop(async () => { throw failure; })).rejects.toBe(failure);
  expect(device.terminateApp).toHaveBeenCalledTimes(1);
});
