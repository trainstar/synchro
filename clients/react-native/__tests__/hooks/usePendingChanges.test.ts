import { renderHook, act, waitFor } from '@testing-library/react-native';
import { usePendingChanges } from '../../src/hooks/usePendingChanges';
import { SynchroClient } from '../../src/SynchroClient';
import { resetNativeModuleMockState } from '../__mocks__/react-native';

function makeClient(): SynchroClient {
  return new SynchroClient({
    dbPath: '/test.db',
    serverURL: 'http://localhost:8080',
    authProvider: async () => 'test-token',
    clientID: 'test-client',
    appVersion: '1.0.0',
  });
}

describe('usePendingChanges', () => {
  beforeEach(() => {
    resetNativeModuleMockState();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('starts with 0 before first poll resolves', async () => {
    const client = makeClient();
    // Mock never resolves during this test — we only check synchronous initial state
    jest.spyOn(client, 'pendingChangeCount').mockReturnValue(new Promise(() => {}));

    const { result } = renderHook(() => usePendingChanges(client, 1000));

    expect(result.current).toBe(0);
  });

  it('polls and updates count', async () => {
    const client = makeClient();
    jest
      .spyOn(client, 'pendingChangeCount')
      .mockResolvedValueOnce(3)
      .mockResolvedValueOnce(5);

    const { result } = renderHook(() => usePendingChanges(client, 1000));

    // Flush the initial async poll within act boundary
    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current).toBe(3);

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    expect(result.current).toBe(5);
    expect(client.pendingChangeCount).toHaveBeenCalledTimes(2);
  });

  it('ignores polling errors and keeps the last good count', async () => {
    const client = makeClient();
    jest
      .spyOn(client, 'pendingChangeCount')
      .mockResolvedValueOnce(2)
      .mockRejectedValueOnce(new Error('temporary failure'));

    const { result } = renderHook(() => usePendingChanges(client, 1000));

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current).toBe(2);

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    expect(result.current).toBe(2);
  });

  it('cleans up interval on unmount', async () => {
    const client = makeClient();
    jest.spyOn(client, 'pendingChangeCount').mockResolvedValue(0);

    const { unmount } = renderHook(() => usePendingChanges(client, 1000));

    // Flush initial poll
    await act(async () => {
      await Promise.resolve();
    });

    const callCount = (client.pendingChangeCount as jest.Mock).mock.calls.length;
    unmount();

    // Advancing timers after unmount should not call pendingChangeCount again
    jest.advanceTimersByTime(5000);
    expect((client.pendingChangeCount as jest.Mock).mock.calls.length).toBe(callCount);
  });
});
