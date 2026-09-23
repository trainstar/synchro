import { Component, type ReactNode } from 'react';
import { renderHook, act, waitFor } from '@testing-library/react-native';
import { useSyncStatus } from '../../src/hooks/useSyncStatus';
import { SynchroClient } from '../../src/SynchroClient';
import type { SyncStatus } from '../../src/types';
import {
  emitNativeEvent,
  mockNativeModule,
  resetNativeModuleMockState,
} from '../__mocks__/react-native';

function makeClient(): SynchroClient {
  return new SynchroClient({
    dbPath: '/test.db',
    serverURL: 'http://localhost:8080',
    authProvider: async () => 'test-token',
    clientID: 'test-client',
    appVersion: '1.0.0',
  });
}

beforeEach(() => {
  jest.restoreAllMocks();
  resetNativeModuleMockState();
});

const ready: SyncStatus = { status: 'ready', retryAt: null, operation: null, failure: null };
const pushing: SyncStatus = { status: 'pushing', retryAt: null, operation: null, failure: null };

describe('useSyncStatus', () => {
  it('starts uninitialized and loads the current status without an event', async () => {
    const client = makeClient();
    const pending = Promise.withResolvers<SyncStatus>();
    jest.spyOn(client, 'getSyncStatus').mockReturnValue(pending.promise);
    const { result } = renderHook(() => useSyncStatus(client));

    expect(result.current.status).toBe('uninitialized');
    expect(result.current.retryAt).toBeNull();
    expect(result.current.operation).toBeNull();
    expect(result.current.failure).toBeNull();
    await act(async () => pending.resolve(ready));
    expect(result.current).toEqual(ready);
  });

  it.each(['resolve', 'reject'] as const)('protects a newer event from a late snapshot %s', async (settle) => {
    const client = makeClient();
    const pending = Promise.withResolvers<SyncStatus>();
    jest.spyOn(client, 'getSyncStatus').mockImplementation(() => {
      emitNativeEvent('onStatusChange', pushing);
      return pending.promise;
    });
    const { result } = renderHook(() => useSyncStatus(client));

    expect(result.current).toEqual(pushing);
    await act(async () => {
      if (settle === 'resolve') pending.resolve(ready);
      else pending.reject(new Error('snapshot failed'));
    });
    expect(result.current).toEqual(pushing);
  });

  it('clears the previous client and ignores its late snapshot and captured callback', async () => {
    const first = makeClient();
    const second = makeClient();
    const firstSnapshot = Promise.withResolvers<SyncStatus>();
    const secondSnapshot = Promise.withResolvers<SyncStatus>();
    jest.spyOn(first, 'getSyncStatus').mockReturnValue(firstSnapshot.promise);
    jest.spyOn(second, 'getSyncStatus').mockReturnValue(secondSnapshot.promise);
    const firstSubscription = jest.spyOn(first, 'onStatusChange');
    const seen: SyncStatus[] = [];
    const { result, rerender } = renderHook(({ client }) => {
      const status = useSyncStatus(client);
      seen.push(status);
      return status;
    }, { initialProps: { client: first } });
    const oldCallback = firstSubscription.mock.calls[0][0];
    act(() => oldCallback(pushing));
    expect(result.current).toEqual(pushing);

    seen.length = 0;
    rerender({ client: second });
    expect(seen.every((status) => status.status === 'uninitialized')).toBe(true);
    await act(async () => {
      oldCallback(pushing);
      firstSnapshot.resolve(pushing);
    });
    expect(result.current.status).toBe('uninitialized');
    await act(async () => secondSnapshot.resolve(ready));
    expect(result.current).toEqual(ready);
    act(() => oldCallback(pushing));
    expect(result.current).toEqual(ready);
  });

  it.each(['resolve', 'reject'] as const)('discards a previous client snapshot that settles with %s', async (settle) => {
    const first = makeClient();
    const second = makeClient();
    const pending = Promise.withResolvers<SyncStatus>();
    jest.spyOn(first, 'getSyncStatus').mockReturnValue(pending.promise);
    jest.spyOn(second, 'getSyncStatus').mockResolvedValue(ready);
    const { result, rerender } = renderHook(
      ({ client }) => useSyncStatus(client),
      { initialProps: { client: first } }
    );
    await act(async () => rerender({ client: second }));
    expect(result.current).toEqual(ready);
    await act(async () => {
      if (settle === 'resolve') pending.resolve(pushing);
      else pending.reject(new Error('old snapshot failed'));
    });
    expect(result.current).toEqual(ready);
  });

  it('surfaces an initial snapshot failure through the React error boundary', async () => {
    const client = makeClient();
    jest.spyOn(client, 'getSyncStatus').mockRejectedValue(new Error('snapshot failed'));
    const caught = jest.fn();
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
    class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
      state = { failed: false };
      static getDerivedStateFromError() { return { failed: true }; }
      componentDidCatch(error: Error) { caught(error); }
      render() { return this.state.failed ? null : this.props.children; }
    }
    renderHook(() => useSyncStatus(client), { wrapper: Boundary });
    await waitFor(() => expect(caught).toHaveBeenCalledWith(expect.objectContaining({ code: 'UNKNOWN' })));
    consoleError.mockRestore();
  });

  it('cleans up subscription and ignores a pending snapshot on unmount', async () => {
    const client = makeClient();
    const pending = Promise.withResolvers<SyncStatus>();
    jest.spyOn(client, 'getSyncStatus').mockReturnValue(pending.promise);
    const { unmount } = renderHook(() => useSyncStatus(client));
    const remove =
      mockNativeModule.onStatusChange.mock.results[0]?.value?.remove as
        | jest.Mock
        | undefined;

    unmount();

    expect(remove).toBeDefined();
    expect(remove).toHaveBeenCalledTimes(1);

    let notified = -1;
    act(() => {
      notified = emitNativeEvent('onStatusChange', {
        status: 'error',
        retryAt: null,
        operation: null,
        failure: {
          operation: 'database',
          code: 'local_database',
          retryable: false,
          message: 'local failure',
          recoveryAction: 'retry',
          metadata: {},
        },
      });
    });

    expect(notified).toBe(0);
    await act(async () => pending.resolve(ready));
  });
});
