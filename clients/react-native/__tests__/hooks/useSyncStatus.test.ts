import { renderHook, act } from '@testing-library/react-native';
import { useSyncStatus } from '../../src/hooks/useSyncStatus';
import { SynchroClient } from '../../src/SynchroClient';
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
  resetNativeModuleMockState();
});

describe('useSyncStatus', () => {
  it('starts with the exact uninitialized status', () => {
    const client = makeClient();
    const { result } = renderHook(() => useSyncStatus(client));

    expect(result.current.status).toBe('uninitialized');
    expect(result.current.retryAt).toBeNull();
    expect(result.current.operation).toBeNull();
    expect(result.current.failure).toBeNull();
  });

  it('updates when status changes', () => {
    const client = makeClient();
    const { result } = renderHook(() => useSyncStatus(client));

    act(() => {
      emitNativeEvent('onStatusChange', {
        status: 'pushing',
        retryAt: null,
        operation: null,
        failure: null,
      });
    });

    expect(result.current.status).toBe('pushing');
  });

  it('cleans up subscription on unmount', () => {
    const client = makeClient();
    const { unmount } = renderHook(() => useSyncStatus(client));
    const remove =
      mockNativeModule.onStatusChange.mock.results[0]?.value?.remove as
        | jest.Mock
        | undefined;

    unmount();

    expect(remove).toBeDefined();
    expect(remove).toHaveBeenCalledTimes(1);

    act(() => {
      emitNativeEvent('onStatusChange', {
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
  });
});
