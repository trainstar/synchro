import { renderHook, act } from '@testing-library/react-native';
import { useSyncStatus } from '../../src/hooks/useSyncStatus';
import { SynchroClient } from '../../src/SynchroClient';
import { emitNativeEvent } from '../__mocks__/react-native';

function makeClient(): SynchroClient {
  return new SynchroClient({
    dbPath: '/test.db',
    serverURL: 'http://localhost:8080',
    authProvider: async () => 'test-token',
    clientID: 'test-client',
    appVersion: '1.0.0',
  });
}

describe('useSyncStatus', () => {
  it('starts with idle status', () => {
    const client = makeClient();
    const { result } = renderHook(() => useSyncStatus(client));

    expect(result.current.status).toBe('idle');
    expect(result.current.retryAt).toBeNull();
  });

  it('updates when status changes', () => {
    const client = makeClient();
    const { result } = renderHook(() => useSyncStatus(client));

    act(() => {
      emitNativeEvent('onStatusChange', { status: 'syncing', retryAt: null });
    });

    expect(result.current.status).toBe('syncing');
  });

  it('cleans up subscription on unmount', () => {
    const client = makeClient();
    const { result, unmount } = renderHook(() => useSyncStatus(client));

    unmount();

    // After unmount, emitting should not cause issues
    emitNativeEvent('onStatusChange', { status: 'error', retryAt: null });
    // No assertion needed — just verifying no crash
  });
});
