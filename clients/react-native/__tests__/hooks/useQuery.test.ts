import { renderHook, act, waitFor } from '@testing-library/react-native';
import { useQuery } from '../../src/hooks/useQuery';
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

beforeEach(() => {
  resetNativeModuleMockState();
});

describe('useQuery', () => {
  it('fetches data in one-shot mode (no tables)', async () => {
    const client = makeClient();
    jest.spyOn(client, 'query').mockResolvedValue([{ id: '1', name: 'test' }]);

    const { result } = renderHook(() => useQuery(client, 'SELECT * FROM items'));

    expect(result.current.loading).toBe(true);

    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.data).toEqual([{ id: '1', name: 'test' }]);
    expect(result.current.error).toBeNull();
  });

  it('handles query errors', async () => {
    const client = makeClient();
    jest.spyOn(client, 'query').mockRejectedValue(new Error('db locked'));

    const { result } = renderHook(() => useQuery(client, 'SELECT * FROM items'));

    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.error).toBeTruthy();
    expect(result.current.data).toEqual([]);
  });

  it('uses watch mode when tables provided', async () => {
    const client = makeClient();
    const unsubscribe = jest.fn();
    jest.spyOn(client, 'watch').mockImplementation((_sql, _params, _tables, callback) => {
      // Simulate initial result
      setTimeout(() => callback([{ id: '1' }]), 0);
      return unsubscribe;
    });

    const { result, unmount } = renderHook(() =>
      useQuery(client, 'SELECT * FROM items', undefined, ['items'])
    );

    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.data).toEqual([{ id: '1' }]);
    expect(client.watch).toHaveBeenCalled();

    unmount();
    expect(unsubscribe).toHaveBeenCalled();
  });

  it('provides a refresh function for one-shot mode', async () => {
    const client = makeClient();
    let callCount = 0;
    jest.spyOn(client, 'query').mockImplementation(async () => {
      callCount++;
      return [{ count: callCount }];
    });

    const { result } = renderHook(() => useQuery(client, 'SELECT count(*) FROM items'));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual([{ count: 1 }]);

    act(() => result.current.refresh());

    await waitFor(() => expect(result.current.data).toEqual([{ count: 2 }]));
  });

  it('re-subscribes when watch dependencies change', async () => {
    const client = makeClient();
    const unsubscribeA = jest.fn();
    const unsubscribeB = jest.fn();

    jest
      .spyOn(client, 'watch')
      .mockImplementationOnce((_sql, _params, _tables, callback) => {
        callback([{ id: '1' }]);
        return unsubscribeA;
      })
      .mockImplementationOnce((_sql, _params, _tables, callback) => {
        callback([{ id: '2' }]);
        return unsubscribeB;
      });

    const { result, rerender, unmount } = renderHook(
      ({ sql }) => useQuery(client, sql, undefined, ['items']),
      { initialProps: { sql: 'SELECT * FROM items' } }
    );

    await waitFor(() => expect(result.current.data).toEqual([{ id: '1' }]));

    rerender({ sql: 'SELECT * FROM items WHERE done = 0' });

    await waitFor(() => expect(result.current.data).toEqual([{ id: '2' }]));

    expect(unsubscribeA).toHaveBeenCalledTimes(1);

    unmount();
    expect(unsubscribeB).toHaveBeenCalledTimes(1);
  });
});
