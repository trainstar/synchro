import { renderHook, act, waitFor } from '@testing-library/react-native';
import { useQuery } from '../../src/hooks/useQuery';
import { SynchroClient } from '../../src/SynchroClient';
import { resetNativeModuleMockState } from '../__mocks__/react-native';
import type { SQLiteBindValue } from '../../src/types';

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
  it.each([false, true])('stabilizes tags and detects changed payloads in watch mode %s', async (reactive) => {
    const client = makeClient();
    const remove = jest.fn().mockResolvedValue(undefined);
    const query = jest.spyOn(client, 'query').mockResolvedValue([]);
    const watch = jest.spyOn(client, 'watch').mockImplementation(async (_sql, _params, _tables, callback) => {
      callback([]);
      return remove;
    });
    const initial: SQLiteBindValue[] = [
      { type: 'int64', value: '9223372036854775807' },
      { type: 'bytes', base64: 'AQ' },
      null, 'label', 1, false,
    ];
    const { result, rerender, unmount } = renderHook(
      ({ params }: { params: SQLiteBindValue[] }) =>
        useQuery(client, 'SELECT ?, ?, ?, ?, ?, ?', params, reactive ? ['items'] : undefined),
      { initialProps: { params: initial } }
    );
    const operation = reactive ? watch : query;
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(operation).toHaveBeenCalledTimes(1);

    const equivalent: SQLiteBindValue[] = [
      { value: '9223372036854775807', type: 'int64' },
      { base64: 'AQ', type: 'bytes' },
      null, 'label', 1, false,
    ];
    await act(async () => rerender({ params: equivalent }));
    expect(operation).toHaveBeenCalledTimes(1);
    expect(remove).not.toHaveBeenCalled();

    const changedInteger: SQLiteBindValue[] = [
      { type: 'int64', value: '9223372036854775806' },
      ...equivalent.slice(1),
    ];
    await act(async () => rerender({ params: changedInteger }));
    expect(operation).toHaveBeenCalledTimes(2);

    const changedBytes: SQLiteBindValue[] = [
      changedInteger[0], { type: 'bytes', base64: '1000' }, ...equivalent.slice(2),
    ];
    await act(async () => rerender({ params: changedBytes }));
    expect(operation).toHaveBeenCalledTimes(3);

    const changedType: SQLiteBindValue[] = [
      changedInteger[0], { type: 'int64', value: '1000' }, ...equivalent.slice(2),
    ];
    await act(async () => rerender({ params: changedType }));
    expect(operation).toHaveBeenCalledTimes(4);

    await act(async () => result.current.refresh());
    expect(operation).toHaveBeenCalledTimes(5);
    unmount();
    expect(remove).toHaveBeenCalledTimes(reactive ? 5 : 0);
  });

  it('does not hide a malformed tag behind an equivalent valid parameter', async () => {
    const client = makeClient();
    const { result, rerender } = renderHook(
      ({ params }: { params: SQLiteBindValue[] }) => useQuery(client, 'SELECT ?', params),
      { initialProps: { params: [{ type: 'bytes', base64: 'AQ' }] } }
    );
    await waitFor(() => expect(result.current.loading).toBe(false));
    const invalid = { type: 'bytes' as const, base64: 'AQ', extra: true };
    rerender({ params: [invalid] });
    await waitFor(() => expect(result.current.error).toBeInstanceOf(Error));

    rerender({ params: [{ type: 'bytes', base64: 'AQ' }] });
    await waitFor(() => expect(result.current.error).toBeNull());
    const inheritedType = { type: 'bytes' as const, base64: 'AQ', extra: true };
    Object.setPrototypeOf(inheritedType, { type: 'bytes' });
    Reflect.deleteProperty(inheritedType, 'type');
    rerender({ params: [inheritedType] });
    await waitFor(() => expect(result.current.error).toBeInstanceOf(Error));
  });

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
    jest.spyOn(client, 'watch').mockImplementation(async (_sql, _params, _tables, callback) => {
      // Simulate initial result
      setTimeout(() => callback([{ id: '1' }]), 0);
      return async () => {
        unsubscribe();
      };
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
      .mockImplementationOnce(async (_sql, _params, _tables, callback) => {
        callback([{ id: '1' }]);
        return async () => {
          unsubscribeA();
        };
      })
      .mockImplementationOnce(async (_sql, _params, _tables, callback) => {
        callback([{ id: '2' }]);
        return async () => {
          unsubscribeB();
        };
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
