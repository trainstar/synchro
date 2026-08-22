import { SynchroClient } from '../src/SynchroClient';
import {
  SYNC_STATUS_TYPES,
  SyncEvent,
  SyncStatus,
  ConflictEvent,
} from '../src/types';
import { emitNativeEvent, mockNativeModule, resetNativeModuleMockState } from './__mocks__/react-native';

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

function detailFreeStatus(status: string) {
  return { status, retryAt: null, operation: null, failure: null };
}

const failure = {
  operation: 'connecting',
  code: 'network_error',
  retryable: true,
  message: 'temporary network failure',
  recoveryAction: 'retry',
  metadata: { source: 'native' },
};

describe('Event routing', () => {
  it('routes onStatusChange to subscriber', () => {
    const client = makeClient();
    const statuses: SyncStatus[] = [];

    client.onStatusChange((s) => statuses.push(s));

    emitNativeEvent('onStatusChange', detailFreeStatus('connecting'));
    emitNativeEvent('onStatusChange', detailFreeStatus('ready'));

    expect(statuses).toHaveLength(2);
    expect(statuses[0].status).toBe('connecting');
    expect(statuses[1].status).toBe('ready');
  });

  it('parses backoff details as typed values', () => {
    const client = makeClient();
    const statuses: SyncStatus[] = [];

    client.onStatusChange((s) => statuses.push(s));

    const iso = '2026-01-01T00:00:00.000Z';
    emitNativeEvent('onStatusChange', {
      status: 'backoff',
      retryAt: iso,
      operation: 'pulling',
      failure: null,
    });

    expect(statuses[0].retryAt).toBeInstanceOf(Date);
    expect(statuses[0].retryAt!.toISOString()).toBe(iso);
    expect(statuses[0].operation).toBe('pulling');
    expect(statuses[0].failure).toBeNull();
  });

  it('preserves typed error details', () => {
    const client = makeClient();
    const statuses: SyncStatus[] = [];

    client.onStatusChange((s) => statuses.push(s));
    emitNativeEvent('onStatusChange', {
      status: 'error',
      retryAt: null,
      operation: null,
      failure,
    });

    expect(statuses[0]).toEqual({ status: 'error', retryAt: null, operation: null, failure });
  });

  it('rejects invalid state detail combinations', () => {
    const client = makeClient();
    client.onStatusChange(() => {});

    expect(() => emitNativeEvent('onStatusChange', detailFreeStatus('backoff'))).toThrow(
      'invalid backoff status details'
    );
    expect(() =>
      emitNativeEvent('onStatusChange', {
        status: 'ready',
        retryAt: null,
        operation: null,
        failure,
      })
    ).toThrow('invalid state detail fields');
  });

  it('routes onConflict with deserialized data', () => {
    const client = makeClient();
    const conflicts: ConflictEvent[] = [];

    client.onConflict((e) => conflicts.push(e));

    emitNativeEvent('onConflict', {
      table: 'items',
      recordID: 'r1',
      clientDataJson: '{"name":"client"}',
      serverDataJson: '{"name":"server"}',
    });

    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].table).toBe('items');
    expect(conflicts[0].clientData).toEqual({ name: 'client' });
    expect(conflicts[0].serverData).toEqual({ name: 'server' });
  });

  it('handles null conflict data', () => {
    const client = makeClient();
    const conflicts: ConflictEvent[] = [];

    client.onConflict((e) => conflicts.push(e));

    emitNativeEvent('onConflict', {
      table: 'items',
      recordID: 'r2',
      clientDataJson: null,
      serverDataJson: null,
    });

    expect(conflicts[0].clientData).toBeNull();
    expect(conflicts[0].serverData).toBeNull();
  });

  it('unsubscribe stops delivery', () => {
    const client = makeClient();
    const statuses: SyncStatus[] = [];

    const unsub = client.onStatusChange((s) => statuses.push(s));

    emitNativeEvent('onStatusChange', detailFreeStatus('connecting'));
    unsub();
    emitNativeEvent('onStatusChange', detailFreeStatus('ready'));

    expect(statuses).toHaveLength(1);
  });

  it('multiple subscribers receive independent events', () => {
    const client = makeClient();
    const a: SyncStatus[] = [];
    const b: SyncStatus[] = [];

    client.onStatusChange((s) => a.push(s));
    client.onStatusChange((s) => b.push(s));

    emitNativeEvent('onStatusChange', detailFreeStatus('connecting'));

    expect(a).toHaveLength(1);
    expect(b).toHaveLength(1);
  });

  it('routes every typed native sync event without deriving event meaning in JavaScript', () => {
    const client = makeClient();
    const events: SyncEvent[] = [];
    client.onSyncEvent((event) => events.push(event));

    const base = {
      from: null,
      to: null,
      operation: null,
      attempt: null,
      retryAt: null,
      source: null,
      target: null,
      action: null,
      mutationID: null,
      tableID: null,
      mutationStatus: null,
      rejectionCode: null,
      scopeID: null,
      rebuildID: null,
      failure: null,
    };
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'state_changed',
      from: 'connecting',
      to: 'ready',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'backoff',
      operation: 'pulling',
      attempt: 2,
      retryAt: '2026-01-01T00:00:00.000Z',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'schema_applying',
      source: { version: 1, hash: 'a'.repeat(64) },
      target: { version: 2, hash: 'b'.repeat(64) },
      action: 'replace',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'schema_applied',
      source: { version: 1, hash: 'a'.repeat(64) },
      target: { version: 2, hash: 'b'.repeat(64) },
      action: 'rebuild_local',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'mutation_accepted',
      mutationID: 'mutation-1',
      tableID: 'table-1',
      mutationStatus: 'applied',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'mutation_rejected',
      mutationID: 'mutation-2',
      tableID: 'table-2',
      mutationStatus: 'conflict',
      rejectionCode: 'version_conflict',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'rebuild_requested',
      scopeID: 'scope-1',
      rebuildID: 'rebuild-1',
    });
    emitNativeEvent('onSyncEvent', {
      ...base,
      type: 'rebuild_completed',
      scopeID: 'scope-1',
      rebuildID: 'rebuild-1',
    });
    emitNativeEvent('onSyncEvent', { ...base, type: 'failure', failure });

    expect(events.map((event) => event.type)).toEqual([
      'state_changed',
      'backoff',
      'schema_applying',
      'schema_applied',
      'mutation_accepted',
      'mutation_rejected',
      'rebuild_requested',
      'rebuild_completed',
      'failure',
    ]);
    expect(events[1]).toEqual({
      type: 'backoff',
      operation: 'pulling',
      attempt: 2,
      retryAt: new Date('2026-01-01T00:00:00.000Z'),
    });
    expect(events[4]).toMatchObject({
      type: 'mutation_accepted',
      mutationID: 'mutation-1',
      tableID: 'table-1',
    });
    expect(events[6]).toEqual({
      type: 'rebuild_requested',
      scopeID: 'scope-1',
      rebuildID: 'rebuild-1',
    });
    expect(events[8]).toEqual({ type: 'failure', failure });
  });

  it('rejects a known but illegal native lifecycle transition', () => {
    const client = makeClient();
    client.onSyncEvent(() => {});

    expect(() =>
      emitNativeEvent('onSyncEvent', {
        type: 'state_changed',
        from: 'stopped',
        to: 'pulling',
      })
    ).toThrow('invalid lifecycle event');
  });

  it('accepts exactly the normative lifecycle adjacency map', () => {
    const client = makeClient();
    client.onSyncEvent(() => {});
    const allowed: Record<string, readonly string[]> = {
      uninitialized: ['local_ready', 'error', 'stopped'],
      local_ready: ['connecting', 'error', 'stopped'],
      connecting: ['schema_applying', 'ready', 'backoff', 'error', 'stopped'],
      schema_applying: ['ready', 'rebuilding', 'error', 'stopped'],
      ready: ['connecting', 'pushing', 'pulling', 'rebuilding', 'error', 'stopped'],
      pushing: ['pushing', 'ready', 'pulling', 'connecting', 'backoff', 'error', 'stopped'],
      pulling: ['pulling', 'ready', 'rebuilding', 'connecting', 'backoff', 'error', 'stopped'],
      rebuilding: ['rebuilding', 'ready', 'connecting', 'backoff', 'error', 'stopped'],
      backoff: ['connecting', 'pushing', 'pulling', 'rebuilding', 'error', 'stopped'],
      error: ['local_ready', 'stopped'],
      stopped: ['local_ready'],
    };

    for (const from of SYNC_STATUS_TYPES) {
      for (const to of SYNC_STATUS_TYPES) {
        const emit = () => emitNativeEvent('onSyncEvent', {
          type: 'state_changed',
          from,
          to,
        });
        if (allowed[from].includes(to)) {
          expect(emit).not.toThrow();
        } else {
          expect(emit).toThrow('invalid lifecycle event');
        }
      }
    }
  });

  it('rejects an omitted or unknown native status value', () => {
    const client = makeClient();
    const statuses: SyncStatus[] = [];
    client.onStatusChange((status) => statuses.push(status));

    expect(() => emitNativeEvent('onStatusChange', { status: 'idle' })).toThrow(
      'invalid sync status'
    );
    expect(statuses).toHaveLength(0);
  });

  it('routes onChange events by observer ID', async () => {
    const client = makeClient();
    const calls1: number[] = [];
    const calls2: number[] = [];

    const unsub1 = await client.onChange(['items'], () => calls1.push(1));
    const unsub2 = await client.onChange(['orders'], () => calls2.push(1));

    // Simulate native firing onChange for the first observer
    // The observer IDs are generated internally, so we extract them from mock calls
    const obs1ID = mockNativeModule.addChangeObserver.mock.calls[0]?.[0];
    const obs2ID = mockNativeModule.addChangeObserver.mock.calls[1]?.[0];

    emitNativeEvent('onChange', { observerID: obs1ID });
    emitNativeEvent('onChange', { observerID: obs2ID });
    emitNativeEvent('onChange', { observerID: obs1ID });

    expect(calls1).toHaveLength(2);
    expect(calls2).toHaveLength(1);

    await unsub1();
    await unsub2();
  });

  it('routes onQueryResult events by observer ID', async () => {
    const client = makeClient();
    const results: unknown[] = [];

    const unsub = await client.watch(
      'SELECT * FROM items WHERE deleted_at IS ?',
      [null],
      ['items'],
      (rows) => results.push(rows)
    );

    const obsID = mockNativeModule.addQueryObserver.mock.calls[0]?.[0];
    expect(mockNativeModule.addQueryObserver).toHaveBeenCalledWith(
      obsID,
      'SELECT * FROM items WHERE deleted_at IS ?',
      [null],
      ['items']
    );

    emitNativeEvent('onQueryResult', {
      observerID: obsID,
      rows: [{ id: '1', name: 'test' }],
    });

    // Event with different observer ID should not be routed
    emitNativeEvent('onQueryResult', {
      observerID: 'other-observer',
      rows: [{ id: '2' }],
    });

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual([{ id: '1', name: 'test' }]);

    await unsub();
  });

  it('unsubscribing onChange cleans up native observer', async () => {
    const client = makeClient();

    const unsub = await client.onChange(['items'], () => {});

    expect(mockNativeModule.addChangeObserver).toHaveBeenCalled();

    await unsub();

    expect(mockNativeModule.removeObserver).toHaveBeenCalled();
  });

  it('propagates native observer registration failures', async () => {
    mockNativeModule.addChangeObserver.mockRejectedValueOnce({
      code: 'NOT_CONNECTED',
      message: 'not initialized',
    });
    mockNativeModule.addQueryObserver.mockRejectedValueOnce({
      code: 'NOT_CONNECTED',
      message: 'not initialized',
    });
    const client = makeClient();

    await expect(client.onChange(['items'], () => {})).rejects.toMatchObject({
      code: 'NOT_CONNECTED',
    });
    await expect(
      client.watch('SELECT * FROM items', [], ['items'], () => {})
    ).rejects.toMatchObject({
      code: 'NOT_CONNECTED',
    });
  });
});
