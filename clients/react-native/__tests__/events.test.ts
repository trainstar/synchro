import { SynchroClient } from '../src/SynchroClient';
import { emitNativeEvent, resetNativeModuleMockState } from './__mocks__/react-native';

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

describe('Event routing', () => {
  it('routes onStatusChange to subscriber', () => {
    const client = makeClient();
    const statuses: any[] = [];

    client.onStatusChange((s) => statuses.push(s));

    emitNativeEvent('onStatusChange', { status: 'syncing', retryAt: null });
    emitNativeEvent('onStatusChange', { status: 'idle', retryAt: null });

    expect(statuses).toHaveLength(2);
    expect(statuses[0].status).toBe('syncing');
    expect(statuses[1].status).toBe('idle');
  });

  it('parses retryAt as Date', () => {
    const client = makeClient();
    const statuses: any[] = [];

    client.onStatusChange((s) => statuses.push(s));

    const iso = '2026-01-01T00:00:00.000Z';
    emitNativeEvent('onStatusChange', { status: 'error', retryAt: iso });

    expect(statuses[0].retryAt).toBeInstanceOf(Date);
    expect(statuses[0].retryAt.toISOString()).toBe(iso);
  });

  it('routes onConflict with deserialized data', () => {
    const client = makeClient();
    const conflicts: any[] = [];

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
    const conflicts: any[] = [];

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
    const statuses: any[] = [];

    const unsub = client.onStatusChange((s) => statuses.push(s));

    emitNativeEvent('onStatusChange', { status: 'syncing', retryAt: null });
    unsub();
    emitNativeEvent('onStatusChange', { status: 'idle', retryAt: null });

    expect(statuses).toHaveLength(1);
  });

  it('multiple subscribers receive independent events', () => {
    const client = makeClient();
    const a: any[] = [];
    const b: any[] = [];

    client.onStatusChange((s) => a.push(s));
    client.onStatusChange((s) => b.push(s));

    emitNativeEvent('onStatusChange', { status: 'syncing', retryAt: null });

    expect(a).toHaveLength(1);
    expect(b).toHaveLength(1);
  });

  it('routes onChange events by observer ID', () => {
    const client = makeClient();
    const calls1: number[] = [];
    const calls2: number[] = [];

    const unsub1 = client.onChange(['items'], () => calls1.push(1));
    const unsub2 = client.onChange(['orders'], () => calls2.push(1));

    // Simulate native firing onChange for the first observer
    // The observer IDs are generated internally, so we extract them from mock calls
    const { mockNativeModule } = require('./__mocks__/react-native');
    const obs1ID = mockNativeModule.addChangeObserver.mock.calls[0]?.[0];
    const obs2ID = mockNativeModule.addChangeObserver.mock.calls[1]?.[0];

    emitNativeEvent('onChange', { observerID: obs1ID });
    emitNativeEvent('onChange', { observerID: obs2ID });
    emitNativeEvent('onChange', { observerID: obs1ID });

    expect(calls1).toHaveLength(2);
    expect(calls2).toHaveLength(1);

    unsub1();
    unsub2();
  });

  it('routes onQueryResult events by observer ID', () => {
    const client = makeClient();
    const results: any[] = [];

    const unsub = client.watch(
      'SELECT * FROM items',
      undefined,
      ['items'],
      (rows) => results.push(rows)
    );

    const { mockNativeModule } = require('./__mocks__/react-native');
    const obsID = mockNativeModule.addQueryObserver.mock.calls[0]?.[0];

    emitNativeEvent('onQueryResult', {
      observerID: obsID,
      rowsJson: '[{"id":"1","name":"test"}]',
    });

    // Event with different observer ID should not be routed
    emitNativeEvent('onQueryResult', {
      observerID: 'other-observer',
      rowsJson: '[{"id":"2"}]',
    });

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual([{ id: '1', name: 'test' }]);

    unsub();
  });

  it('unsubscribing onChange cleans up native observer', () => {
    const client = makeClient();
    const { mockNativeModule } = require('./__mocks__/react-native');

    const unsub = client.onChange(['items'], () => {});

    expect(mockNativeModule.addChangeObserver).toHaveBeenCalled();

    unsub();

    expect(mockNativeModule.removeObserver).toHaveBeenCalled();
  });
});
