import { SynchroClient } from '../src/SynchroClient';
import { SyncStatus } from '../src/types';
import {
  mockNativeModule,
  emitNativeEvent,
  resetNativeModuleMockState,
} from './__mocks__/react-native';

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

describe('SynchroClient', () => {
  describe('initialize', () => {
    it('calls native initialize with config', async () => {
      const client = makeClient();
      await client.initialize();

      expect(mockNativeModule.initialize).toHaveBeenCalledWith({
        dbPath: '/test.db',
        serverURL: 'http://localhost:8080',
        clientID: 'test-client',
        platform: 'ios',
        appVersion: '1.0.0',
        syncInterval: 30,
        pushDebounce: 0.5,
        maxRetryAttempts: 5,
        pullPageSize: 100,
        pushBatchSize: 100,
      });
    });
  });

  describe('query', () => {
    it('serializes params and deserializes rows', async () => {
      const rows = [{ id: '1', name: 'test' }];
      mockNativeModule.query.mockResolvedValueOnce(JSON.stringify(rows));

      const client = makeClient();
      const result = await client.query('SELECT * FROM items WHERE id = ?', ['1']);

      expect(mockNativeModule.query).toHaveBeenCalledWith(
        'SELECT * FROM items WHERE id = ?',
        '["1"]'
      );
      expect(result).toEqual(rows);
    });

    it('handles empty params', async () => {
      mockNativeModule.query.mockResolvedValueOnce('[]');

      const client = makeClient();
      await client.query('SELECT 1');

      expect(mockNativeModule.query).toHaveBeenCalledWith('SELECT 1', '[]');
    });
  });

  describe('queryOne', () => {
    it('returns null when native returns null', async () => {
      mockNativeModule.queryOne.mockResolvedValueOnce(null);

      const client = makeClient();
      const result = await client.queryOne('SELECT * FROM items WHERE id = ?', ['missing']);

      expect(result).toBeNull();
    });

    it('deserializes single row', async () => {
      const row = { id: '1', name: 'test' };
      mockNativeModule.queryOne.mockResolvedValueOnce(JSON.stringify(row));

      const client = makeClient();
      const result = await client.queryOne('SELECT * FROM items LIMIT 1');

      expect(result).toEqual(row);
    });
  });

  describe('execute', () => {
    it('returns rowsAffected', async () => {
      mockNativeModule.execute.mockResolvedValueOnce({ rowsAffected: 3 });

      const client = makeClient();
      const result = await client.execute('UPDATE items SET name = ?', ['new']);

      expect(result.rowsAffected).toBe(3);
    });
  });

  describe('executeBatch', () => {
    it('serializes statements array', async () => {
      mockNativeModule.executeBatch.mockResolvedValueOnce({ totalRowsAffected: 2 });

      const client = makeClient();
      const result = await client.executeBatch([
        { sql: 'INSERT INTO items (id) VALUES (?)', params: ['a'] },
        { sql: 'INSERT INTO items (id) VALUES (?)', params: ['b'] },
      ]);

      expect(result.totalRowsAffected).toBe(2);
      const call = mockNativeModule.executeBatch.mock.calls[0][0];
      const parsed = JSON.parse(call);
      expect(parsed).toHaveLength(2);
      expect(parsed[0].sql).toBe('INSERT INTO items (id) VALUES (?)');
    });
  });

  describe('writeTransaction', () => {
    it('begins, executes, and commits', async () => {
      mockNativeModule.txExecute.mockResolvedValueOnce({ rowsAffected: 1 });
      mockNativeModule.txQuery.mockResolvedValueOnce(JSON.stringify([{ count: 1 }]));

      const client = makeClient();
      const result = await client.writeTransaction(async (tx) => {
        await tx.execute('INSERT INTO items (id) VALUES (?)', ['1']);
        const rows = await tx.query('SELECT count(*) as count FROM items');
        return rows[0].count;
      });

      expect(mockNativeModule.beginWriteTransaction).toHaveBeenCalled();
      expect(mockNativeModule.txExecute).toHaveBeenCalledWith('tx-1', expect.any(String), expect.any(String));
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-1');
      expect(result).toBe(1);
    });

    it('rolls back on error', async () => {
      mockNativeModule.txExecute.mockRejectedValueOnce(
        new Error('constraint violation')
      );

      const client = makeClient();
      await expect(
        client.writeTransaction(async (tx) => {
          await tx.execute('INSERT INTO items (id) VALUES (?)', ['dup']);
        })
      ).rejects.toThrow();

      expect(mockNativeModule.rollbackTransaction).toHaveBeenCalledWith('tx-1');
    });
  });

  describe('readTransaction', () => {
    it('begins read, executes, and commits', async () => {
      mockNativeModule.txQuery.mockResolvedValueOnce(JSON.stringify([{ id: '1' }]));

      const client = makeClient();
      const result = await client.readTransaction(async (tx) => {
        return await tx.query('SELECT * FROM items');
      });

      expect(mockNativeModule.beginReadTransaction).toHaveBeenCalled();
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-1');
      expect(result).toEqual([{ id: '1' }]);
    });
  });

  describe('auth callback', () => {
    it('resolves auth requests from native', async () => {
      makeClient();

      // Simulate native requesting auth
      emitNativeEvent('onAuthRequest', { requestID: 'auth-1' });

      // Give the async handler time to run
      await new Promise((r) => setTimeout(r, 10));

      expect(mockNativeModule.resolveAuthRequest).toHaveBeenCalledWith(
        'auth-1',
        'test-token'
      );
    });

    it('rejects auth requests when provider throws', async () => {
      new SynchroClient({
        dbPath: '/test.db',
        serverURL: 'http://localhost:8080',
        authProvider: async () => {
          throw new Error('auth failed');
        },
        clientID: 'test-client',
        appVersion: '1.0.0',
      });

      emitNativeEvent('onAuthRequest', { requestID: 'auth-2' });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockNativeModule.rejectAuthRequest).toHaveBeenCalledWith(
        'auth-2',
        'auth failed'
      );
    });
  });

  describe('status listener multiplexing', () => {
    it('delivers status events to multiple subscribers independently', () => {
      const client = makeClient();
      const a: SyncStatus[] = [];
      const b: SyncStatus[] = [];

      const unsub1 = client.onStatusChange((s) => a.push(s));
      const unsub2 = client.onStatusChange((s) => b.push(s));

      emitNativeEvent('onStatusChange', { status: 'syncing', retryAt: null });

      expect(a).toHaveLength(1);
      expect(b).toHaveLength(1);
      expect(a[0].status).toBe('syncing');
      expect(b[0].status).toBe('syncing');

      unsub1();
      emitNativeEvent('onStatusChange', { status: 'idle', retryAt: null });

      expect(a).toHaveLength(1); // unsubscribed, no new event
      expect(b).toHaveLength(2);
      expect(b[1].status).toBe('idle');

      unsub2();
    });
  });

  describe('close', () => {
    it('calls native close', async () => {
      const client = makeClient();
      await client.close();
      expect(mockNativeModule.close).toHaveBeenCalled();
    });
  });

  describe('sync control', () => {
    it('start calls native start', async () => {
      const client = makeClient();
      await client.start();
      expect(mockNativeModule.start).toHaveBeenCalled();
    });

    it('does not require JS to call start twice when native startup retries', async () => {
      const client = makeClient();
      const statuses: SyncStatus[] = [];

      client.onStatusChange((status) => statuses.push(status));

      await client.start();

      emitNativeEvent('onStatusChange', {
        status: 'error',
        retryAt: '2026-01-01T00:00:00.000Z',
      });
      emitNativeEvent('onStatusChange', {
        status: 'idle',
        retryAt: null,
      });

      expect(mockNativeModule.start).toHaveBeenCalledTimes(1);
      expect(statuses.map((status) => status.status)).toEqual(['error', 'idle']);
    });

    it('stop calls native stop', async () => {
      const client = makeClient();
      await client.stop();
      expect(mockNativeModule.stop).toHaveBeenCalled();
    });

    it('syncNow calls native syncNow', async () => {
      const client = makeClient();
      await client.syncNow();
      expect(mockNativeModule.syncNow).toHaveBeenCalled();
    });
  });
});
