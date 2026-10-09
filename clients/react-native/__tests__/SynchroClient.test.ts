import { SynchroClient } from '../src/SynchroClient';
import { SynchroInspection } from '../src/inspection';
import { SyncStatus } from '../src/types';
import { AtomicGroupInvalidError } from '../src/errors';
import {
  mockNativeModule,
  emitNativeEvent,
  resetNativeModuleMockState,
} from './__mocks__/react-native';

const CLIENT_STATE_COUNTS = {
  application_row_count: Number.MAX_SAFE_INTEGER,
  mutation_ledger_count: 2,
  mutation_outcome_count: 3,
  sealed_batch_count: 4,
  rejected_mutation_count: 5,
  scope_state_count: 6,
  scope_row_count: 7,
  provenance_count: 8,
  row_metadata_count: 9,
  rebuild_attempt_count: 10,
  rebuild_receipt_count: 11,
};

function snapshotResult(clientState: Record<string, unknown>, details: Record<string, unknown> = {}) {
  return {
    inspection: JSON.stringify({
      client_state: { capture_overflowed: false, ...clientState },
      retained_mutations: [],
      rejected_mutations: [],
      migration_journal: null,
      migration_journal_truncated: false,
      physical_schema: [],
      physical_schema_truncated: false,
      accepted_mutation_outcomes: {},
      accepted_mutation_outcomes_truncated: false,
      ...details,
    }),
    applicationRows: [],
  };
}

function makeClient(): SynchroClient {
  return new SynchroClient({
    dbPath: '/test.db',
    serverURL: 'http://localhost:8080',
    authProvider: async () => 'test-token',
    clientID: 'test-client',
    appVersion: '1.0.0',
  });
}

async function makeInspection(): Promise<{ client: SynchroClient; inspection: SynchroInspection }> {
  const client = makeClient();
  const inspection = new SynchroInspection(client, { transportObservationCapacity: 8 });
  await client.initialize();
  return { client, inspection };
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
        seedDatabasePath: undefined,
        transportObservationCapacity: 0,
        requireNewDatabase: false,
      });
      await client.close();
    });
  });

  describe('query', () => {
    it('passes typed params and returns rows', async () => {
      const rows = [{ id: '1', name: 'test' }];
      mockNativeModule.query.mockResolvedValueOnce(rows);

      const client = makeClient();
      const result = await client.query('SELECT * FROM items WHERE id = ?', ['1']);

      expect(mockNativeModule.query).toHaveBeenCalledWith(
        'SELECT * FROM items WHERE id = ?',
        ['1']
      );
      expect(result).toEqual(rows);
    });

    it('handles empty params', async () => {
      mockNativeModule.query.mockResolvedValueOnce([]);

      const client = makeClient();
      await client.query('SELECT 1');

      expect(mockNativeModule.query).toHaveBeenCalledWith('SELECT 1', []);
    });

    it('passes null bind params without removing positional slots', async () => {
      mockNativeModule.query.mockResolvedValueOnce([]);

      const client = makeClient();
      await client.query('SELECT * FROM items WHERE deleted_at IS ? AND name = ?', [null, 'x']);

      expect(mockNativeModule.query).toHaveBeenCalledWith(
        'SELECT * FROM items WHERE deleted_at IS ? AND name = ?',
        [null, 'x']
      );
    });
  });

  describe('queryOne', () => {
    it('returns null when native returns null', async () => {
      mockNativeModule.queryOne.mockResolvedValueOnce(null);

      const client = makeClient();
      const result = await client.queryOne('SELECT * FROM items WHERE id = ?', ['missing']);

      expect(result).toBeNull();
    });

    it('returns null when iOS native returns undefined for no row', async () => {
      mockNativeModule.queryOne.mockResolvedValueOnce(undefined);

      const client = makeClient();
      const result = await client.queryOne('SELECT * FROM items WHERE id = ?', ['missing']);

      expect(result).toBeNull();
    });

    it('deserializes single row', async () => {
      const row = { id: '1', name: 'test' };
      mockNativeModule.queryOne.mockResolvedValueOnce(row);

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

    it('passes null bind params to execute without removing positional slots', async () => {
      mockNativeModule.execute.mockResolvedValueOnce({ rowsAffected: 1 });

      const client = makeClient();
      await client.execute(
        'INSERT INTO items (id, deleted_at, name) VALUES (?, ?, ?)',
        ['1', null, 'x']
      );

      expect(mockNativeModule.execute).toHaveBeenCalledWith(
        'INSERT INTO items (id, deleted_at, name) VALUES (?, ?, ?)',
        ['1', null, 'x']
      );
    });
  });

  describe('executeAuthoredWrite', () => {
    it('passes authored context and returns rowsAffected', async () => {
      mockNativeModule.executeAuthoredWrite.mockResolvedValueOnce({ rowsAffected: 3 });

      const client = makeClient();
      const result = await client.executeAuthoredWrite(
        'items',
        'insert',
        ['value'],
        'INSERT INTO items (id, value) VALUES (?, ?)',
        ['item-1', 'new']
      );

      expect(result.rowsAffected).toBe(3);
      expect(mockNativeModule.executeAuthoredWrite).toHaveBeenCalledWith(
        'items',
        'insert',
        ['value'],
        'INSERT INTO items (id, value) VALUES (?, ?)',
        ['item-1', 'new']
      );
    });

    it('passes null bind values without removing positional slots', async () => {
      mockNativeModule.executeAuthoredWrite.mockResolvedValueOnce({ rowsAffected: 1 });

      const client = makeClient();
      await client.executeAuthoredWrite(
        'items',
        'update',
        ['deleted_at', 'value'],
        'UPDATE items SET deleted_at = ?, value = ? WHERE id = ?',
        [null, 'new', 'item-1']
      );

      expect(mockNativeModule.executeAuthoredWrite).toHaveBeenCalledWith(
        'items',
        'update',
        ['deleted_at', 'value'],
        'UPDATE items SET deleted_at = ?, value = ? WHERE id = ?',
        [null, 'new', 'item-1']
      );
    });
  });

  describe('executeBatch', () => {
    it('passes statements array with typed params', async () => {
      mockNativeModule.executeBatch.mockResolvedValueOnce({ totalRowsAffected: 2 });

      const client = makeClient();
      const result = await client.executeBatch([
        { sql: 'INSERT INTO items (id, deleted_at) VALUES (?, ?)', params: ['a', null] },
        { sql: 'INSERT INTO items (id, deleted_at) VALUES (?, ?)', params: ['b', null] },
      ]);

      expect(result.totalRowsAffected).toBe(2);
      expect(mockNativeModule.executeBatch).toHaveBeenCalledWith([
        { sql: 'INSERT INTO items (id, deleted_at) VALUES (?, ?)', params: ['a', null] },
        { sql: 'INSERT INTO items (id, deleted_at) VALUES (?, ?)', params: ['b', null] },
      ]);
    });
  });

  describe('writeTransaction', () => {
    it('begins, executes, and commits', async () => {
      mockNativeModule.txExecute.mockResolvedValueOnce({ rowsAffected: 1 });
      mockNativeModule.txQuery.mockResolvedValueOnce([{ count: 1 }]);

      const client = makeClient();
      const result = await client.writeTransaction(async (tx) => {
        await tx.execute('INSERT INTO items (id) VALUES (?)', ['1']);
        const rows = await tx.query('SELECT count(*) as count FROM items');
        return rows[0].count;
      });

      expect(mockNativeModule.beginWriteTransaction).toHaveBeenCalled();
      expect(mockNativeModule.txExecute).toHaveBeenCalledWith('tx-1', expect.any(String), expect.any(Array));
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-1');
      expect(result).toBe(1);
    });

    it('normalizes transaction queryOne missing rows to null', async () => {
      mockNativeModule.txQueryOne.mockResolvedValueOnce(undefined);

      const client = makeClient();
      const result = await client.writeTransaction(async (tx) => {
        return await tx.queryOne('SELECT * FROM items WHERE id = ?', ['missing']);
      });

      expect(mockNativeModule.txQueryOne).toHaveBeenCalledWith(
        'tx-1',
        'SELECT * FROM items WHERE id = ?',
        ['missing']
      );
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-1');
      expect(result).toBeNull();
    });

    it('rolls back on error', async () => {
      mockNativeModule.txExecute.mockRejectedValueOnce(
        new Error('constraint violation')
      );

      const client = makeClient();
      await expect(
        client.writeTransaction(async (tx) => {
          await tx.execute('INSERT INTO items (id, deleted_at) VALUES (?, ?)', ['dup', null]);
        })
      ).rejects.toThrow();

      expect(mockNativeModule.txExecute).toHaveBeenCalledWith(
        'tx-1',
        'INSERT INTO items (id, deleted_at) VALUES (?, ?)',
        ['dup', null]
      );
      expect(mockNativeModule.rollbackTransaction).toHaveBeenCalledWith('tx-1');
    });
  });

  describe('atomicWriteTransaction', () => {
    it('begins an atomic native transaction, runs statements in it, and commits', async () => {
      const client = makeClient();
      const result = await client.atomicWriteTransaction(async (tx) => {
        await tx.execute('UPDATE items SET name = ? WHERE id = ?', ['a', '1']);
        await tx.execute('INSERT INTO items (id, name) VALUES (?, ?)', ['2', 'b']);
        return 'done';
      });

      expect(mockNativeModule.beginAtomicWriteTransaction).toHaveBeenCalledTimes(1);
      expect(mockNativeModule.beginWriteTransaction).not.toHaveBeenCalled();
      expect(mockNativeModule.txExecute.mock.calls).toEqual([
        ['tx-atomic-1', 'UPDATE items SET name = ? WHERE id = ?', ['a', '1']],
        ['tx-atomic-1', 'INSERT INTO items (id, name) VALUES (?, ?)', ['2', 'b']],
      ]);
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-atomic-1');
      expect(mockNativeModule.rollbackTransaction).not.toHaveBeenCalled();
      expect(result).toBe('done');
    });

    it('rolls back the atomic transaction when the callback throws', async () => {
      const failure = new Error('application failure');

      const client = makeClient();
      await expect(
        client.atomicWriteTransaction(async (tx) => {
          await tx.execute('DELETE FROM items WHERE id = ?', ['1']);
          throw failure;
        })
      ).rejects.toMatchObject({ code: 'UNKNOWN' });

      expect(mockNativeModule.beginAtomicWriteTransaction).toHaveBeenCalledTimes(1);
      expect(mockNativeModule.commitTransaction).not.toHaveBeenCalled();
      expect(mockNativeModule.rollbackTransaction).toHaveBeenCalledWith('tx-atomic-1');
    });

    it('rejects with the typed invalid-group error when the native commit rejects the group', async () => {
      mockNativeModule.commitTransaction.mockRejectedValueOnce({
        code: 'atomic_group_invalid',
        message: 'native text',
        userInfo: { reason: 'deleteFollowedByWrite' },
      });

      const client = makeClient();
      const rejection = client.atomicWriteTransaction(async (tx) => {
        await tx.execute('DELETE FROM items WHERE id = ?', ['1']);
        await tx.execute('INSERT INTO items (id, name) VALUES (?, ?)', ['1', 'again']);
      });

      await expect(rejection).rejects.toBeInstanceOf(AtomicGroupInvalidError);
      await expect(rejection).rejects.toMatchObject({
        code: 'atomic_group_invalid',
        reason: 'deleteFollowedByWrite',
      });
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-atomic-1');
      expect(mockNativeModule.rollbackTransaction).toHaveBeenCalledWith('tx-atomic-1');
    });
  });

  describe('readTransaction', () => {
    it('exposes queries only and commits', async () => {
      mockNativeModule.txQuery.mockResolvedValueOnce([{ id: '1' }]);

      const client = makeClient();
      const result = await client.readTransaction(async (tx) => {
        expect(tx).not.toHaveProperty('execute');
        return await tx.query('SELECT * FROM items WHERE deleted_at IS ?', [null]);
      });

      expect(mockNativeModule.beginReadTransaction).toHaveBeenCalled();
      expect(mockNativeModule.txQuery).toHaveBeenCalledWith(
        'tx-1',
        'SELECT * FROM items WHERE deleted_at IS ?',
        [null]
      );
      expect(mockNativeModule.commitTransaction).toHaveBeenCalledWith('tx-1');
      expect(mockNativeModule.txExecute).not.toHaveBeenCalled();
      expect(result).toEqual([{ id: '1' }]);
    });
  });

  describe('auth callback', () => {
    it('resolves auth requests from native', async () => {
      const client = makeClient();
      await client.initialize();

      // Simulate native requesting auth
      emitNativeEvent('onAuthRequest', { requestID: 'auth-1' });

      // Give the async handler time to run
      await new Promise((r) => setTimeout(r, 10));

      expect(mockNativeModule.resolveAuthRequest).toHaveBeenCalledWith(
        'auth-1',
        'test-token'
      );
      await client.close();
    });

    it('rejects auth requests when provider throws', async () => {
      const client = new SynchroClient({
        dbPath: '/test.db',
        serverURL: 'http://localhost:8080',
        authProvider: async () => {
          throw new Error('auth failed');
        },
        clientID: 'test-client',
        appVersion: '1.0.0',
      });
      await client.initialize();

      emitNativeEvent('onAuthRequest', { requestID: 'auth-2' });
      await new Promise((r) => setTimeout(r, 10));

      expect(mockNativeModule.rejectAuthRequest).toHaveBeenCalledWith(
        'auth-2',
        'auth failed'
      );
      await client.close();
    });

    it('prevents stale clients from answering a replacement client auth request', async () => {
      const first = makeClient();
      const second = new SynchroClient({
        dbPath: '/second.db',
        serverURL: 'http://localhost:8080',
        authProvider: async () => 'second-token',
        clientID: 'second-client',
        appVersion: '1.0.0',
      });

      await first.initialize();
      await expect(second.initialize()).rejects.toMatchObject({
        code: 'CLIENT_ALREADY_ACTIVE',
      });

      emitNativeEvent('onAuthRequest', { requestID: 'auth-first' });
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(mockNativeModule.resolveAuthRequest).toHaveBeenLastCalledWith(
        'auth-first',
        'test-token'
      );

      await first.close();
      mockNativeModule.resolveAuthRequest.mockClear();
      await second.initialize();
      emitNativeEvent('onAuthRequest', { requestID: 'auth-second' });
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(mockNativeModule.resolveAuthRequest).toHaveBeenCalledTimes(1);
      expect(mockNativeModule.resolveAuthRequest).toHaveBeenCalledWith(
        'auth-second',
        'second-token'
      );
      await second.close();
    });
  });

  describe('status listener multiplexing', () => {
    it('delivers status events to multiple subscribers independently', () => {
      const client = makeClient();
      const a: SyncStatus[] = [];
      const b: SyncStatus[] = [];

      const unsub1 = client.onStatusChange((s) => a.push(s));
      const unsub2 = client.onStatusChange((s) => b.push(s));

      emitNativeEvent('onStatusChange', {
        status: 'connecting',
        retryAt: null,
        operation: null,
        failure: null,
      });

      expect(a).toHaveLength(1);
      expect(b).toHaveLength(1);
      expect(a[0].status).toBe('connecting');
      expect(b[0].status).toBe('connecting');

      unsub1();
      emitNativeEvent('onStatusChange', {
        status: 'ready',
        retryAt: null,
        operation: null,
        failure: null,
      });

      expect(a).toHaveLength(1); // unsubscribed, no new event
      expect(b).toHaveLength(2);
      expect(b[1].status).toBe('ready');

      unsub2();
    });
  });

  describe('close', () => {
    it('calls native close', async () => {
      const client = makeClient();
      await client.initialize();
      await client.close();
      expect(mockNativeModule.close).toHaveBeenCalled();
    });
  });

  describe('sync control', () => {
    it('does not require JS to call start twice when native startup retries', async () => {
      const client = makeClient();
      const statuses: SyncStatus[] = [];

      client.onStatusChange((status) => statuses.push(status));

      await client.start();

      emitNativeEvent('onStatusChange', {
        status: 'error',
        retryAt: null,
        operation: null,
        failure: {
          operation: 'connecting',
          code: 'network_error',
          retryable: true,
          message: 'temporary network failure',
          recoveryAction: 'retry',
          metadata: {},
        },
      });
      emitNativeEvent('onStatusChange', {
        status: 'ready',
        retryAt: null,
        operation: null,
        failure: null,
      });

      expect(mockNativeModule.start).toHaveBeenCalledTimes(1);
      expect(statuses.map((status) => status.status)).toEqual(['error', 'ready']);
    });

    it('stop calls native stop', async () => {
      const client = makeClient();
      await client.stop();
      expect(mockNativeModule.stop).toHaveBeenCalled();
    });

    it('does not resolve stop before native drain completes', async () => {
      let resolveStop: (() => void) | undefined;
      mockNativeModule.stop.mockImplementationOnce(
        () => new Promise<void>((resolve) => {
          resolveStop = resolve;
        })
      );
      const client = makeClient();
      let settled = false;
      const stopPromise = client.stop().then(() => {
        settled = true;
      });

      await Promise.resolve();
      expect(settled).toBe(false);
      resolveStop!();
      await stopPromise;
      expect(settled).toBe(true);
    });

    it.each([
      ['enterBackground', () => makeClient().enterBackground()],
      ['enterForeground', () => makeClient().enterForeground()],
      ['retryAfterError', () => makeClient().retryAfterError()],
      ['resetSchemaAndStart', () => makeClient().resetSchemaAndStart()],
    ])('forwards %s as a thin native lifecycle call', async (method, invoke) => {
      await invoke();
      expect(mockNativeModule[method]).toHaveBeenCalledTimes(1);
    });

  });

  describe('status and mutation inspection', () => {
      it('maps the native status JSON', async () => {
        mockNativeModule.getSyncStatus.mockResolvedValueOnce(
        '{"status":"error","retryAt":null,"operation":null,"failure":{"operation":"connecting","code":"network_error","retryable":true,"message":"temporary network failure","recoveryAction":"retry","metadata":{"source":"native"}}}'
      );

      const status = await makeClient().getSyncStatus();

      expect(mockNativeModule.getSyncStatus).toHaveBeenCalledTimes(1);
      expect(status).toEqual({
        status: 'error',
        retryAt: null,
        operation: null,
        failure: {
          operation: 'connecting',
          code: 'network_error',
          retryable: true,
          message: 'temporary network failure',
          recoveryAction: 'retry',
          metadata: { source: 'native' },
        },
      });
    });

    it('maps pending mutation inspection JSON', async () => {
      const pending = {
        mutationID: 'mutation-1',
        localOrder: 7,
        tableID: 'table-1',
        tableName: 'items',
        recordID: 'record-1',
        primaryKeyFieldID: 'field-id',
        primaryKeyLogicalType: 'uuid',
        operation: 'update',
        authoredSchema: { version: 3, hash: 'a'.repeat(64) },
        baseVersion: 'server-v1',
        clientVersion: 'client-v2',
        status: 'sealed',
        sourceKind: 'local_write',
        dependsOnMutationID: null,
        normalizedMutationID: 'mutation-0',
        sealedBatchID: 'batch-1',
        sealedOrdinal: 2,
        authoredFields: [
          { fieldID: 'field-name', logicalType: 'string', value: 'updated' },
        ],
      };
      mockNativeModule.inspectPendingMutations.mockResolvedValueOnce(
        JSON.stringify([pending])
      );

      await expect(makeClient().inspectPendingMutations()).resolves.toEqual([
        pending,
      ]);
      expect(mockNativeModule.inspectPendingMutations).toHaveBeenCalledTimes(1);
    });

    it('maps retained mutation inspection JSON with a server rejection', async () => {
      const retained = {
        mutationID: 'mutation-2',
        localOrder: 8,
        tableID: 'table-1',
        tableName: 'items',
        recordID: 'record-2',
        primaryKeyFieldID: 'field-id',
        primaryKeyLogicalType: 'uuid',
        operation: 'update',
        authoredSchema: { version: 3, hash: 'b'.repeat(64) },
        baseVersion: 'server-v1',
        clientVersion: 'client-v2',
        status: 'server_rejected',
        sourceKind: 'local_write',
        dependsOnMutationID: null,
        normalizedMutationID: null,
        sealedBatchID: 'batch-1',
        sealedOrdinal: 2,
        authoredFields: [
          { fieldID: 'field-name', logicalType: 'string', value: 'rejected' },
        ],
      };
      mockNativeModule.inspectRetainedMutations.mockResolvedValueOnce(
        JSON.stringify([retained])
      );

      await expect(makeClient().inspectRetainedMutations()).resolves.toEqual([
        retained,
      ]);
      expect(mockNativeModule.inspectRetainedMutations).toHaveBeenCalledTimes(1);
    });

    it('maps retained mutation inspection JSON with a push limit status', async () => {
      const retained = {
        mutationID: 'mutation-3',
        localOrder: 9,
        tableID: 'table-1',
        tableName: 'items',
        recordID: 'record-3',
        primaryKeyFieldID: 'field-id',
        primaryKeyLogicalType: 'uuid',
        operation: 'insert',
        authoredSchema: { version: 3, hash: 'c'.repeat(64) },
        baseVersion: null,
        clientVersion: 'client-v1',
        status: 'exceeds_push_limit',
        sourceKind: 'local_write',
        dependsOnMutationID: null,
        normalizedMutationID: null,
        sealedBatchID: null,
        sealedOrdinal: null,
        authoredFields: [
          { fieldID: 'field-name', logicalType: 'string', value: 'oversize' },
        ],
      };
      mockNativeModule.inspectRetainedMutations.mockResolvedValueOnce(
        JSON.stringify([retained])
      );

      await expect(makeClient().inspectRetainedMutations()).resolves.toEqual([
        retained,
      ]);
      expect(mockNativeModule.inspectRetainedMutations).toHaveBeenCalledTimes(1);
    });

    // No public flow creates a legacy record, so this authored payload is the positive codec proof.
    it('decodes authored legacy retained and rejected records with their stored fields only', async () => {
      const retained = {
        representation: 'legacy',
        mutationID: 'mutation-4',
        localOrder: 1,
        tableName: 'orders',
        recordID: 'r1',
        operation: 'insert',
        baseVersion: null,
        clientVersion: '2026-01-01T00:00:00.000000Z',
        status: 'blocked_by_predecessor',
        sourceKind: 'legacy_import',
      };
      const rejected = {
        representation: 'legacy',
        mutationID: 'm1',
        tableName: 'orders',
        recordID: 'r0',
        status: 'rejected_terminal',
        code: 'policy_rejected',
        message: 'blocked',
        serverRowJSON: '{"id":"r0"}',
        serverVersion: 'server-v7',
        createdAt: '2026-01-01T00:00:00.000000Z',
        updatedAt: '2026-01-01T00:00:00.000000Z',
      };
      mockNativeModule.inspectRetainedMutationRecords.mockResolvedValueOnce(JSON.stringify([retained]));
      mockNativeModule.inspectRejectedMutationRecords.mockResolvedValueOnce(JSON.stringify([rejected]));

      const client = makeClient();
      // Both calls settle before the assertions, so a failure leaves no queued native result.
      const results = await Promise.allSettled([
        client.inspectRetainedMutationRecords(),
        client.inspectRejectedMutationRecords(),
      ]);
      expect(results).toStrictEqual([
        { status: 'fulfilled', value: [retained] },
        { status: 'fulfilled', value: [rejected] },
      ]);
    });

    // The payload is otherwise a complete current record, so only the representation rejects it.
    it.each([undefined, 'placeholder'])(
      'rejects a retained mutation with representation %p',
      async (representation) => {
        mockNativeModule.inspectRetainedMutationRecords.mockResolvedValueOnce(
          JSON.stringify([
            {
              representation,
              mutationID: 'mutation-5',
              localOrder: 1,
              tableID: 'table-1',
              tableName: 'orders',
              recordID: 'r1',
              primaryKeyFieldID: 'field-id',
              primaryKeyLogicalType: 'uuid',
              operation: 'insert',
              authoredSchema: { version: 3, hash: 'd'.repeat(64) },
              baseVersion: null,
              clientVersion: 'client-v1',
              status: 'pending',
              sourceKind: 'local_write',
              dependsOnMutationID: null,
              normalizedMutationID: null,
              sealedBatchID: null,
              sealedOrdinal: null,
              authoredFields: [],
            },
          ])
        );

        await expect(makeClient().inspectRetainedMutationRecords()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
      }
    );

    it('maps rejected mutation inspection JSON without parsing retained JSON', async () => {
      const mutationJSON = '{ "operation": "update", "value": 1 }';
      const rejectionJSON = '{ "code": "version_conflict" }';
      const rejected = {
        mutationID: 'mutation-2',
        tableName: 'items',
        recordID: 'record-2',
        status: 'conflict',
        code: 'version_conflict',
        message: null,
        serverRowJSON: '{"id":"record-2"}',
        serverVersion: 'server-v3',
        mutationJSON,
        rejectionJSON,
        createdAt: '2026-08-17T10:00:00.000Z',
        updatedAt: '2026-08-17T10:01:00.000Z',
      };
      mockNativeModule.inspectRejectedMutations.mockResolvedValueOnce(
        JSON.stringify([rejected])
      );

      const result = await makeClient().inspectRejectedMutations();

      expect(result).toStrictEqual([rejected]);
      expect(result[0].mutationJSON).toBe(mutationJSON);
      expect(result[0].rejectionJSON).toBe(rejectionJSON);
    });

    // The payload is otherwise a complete current record, so only the representation rejects it.
    it.each([undefined, 'placeholder'])(
      'rejects a rejected mutation with representation %p',
      async (representation) => {
        mockNativeModule.inspectRejectedMutationRecords.mockResolvedValueOnce(
          JSON.stringify([
            {
              representation,
              mutationID: 'mutation-6',
              tableName: 'orders',
              recordID: 'r1',
              status: 'conflict',
              code: 'version_conflict',
              message: null,
              serverRowJSON: null,
              serverVersion: null,
              mutationJSON: '{}',
              rejectionJSON: '{}',
              createdAt: '2026-01-01T00:00:00.000000Z',
              updatedAt: '2026-01-01T00:00:00.000000Z',
            },
          ])
        );

        await expect(makeClient().inspectRejectedMutationRecords()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
      }
    );

    it('reads client state, retained details, and application rows from one native snapshot', async () => {
      const acceptedID = '00000000-0000-4000-8000-000000000011';
      const acceptedRaw = ` { "mutation_id": "${acceptedID}", "marker": "é" }\n`;
      const migrationJournal = {
        source: { version: 1, hash: 'a'.repeat(64) },
        target: { version: 2, hash: 'b'.repeat(64) },
        action: 'replace',
        phase: 'prepared',
        stored: {
          journal_version: '1', target_manifest_json: '{ "schema_version": 2 }',
          affected_scopes_json: '[]', scope_cursor_updates_json: '{}',
          migration_plan_version: '1', migration_plan_json: '{ "operations": ["add_column"] }',
          migration_plan_hash: 'c'.repeat(64), is_schema_reset: '0',
        },
      };
      const physicalSchema = [{ table_name: 'orders', name: 'id', type: 'TEXT', not_null: true, primary_key_position: 1 }];
      const clientState = {
        schema: null,
        scope_states: [],
        scope_rows: [],
        rebuild_attempts: [],
        ...CLIENT_STATE_COUNTS,
        provenance_maintenance_work_cursor: '3',
      };
      const legacy = {
        representation: 'legacy',
        mutationID: 'mutation-legacy',
        localOrder: 1,
        tableName: 'orders',
        recordID: 'r1',
        operation: 'insert',
        baseVersion: null,
        clientVersion: 'client-v1',
        status: 'pending',
        sourceKind: 'legacy_import',
      };
      mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce({
        ...snapshotResult(clientState, {
          retained_mutations: [legacy],
          rejected_mutations: null,
          migration_journal: migrationJournal,
          physical_schema: physicalSchema,
          accepted_mutation_outcomes: { [acceptedID]: acceptedRaw },
        }),
        applicationRows: [{ id: 'r1', name: 'first' }],
      });

      const { client, inspection } = await makeInspection();
      const snapshot = await inspection.captureSnapshot([
        { sql: 'SELECT * FROM "orders" WHERE "id" = ?', params: ['r1'] },
      ]);

      expect(mockNativeModule.inspectClientStateSnapshot).toHaveBeenCalledTimes(1);
      expect(mockNativeModule.inspectClientStateSnapshot).toHaveBeenCalledWith([
        { sql: 'SELECT * FROM "orders" WHERE "id" = ?', params: ['r1'] },
      ]);
      expect(snapshot).toStrictEqual({
        clientState: {
          schema: null,
          scopeStates: [],
          scopeRows: [],
          rebuildAttempts: [],
          applicationRowCount: Number.MAX_SAFE_INTEGER,
          mutationLedgerCount: 2,
          mutationOutcomeCount: 3,
          sealedBatchCount: 4,
          rejectedMutationCount: 5,
          scopeStateCount: 6,
          scopeRowCount: 7,
          provenanceCount: 8,
          rowMetadataCount: 9,
          rebuildAttemptCount: 10,
          rebuildReceiptCount: 11,
          provenanceMaintenanceWorkCursor: '3',
        },
        retainedMutations: [legacy],
        rejectedMutations: null,
        applicationRows: [{ id: 'r1', name: 'first' }],
        migrationJournal,
        captureOverflowed: false,
        migrationJournalTruncated: false,
        physicalSchema,
        physicalSchemaTruncated: false,
        acceptedMutationOutcomes: { [acceptedID]: acceptedRaw },
        acceptedMutationOutcomesTruncated: false,
      });
      await client.close();
    });

    it.each([
      ['retained_mutations', {}],
      ['rejected_mutations', [{}]],
      ['client_state', null],
      ['migration_journal', undefined],
      ['migration_journal', { source: { version: 1, hash: 'a'.repeat(64) }, target: { version: 2, hash: 'b'.repeat(64) }, action: 'replace', phase: 'prepared', stored: {} }],
      ['migration_journal_truncated', undefined],
      ['physical_schema', [{ table_name: 'orders', name: 'note', type: 'TEXT', not_null: 'false', primary_key_position: 0 }]],
      ['physical_schema_truncated', undefined],
      ['accepted_mutation_outcomes', undefined],
      ['accepted_mutation_outcomes', []],
      ['accepted_mutation_outcomes', { mutation: 1 }],
      ['accepted_mutation_outcomes', Object.fromEntries(Array.from({ length: 513 }, (_, index) => [String(index), '{}']))],
      ['accepted_mutation_outcomes', { mutation: 'é'.repeat(32_768) }],
      ['accepted_mutation_outcomes_truncated', undefined],
      ['accepted_mutation_outcomes_truncated', 'false'],
    ])('rejects an invalid snapshot %s member', async (member, value) => {
      mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce(snapshotResult({
        schema: null,
        scope_states: [],
        scope_rows: [],
        rebuild_attempts: [],
        ...CLIENT_STATE_COUNTS,
        provenance_maintenance_work_cursor: '0',
      }, { [member]: value }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.captureSnapshot()).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
      await client.close();
    });

    it('passes migration targets through the existing pause controls', async () => {
      const { client, inspection } = await makeInspection();
      for (const target of ['migration_prepared', 'migration_committed', 'connect'] as const) {
        await inspection.armTransportPause(target);
        await inspection.awaitTransportPause(target, 1000);
        await inspection.resumeTransportPause();
        expect(mockNativeModule.armTransportPause).toHaveBeenLastCalledWith(target);
        expect(mockNativeModule.awaitTransportPause).toHaveBeenLastCalledWith(target, 1000);
      }
      await expect(inspection.armTransportPause('prepared' as never)).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
      expect(mockNativeModule.armTransportPause).toHaveBeenCalledTimes(3);
      await client.close();
    });

    it('accepts an atomic_batch_rejected terminal rejection', async () => {
      const rejected = {
        mutationID: 'mutation-3',
        tableName: 'items',
        recordID: 'record-3',
        status: 'rejected_terminal',
        code: 'atomic_batch_rejected',
        message: null,
        serverRowJSON: null,
        serverVersion: null,
        mutationJSON: '{}',
        rejectionJSON: '{}',
        createdAt: '2026-08-17T10:00:00.000Z',
        updatedAt: '2026-08-17T10:01:00.000Z',
      };
      mockNativeModule.inspectRejectedMutations.mockResolvedValueOnce(
        JSON.stringify([rejected])
      );

      await expect(makeClient().inspectRejectedMutations()).resolves.toEqual([rejected]);
    });

    it('maps the maximum provenance maintenance cursor without numeric precision loss', async () => {
      mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce(snapshotResult({
        schema: null,
        scope_states: [],
        scope_rows: [],
        rebuild_attempts: [],
        ...CLIENT_STATE_COUNTS,
        provenance_maintenance_work_cursor: '9223372036854775807',
      }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.captureSnapshot().then((snapshot) => snapshot.clientState)).resolves.toEqual({
        schema: null,
        scopeStates: [],
        scopeRows: [],
        rebuildAttempts: [],
        applicationRowCount: Number.MAX_SAFE_INTEGER,
        mutationLedgerCount: 2,
        mutationOutcomeCount: 3,
        sealedBatchCount: 4,
        rejectedMutationCount: 5,
        scopeStateCount: 6,
        scopeRowCount: 7,
        provenanceCount: 8,
        rowMetadataCount: 9,
        rebuildAttemptCount: 10,
        rebuildReceiptCount: 11,
        provenanceMaintenanceWorkCursor: '9223372036854775807',
      });
      await client.close();
    });

    it.each(['-1', '01', '1.0', '9223372036854775808', 1, null])(
      'rejects invalid provenance maintenance cursor %p',
      async (cursor) => {
        mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce(snapshotResult({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          provenance_maintenance_work_cursor: cursor,
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.captureSnapshot()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each(Object.keys(CLIENT_STATE_COUNTS))(
      'rejects a negative %s client-state count',
      async (countName) => {
        mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce(snapshotResult({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          [countName]: -1,
          provenance_maintenance_work_cursor: '0',
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.captureSnapshot()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each([Number.MAX_SAFE_INTEGER + 1, 1.5, '1', null, undefined])(
      'rejects malformed application row count %p',
      async (count) => {
        mockNativeModule.inspectClientStateSnapshot.mockResolvedValueOnce(snapshotResult({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          application_row_count: count,
          provenance_maintenance_work_cursor: '0',
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.captureSnapshot()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each([0, Number.MAX_SAFE_INTEGER])(
      'accepts nonnegative safe request mutation count %p and unknown facts',
      async (mutationCount) => {
        mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
          observations: [{
            sequence: 1,
            operation_class: 'push',
            status_code: 200,
            duration_nanoseconds: 1,
            request_facts: {
              mutation_count: mutationCount,
              future_fact: { enabled: true },
            },
          }],
          overflowed: false,
          sequence_checkpoint: 1,
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.transportObservations()).resolves.toEqual({
          observations: [{
            sequence: 1,
            operationClass: 'push',
            statusCode: 200,
            durationNanoseconds: 1,
            requestFacts: {
              mutation_count: mutationCount,
              future_fact: { enabled: true },
            },
          }],
          overflowed: false,
          sequenceCheckpoint: 1,
        });
        await client.close();
      }
    );

    it.each([
      { native: { error_code: 'capture_pending', retryable: true }, expected: { errorCode: 'capture_pending', retryable: true } },
      { native: { error_code: 'future_code' }, expected: { errorCode: 'future_code' } },
      { native: { retryable: false }, expected: { retryable: false } },
      { native: {}, expected: {} },
    ])('preserves optional transport error facts %p', async ({ native, expected }) => {
      mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
        observations: [{ sequence: 1, operation_class: 'pull', status_code: 503, duration_nanoseconds: 1, ...native }],
        overflowed: false,
        sequence_checkpoint: 1,
      }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.transportObservations()).resolves.toEqual({
        observations: [{ sequence: 1, operationClass: 'pull', statusCode: 503, durationNanoseconds: 1, ...expected }],
        overflowed: false,
        sequenceCheckpoint: 1,
      });
      await client.close();
    });

    it.each([
      { error_code: 503 },
      { error_code: false },
      { error_code: null },
      { error_code: {} },
      { retryable: 'true' },
      { retryable: 1 },
      { retryable: null },
    ])('rejects malformed transport error facts %p', async (facts) => {
      mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
        observations: [{ sequence: 1, operation_class: 'pull', status_code: 503, duration_nanoseconds: 1, ...facts }],
        overflowed: false,
        sequence_checkpoint: 1,
      }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.transportObservations()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
      await client.close();
    });

    it.each([
      Number.MAX_SAFE_INTEGER + 1,
      -1,
      1.5,
      '1',
    ])('rejects invalid request mutation count %p', async (mutationCount) => {
      mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
        observations: [{
          sequence: 1,
          operation_class: 'push',
          status_code: 200,
          duration_nanoseconds: 1,
          request_facts: { mutation_count: mutationCount },
        }],
        overflowed: false,
        sequence_checkpoint: 1,
      }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.transportObservations()).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
      await client.close();
    });

    it('preserves explicit null connect cursor updates and rejects malformed facts', async () => {
      const scope = 'a'.repeat(64);
      const facts = {
        action: 'replace', schema_version: 2, schema_hash: 'b'.repeat(64),
        affected_scope_fingerprints: [scope], affected_scopes_complete: true,
        scope_cursor_updates: { [scope]: null }, scope_cursor_updates_complete: true,
      };
      const observation = {
        sequence: 1, operation_class: 'connect', status_code: 200, duration_nanoseconds: 1,
        connect_response_facts: facts,
      };
      const { client, inspection } = await makeInspection();
      mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
        observations: [observation], overflowed: false, sequence_checkpoint: 1,
      }));
      const snapshot = await inspection.transportObservations();
      const updates = snapshot.observations[0].connectResponseFacts!.scope_cursor_updates as Record<string, unknown>;
      expect(updates[scope]).toBeNull();
      expect(Object.prototype.hasOwnProperty.call(updates, 'c'.repeat(64))).toBe(false);
      for (const invalid of [
        { ...facts, action: 'unknown' },
        { ...facts, schema_version: null },
        { ...facts, schema_hash: 'raw-schema' },
        { ...facts, affected_scope_fingerprints: Array(17).fill(scope) },
        { ...facts, affected_scopes_complete: 'true' },
        { ...facts, scope_cursor_updates: { [scope]: 'raw-token' } },
        { ...facts, scope_cursor_updates: Object.fromEntries(Array.from({ length: 17 }, (_, index) => [index.toString(16).padStart(64, '0'), null])) },
        { ...facts, scope_cursor_updates_complete: null },
        { ...facts, response_body: 'not allowed' },
      ]) {
        mockNativeModule.inspectTransportObservations.mockResolvedValueOnce(JSON.stringify({
          observations: [{ ...observation, connect_response_facts: invalid }], overflowed: false, sequence_checkpoint: 1,
        }));
        await expect(inspection.transportObservations()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
      }
      await client.close();
    });

    it('returns the native process identity', async () => {
      const { client, inspection } = await makeInspection();

      await expect(inspection.processIdentity()).resolves.toBe('ios-app:1234');
      await client.close();
    });

    it.each(['', '1234', 'ios-app:0', 'android-app:-1', 'process-a'])(
      'rejects invalid native process identity %p',
      async (processID) => {
        mockNativeModule.getProcessIdentity.mockResolvedValueOnce(processID);
        const { client, inspection } = await makeInspection();

        await expect(inspection.processIdentity()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each([
      ['getSyncStatus', () => makeClient().getSyncStatus()],
      ['inspectPendingMutations', () => makeClient().inspectPendingMutations()],
      ['inspectRetainedMutations', () => makeClient().inspectRetainedMutations()],
      ['inspectRejectedMutations', () => makeClient().inspectRejectedMutations()],
      ['inspectRetainedMutationRecords', () => makeClient().inspectRetainedMutationRecords()],
      ['inspectRejectedMutationRecords', () => makeClient().inspectRejectedMutationRecords()],
    ])('rejects malformed JSON from %s', async (method, invoke) => {
      mockNativeModule[method].mockResolvedValueOnce('{invalid');

      await expect(invoke()).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
    });

    it.each([
      ['inspectClientStateSnapshot', (inspection: SynchroInspection) => inspection.captureSnapshot(), { inspection: '{invalid', applicationRows: [] }],
      ['inspectTransportObservations', (inspection: SynchroInspection) => inspection.transportObservations(), '{invalid'],
    ])('rejects malformed JSON from the %s facade', async (method, invoke, result) => {
      mockNativeModule[method].mockResolvedValueOnce(result);
      const { client, inspection } = await makeInspection();

      await expect(invoke(inspection)).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
      await client.close();
    });

    it.each([
      ['inspectPendingMutations', () => makeClient().inspectPendingMutations()],
      ['inspectRetainedMutations', () => makeClient().inspectRetainedMutations()],
      ['inspectRejectedMutations', () => makeClient().inspectRejectedMutations()],
      ['inspectRetainedMutationRecords', () => makeClient().inspectRetainedMutationRecords()],
      ['inspectRejectedMutationRecords', () => makeClient().inspectRejectedMutationRecords()],
    ])('rejects structurally invalid JSON from %s', async (method, invoke) => {
      mockNativeModule[method].mockResolvedValueOnce('{}');

      await expect(invoke()).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
    });

    it('passes rejected mutation clearing to native', async () => {
      await makeClient().clearRejectedMutations();

      expect(mockNativeModule.clearRejectedMutations).toHaveBeenCalledTimes(1);
    });
  });

  describe('native ownership', () => {
    it('does not release ownership until asynchronous close completes', async () => {
      let resolveClose: (() => void) | undefined;
      mockNativeModule.close.mockImplementationOnce(
        () => new Promise<void>((resolve) => {
          resolveClose = resolve;
        })
      );
      const first = makeClient();
      const second = new SynchroClient({
        dbPath: '/second.db',
        serverURL: 'http://localhost:8080',
        authProvider: async () => 'second-token',
        clientID: 'second-client',
        appVersion: '1.0.0',
      });

      await first.initialize();
      const closePromise = first.close();
      await expect(second.initialize()).rejects.toMatchObject({
        code: 'CLIENT_ALREADY_ACTIVE',
      });
      resolveClose!();
      await closePromise;
      await second.initialize();
      await second.close();
    });
  });
});
