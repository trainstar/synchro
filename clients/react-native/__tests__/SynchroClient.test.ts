import { SynchroClient } from '../src/SynchroClient';
import { SynchroInspection } from '../src/inspection';
import { SyncStatus } from '../src/types';
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

      expect(result).toEqual([rejected]);
      expect(result[0].mutationJSON).toBe(mutationJSON);
      expect(result[0].rejectionJSON).toBe(rejectionJSON);
    });

    it('maps the maximum provenance maintenance cursor without numeric precision loss', async () => {
      mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
        schema: null,
        scope_states: [],
        scope_rows: [],
        rebuild_attempts: [],
        ...CLIENT_STATE_COUNTS,
        provenance_maintenance_work_cursor: '9223372036854775807',
      }));

      const { client, inspection } = await makeInspection();
      await expect(inspection.clientState()).resolves.toEqual({
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
        mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          provenance_maintenance_work_cursor: cursor,
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.clientState()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each(Object.keys(CLIENT_STATE_COUNTS))(
      'rejects a negative %s client-state count',
      async (countName) => {
        mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          [countName]: -1,
          provenance_maintenance_work_cursor: '0',
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.clientState()).rejects.toMatchObject({
          code: 'INVALID_RESPONSE',
        });
        await client.close();
      }
    );

    it.each([Number.MAX_SAFE_INTEGER + 1, 1.5, '1', null, undefined])(
      'rejects malformed application row count %p',
      async (count) => {
        mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
          schema: null,
          scope_states: [],
          scope_rows: [],
          rebuild_attempts: [],
          ...CLIENT_STATE_COUNTS,
          application_row_count: count,
          provenance_maintenance_work_cursor: '0',
        }));

        const { client, inspection } = await makeInspection();
        await expect(inspection.clientState()).rejects.toMatchObject({
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
    ])('rejects malformed JSON from %s', async (method, invoke) => {
      mockNativeModule[method].mockResolvedValueOnce('{invalid');

      await expect(invoke()).rejects.toMatchObject({
        code: 'INVALID_RESPONSE',
      });
    });

    it.each([
      ['inspectClientState', (inspection: SynchroInspection) => inspection.clientState()],
      ['inspectTransportObservations', (inspection: SynchroInspection) => inspection.transportObservations()],
    ])('rejects malformed JSON from the %s facade', async (method, invoke) => {
      mockNativeModule[method].mockResolvedValueOnce('{invalid');
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
