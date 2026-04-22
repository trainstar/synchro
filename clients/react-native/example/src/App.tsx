import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Platform,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';
import type { ConflictEvent } from '@trainstar/synchro-react-native';

const SYNCHRO_TEST_URL =
  Platform.OS === 'android'
    ? 'http://10.0.2.2:8081'
    : 'http://127.0.0.1:8081';

const USER1_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhMTExMTExMS0xMTExLTExMTEtMTExMS0xMTExMTExMTExMTEiLCJleHAiOjQxMDI0NDQ4MDB9.ZPjufmc-mgkQC6rc6GVNzH9V3jhqQZMl2AuF0Cleuz8';
const USER1_ID = 'a1111111-1111-1111-1111-111111111111';
const USER2_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJiMjIyMjIyMi0yMjIyLTIyMjItMjIyMi0yMjIyMjIyMjIyMjIiLCJleHAiOjQxMDI0NDQ4MDB9.md1BWZARNDofCHihSjDmFY6Wr2L1MBf9r-BDc5zrhFE';
const TEST_SYNC_INTERVAL_SECONDS = 300;
const TEST_PUSH_DEBOUNCE_SECONDS = 60;

type ResultKey =
  | 'init'
  | 'query'
  | 'execute'
  | 'writeTx'
  | 'rollbackTx'
  | 'readTx'
  | 'txTimeout'
  | 'txRecovery'
  | 'start'
  | 'pushPull'
  | 'conflict'
  | 'multiUser'
  | 'stop'
  | 'errorMap'
  | 'seedInit';

type TestResult = boolean | null;
type Results = Record<ResultKey, TestResult>;
type LastResult = { key: ResultKey | null; ok: TestResult };

function createEmptyResults(): Results {
  return {
    init: null,
    query: null,
    execute: null,
    writeTx: null,
    rollbackTx: null,
    readTx: null,
    txTimeout: null,
    txRecovery: null,
    start: null,
    pushPull: null,
    conflict: null,
    multiUser: null,
    stop: null,
    errorMap: null,
    seedInit: null,
  };
}

function uuid(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function createClient(): SynchroClient {
  const launchID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return new SynchroClient({
    dbPath: `synchro-test-${launchID}.db`,
    serverURL: SYNCHRO_TEST_URL,
    authProvider: async () => USER1_JWT,
    clientID: `rn-test-device-${launchID}`,
    appVersion: '1.0.0',
    syncInterval: TEST_SYNC_INTERVAL_SECONDS,
    pushDebounce: TEST_PUSH_DEBOUNCE_SECONDS,
  });
}

async function syncHTTP(
  method: string,
  path: string,
  token: string,
  body?: object
): Promise<any> {
  const res = await fetch(`${SYNCHRO_TEST_URL}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

async function connectManualClient(token: string, clientID: string): Promise<any> {
  return syncHTTP('POST', '/sync/connect', token, {
    client_id: clientID,
    platform: 'test',
    app_version: '1.0.0',
    protocol_version: 1,
    schema: {
      version: 0,
      hash: '',
    },
    scope_set_version: 0,
    known_scopes: {},
  });
}

function schemaRefFromConnect(connect: any) {
  return {
    version: connect.schema.version,
    hash: connect.schema.hash,
  };
}

async function rebuildAllAssignedScopes(
  token: string,
  clientID: string,
  connect: any
): Promise<any[]> {
  const records: any[] = [];
  const scopes = Array.isArray(connect.scopes?.add) ? connect.scopes.add : [];

  for (const scope of scopes) {
    let cursor = scope.cursor ?? null;

    while (true) {
      const rebuild = await syncHTTP('POST', '/sync/rebuild', token, {
        client_id: clientID,
        scope: scope.id,
        cursor,
        limit: 100,
      });

      if (Array.isArray(rebuild.records)) {
        records.push(...rebuild.records);
      }

      if (!rebuild.has_more) {
        break;
      }

      cursor = rebuild.cursor ?? null;
    }
  }

  return records;
}

async function waitForPendingDrain(client: SynchroClient, timeoutMs = 5000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if ((await client.pendingChangeCount()) === 0) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  return false;
}

async function waitForCondition(
  condition: () => Promise<boolean>,
  timeoutMs = 5000,
  intervalMs = 250
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await condition()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

async function waitForWAL(delayMs = 1000) {
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

async function insertCustomer(
  client: SynchroClient,
  id: string,
  userID: string,
  name: string
) {
  await client.execute(
    "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, datetime('now'), datetime('now'))",
    [id, userID, name]
  );
}

function StatusBadge({ label, ok }: { label: string; ok: TestResult }) {
  return (
    <View style={styles.badge}>
      <Text style={styles.badgeLabel}>{label}</Text>
      <Text
        testID={`badge-${label}`}
        style={[
          styles.badgeStatus,
          ok === null ? styles.pending : ok ? styles.pass : styles.fail,
        ]}
      >
        {ok === null ? 'PENDING' : ok ? 'PASS' : 'FAIL'}
      </Text>
    </View>
  );
}

export default function App() {
  const [client, setClient] = useState(() => createClient());
  const initializedRef = useRef(false);
  const startedRef = useRef(false);
  const statusSubscriptionRef = useRef<(() => void) | null>(null);
  const conflictSubscriptionRef = useRef<(() => void) | null>(null);
  const conflictsRef = useRef<ConflictEvent[]>([]);
  const currentStepRef = useRef('idle');

  const [results, setResults] = useState<Results>(() => createEmptyResults());
  const [displayStatus, setDisplayStatus] = useState('idle');
  const [currentStep, setCurrentStep] = useState('idle');
  const [lastError, setLastError] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<LastResult>({
    key: null,
    ok: null,
  });

  useEffect(() => {
    return () => {
      statusSubscriptionRef.current?.();
      conflictSubscriptionRef.current?.();
      void client.close().catch(() => {
        // Cleanup is best-effort when Detox terminates the app.
      });
    };
  }, [client]);

  const update = useCallback((key: ResultKey, ok: boolean) => {
    setResults((prev) => ({ ...prev, [key]: ok }));
    setLastResult({ key, ok });
    const nextStep = ok ? `${key}:pass` : `${key}:fail`;
    currentStepRef.current = nextStep;
    setCurrentStep(nextStep);
    console.log(`step: ${nextStep}`);
  }, [client]);

  const markStep = useCallback((step: string) => {
    currentStepRef.current = step;
    setCurrentStep(step);
    console.log(`step: ${step}`);
  }, []);

  const formatError = useCallback((error: unknown) => {
    if (error instanceof Error) {
      return `${error.name}: ${error.message}`;
    }
    return String(error);
  }, []);

  const captureError = useCallback((key: ResultKey, error: unknown) => {
    const message = `${key}@${currentStepRef.current}: ${formatError(error)}`;
    setLastError(message);
    setCurrentStep(`${key}:error`);
    console.error(message, error);
  }, [formatError]);

  const ensureInitialized = useCallback(async () => {
    if (initializedRef.current) {
      return;
    }
    await client.initialize();
    statusSubscriptionRef.current?.();
    statusSubscriptionRef.current = client.onStatusChange((status) => {
      setDisplayStatus(status.status);
    });
    conflictSubscriptionRef.current?.();
    conflictSubscriptionRef.current = client.onConflict((event) => {
      conflictsRef.current.push(event);
    });
    initializedRef.current = true;
    setDisplayStatus('idle');
  }, [client]);

  const ensureStarted = useCallback(async () => {
    await ensureInitialized();
    if (startedRef.current) {
      return;
    }
    await client.start();
    startedRef.current = true;
  }, [client, ensureInitialized]);

  const stopSync = useCallback(async () => {
    if (!startedRef.current) {
      return;
    }
    await client.stop();
    startedRef.current = false;
  }, [client]);

  const ensureLocalTable = useCallback(async () => {
    await ensureInitialized();
    await client.createTable('test_items', [
      { name: 'id', type: 'TEXT', primaryKey: true },
      { name: 'name', type: 'TEXT' },
    ]);
  }, [client, ensureInitialized]);

  const resetHarness = useCallback(async () => {
    try {
      statusSubscriptionRef.current?.();
      statusSubscriptionRef.current = null;
      conflictSubscriptionRef.current?.();
      conflictSubscriptionRef.current = null;
      await client.close();
    } catch {
      // Best-effort reset for the harness.
    }

    conflictsRef.current = [];
    initializedRef.current = false;
    startedRef.current = false;
    setResults(createEmptyResults());
    setLastResult({ key: null, ok: null });
    setDisplayStatus('idle');
    setCurrentStep('idle');
    currentStepRef.current = 'idle';
    setLastError(null);
    setClient(createClient());
  }, [client]);

  const runInit = useCallback(async () => {
    try {
      await ensureInitialized();
      update('init', true);
    } catch {
      update('init', false);
    }
  }, [ensureInitialized, update]);

  const runQuery = useCallback(async () => {
    try {
      await ensureInitialized();
      const rows = await client.query('SELECT 1 as value');
      update('query', rows.length === 1 && rows[0].value === 1);
    } catch {
      update('query', false);
    }
  }, [client, ensureInitialized, update]);

  const runExecute = useCallback(async () => {
    try {
      await ensureLocalTable();
      const result = await client.execute(
        'INSERT INTO test_items (id, name) VALUES (?, ?)',
        [uuid(), 'test']
      );
      update('execute', result.rowsAffected === 1);
    } catch {
      update('execute', false);
    }
  }, [client, ensureLocalTable, update]);

  const runWriteTx = useCallback(async () => {
    try {
      await ensureLocalTable();
      const recordID = uuid();
      const value = await client.writeTransaction(async (tx) => {
        await tx.execute(
          'INSERT INTO test_items (id, name) VALUES (?, ?)',
          [recordID, 'txtest']
        );
        const rows = await tx.query(
          'SELECT name FROM test_items WHERE id = ?',
          [recordID]
        );
        return rows[0]?.name;
      });
      update('writeTx', value === 'txtest');
    } catch {
      update('writeTx', false);
    }
  }, [client, ensureLocalTable, update]);

  const runRollbackTx = useCallback(async () => {
    try {
      await ensureLocalTable();
      const rollbackID = uuid();
      try {
        await client.writeTransaction(async (tx) => {
          await tx.execute(
            'INSERT INTO test_items (id, name) VALUES (?, ?)',
            [rollbackID, 'should-not-persist']
          );
          throw new Error('intentional rollback');
        });
      } catch {
        // expected
      }

      const row = await client.queryOne(
        'SELECT * FROM test_items WHERE id = ?',
        [rollbackID]
      );
      update('rollbackTx', row === null);
    } catch {
      update('rollbackTx', false);
    }
  }, [client, ensureLocalTable, update]);

  const runReadTx = useCallback(async () => {
    try {
      await ensureLocalTable();
      const seedID = uuid();
      await client.execute(
        'INSERT INTO test_items (id, name) VALUES (?, ?)',
        [seedID, 'read-seed']
      );
      const rows = await client.readTransaction((tx) =>
        tx.query('SELECT * FROM test_items WHERE id = ?', [seedID])
      );
      update('readTx', rows.length === 1 && rows[0].id === seedID);
    } catch {
      update('readTx', false);
    }
  }, [client, ensureLocalTable, update]);

  const runTxTimeout = useCallback(async () => {
    try {
      await ensureLocalTable();
      await client.writeTransaction(async () => {
        await new Promise((resolve) => setTimeout(resolve, 6000));
      });
      update('txTimeout', false);
    } catch (error: any) {
      update(
        'txTimeout',
        error?.code === 'TRANSACTION_TIMEOUT' ||
          String(error?.message ?? '').includes('timeout')
      );
    }
  }, [client, ensureLocalTable, update]);

  const runTxRecovery = useCallback(async () => {
    try {
      await ensureLocalTable();
      try {
        await client.writeTransaction(async () => {
          await new Promise((resolve) => setTimeout(resolve, 6000));
        });
      } catch {
        // expected timeout
      }

      const recoveryID = uuid();
      const result = await client.execute(
        'INSERT INTO test_items (id, name) VALUES (?, ?)',
        [recoveryID, 'recovered']
      );
      const row = await client.queryOne(
        'SELECT name FROM test_items WHERE id = ?',
        [recoveryID]
      );
      update(
        'txRecovery',
        result.rowsAffected === 1 && row?.name === 'recovered'
      );
    } catch {
      update('txRecovery', false);
    }
  }, [client, ensureLocalTable, update]);

  const runStart = useCallback(async () => {
    try {
      await ensureStarted();
      await stopSync();
      update('start', true);
    } catch {
      update('start', false);
    }
  }, [ensureStarted, stopSync, update]);

  const runPushPull = useCallback(async () => {
    try {
      setLastError(null);
      markStep('pushPull:start');
      await ensureStarted();
      markStep('pushPull:started');
      const customerID = uuid();
      await insertCustomer(client, customerID, USER1_ID, 'push-test-customer');
      markStep('pushPull:inserted');
      await client.syncNow();
      markStep('pushPull:synced');
      update('pushPull', await waitForPendingDrain(client));
    } catch (error) {
      captureError('pushPull', error);
      update('pushPull', false);
    } finally {
      try {
        await stopSync();
      } catch {
        // Best-effort cleanup for the harness.
      }
    }
  }, [client, ensureStarted, stopSync, update]);

  const runConflict = useCallback(async () => {
    try {
      setLastError(null);
      markStep('conflict:start');
      await ensureStarted();
      conflictsRef.current = [];
      markStep('conflict:started');

      const recordID = uuid();
      await insertCustomer(client, recordID, USER1_ID, 'original');
      markStep('conflict:inserted');
      await client.syncNow();
      markStep('conflict:initial-sync');
      await waitForPendingDrain(client);

      await stopSync();
      markStep('conflict:stopped');

      const localVersion = '2026-01-01T00:00:00.000Z';
      await client.execute(
        'UPDATE customers SET name = ?, updated_at = ? WHERE id = ?',
        ['client-version', localVersion, recordID]
      );
      markStep('conflict:updated-local');

      const clientBID = `rn-conflict-client-${uuid()}`;
      const connect = await connectManualClient(USER1_JWT, clientBID);
      markStep('conflict:manual-connected');

      const serverVersion = '2030-01-01T00:00:00.000Z';
      await syncHTTP('POST', '/sync/push', USER1_JWT, {
        client_id: clientBID,
        batch_id: `batch-${recordID}`,
        schema: schemaRefFromConnect(connect),
        mutations: [
          {
            mutation_id: `mutation-${recordID}`,
            table: 'customers',
            op: 'update',
            pk: {
              id: recordID,
            },
            columns: {
              user_id: USER1_ID,
              name: 'server-version',
              updated_at: serverVersion,
            },
            client_version: serverVersion,
          },
        ],
      });
      markStep('conflict:manual-pushed');

      await waitForWAL(1500);
      markStep('conflict:wal-1');

      await client.syncNow();
      markStep('conflict:resynced');

      const conflictResolved = await waitForCondition(async () => {
        const row = await client.queryOne(
          'SELECT name FROM customers WHERE id = ?',
          [recordID]
        );
        const conflictEvent = conflictsRef.current.find(
          (event) => event.recordID === recordID
        );
        return (
          conflictEvent?.serverData?.name === 'server-version' &&
          row?.name === 'server-version' &&
          (await client.pendingChangeCount()) === 0
        );
      }, 10000);
      update('conflict', conflictResolved);
    } catch (error) {
      captureError('conflict', error);
      update('conflict', false);
    } finally {
      try {
        await stopSync();
      } catch {
        // Best-effort cleanup for the harness.
      }
    }
  }, [client, ensureStarted, stopSync, update]);

  const runMultiUser = useCallback(async () => {
    try {
      setLastError(null);
      markStep('multiUser:start');
      await ensureStarted();
      markStep('multiUser:started');

      const isolationID = uuid();
      await insertCustomer(client, isolationID, USER1_ID, 'user1-only');
      markStep('multiUser:inserted');
      await client.syncNow();
      markStep('multiUser:synced');
      await waitForPendingDrain(client);
      await waitForWAL();
      markStep('multiUser:wal');

      const client2ID = `rn-isolation-client-${uuid()}`;
      const connect = await connectManualClient(USER2_JWT, client2ID);
      markStep('multiUser:user2-connected');
      const rebuiltRecords = await rebuildAllAssignedScopes(
        USER2_JWT,
        client2ID,
        connect
      );
      markStep('multiUser:user2-rebuilt');

      const hasUser1Record = rebuiltRecords.some(
        (record: any) =>
          record.table === 'customers' && record.pk?.id === isolationID
      );
      update('multiUser', !hasUser1Record);
    } catch (error) {
      captureError('multiUser', error);
      update('multiUser', false);
    } finally {
      try {
        await stopSync();
      } catch {
        // Best-effort cleanup for the harness.
      }
    }
  }, [client, ensureStarted, stopSync, update]);

  const runStop = useCallback(async () => {
    try {
      await ensureStarted();
      await stopSync();
      update('stop', true);
    } catch {
      update('stop', false);
    } finally {
      startedRef.current = false;
    }
  }, [ensureStarted, stopSync, update]);

  const runErrorMap = useCallback(async () => {
    try {
      await ensureInitialized();
      await client.query('SELECT * FROM nonexistent_table_xyz');
      update('errorMap', false);
    } catch (error: any) {
      update('errorMap', typeof error?.code === 'string' && error.code.length > 0);
    }
  }, [client, ensureInitialized, update]);

  const runSeedInit = useCallback(async () => {
    const seedID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    let seedClient: SynchroClient | null = null;
    try {
      seedClient = new SynchroClient({
        dbPath: `synchro-seed-test-${seedID}.db`,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID: `rn-seed-device-${seedID}`,
        appVersion: '1.0.0',
        seedDatabasePath: 'seed.db',
      });
      await seedClient.initialize();

      // Verify the orders table exists from the seed
      const rows = await seedClient.query('SELECT * FROM orders');
      const tableExists = Array.isArray(rows);

      // Insert a row into orders
      const orderID = uuid();
      await seedClient.execute(
        "INSERT INTO orders (id, user_id, ship_address, created_at, updated_at) VALUES (?, ?, ?, datetime('now'), datetime('now'))",
        [orderID, USER1_ID, 'seed-test']
      );

      // Verify the CDC trigger fired
      const pending = await seedClient.query(
        'SELECT * FROM _synchro_pending_changes WHERE record_id = ?',
        [orderID]
      );

      update('seedInit', tableExists && pending.length > 0);
    } catch {
      update('seedInit', false);
    } finally {
      try {
        await seedClient?.close();
      } catch {
        // Best-effort cleanup.
      }
    }
  }, [update]);

  return (
    <View style={styles.container}>
      <ScrollView testID="test-scroll" contentContainerStyle={styles.scroll}>
        <Text style={styles.header} testID="header">
          Synchro RN Test Harness
        </Text>

        <View style={styles.statusRow} testID="sync-status">
          <Text>Status</Text>
          <Text testID="status-value">{displayStatus}</Text>
        </View>
        <Text testID="step-value">{currentStep}</Text>
        <Text testID="error-value">{lastError ?? 'none'}</Text>

        <TouchableOpacity
          style={styles.button}
          onPress={() => {
            void resetHarness();
          }}
          testID="btn-reset"
        >
          <Text>Reset Harness</Text>
        </TouchableOpacity>

        {Object.entries(results).map(([key, value]) => (
          <StatusBadge key={key} label={key} ok={value} />
        ))}

        <View style={styles.buttons}>
          <TouchableOpacity style={styles.button} onPress={runInit} testID="btn-init">
            <Text>Initialize</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runQuery} testID="btn-query">
            <Text>Query</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runExecute} testID="btn-execute">
            <Text>Execute</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runWriteTx} testID="btn-writeTx">
            <Text>Write Tx</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runRollbackTx} testID="btn-rollbackTx">
            <Text>Rollback Tx</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runReadTx} testID="btn-readTx">
            <Text>Read Tx</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runTxTimeout} testID="btn-txTimeout">
            <Text>Tx Timeout</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runTxRecovery} testID="btn-txRecovery">
            <Text>Tx Recovery</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runStart} testID="btn-start">
            <Text>Start Sync</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runPushPull} testID="btn-pushPull">
            <Text>Push/Pull</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runConflict} testID="btn-conflict">
            <Text>Conflict</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runMultiUser} testID="btn-multiUser">
            <Text>Multi-User</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runStop} testID="btn-stop">
            <Text>Stop Sync</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runErrorMap} testID="btn-errorMap">
            <Text>Error Mapping</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runSeedInit} testID="btn-seedInit">
            <Text>Seed Init</Text>
          </TouchableOpacity>
        </View>
      </ScrollView>
      <View style={styles.lastResult} testID="last-result">
        <Text testID="last-result-key">{lastResult.key ?? 'none'}</Text>
        <Text testID="last-result-status">
          {lastResult.ok === null
            ? 'PENDING'
            : lastResult.ok
              ? 'PASS'
              : 'FAIL'}
        </Text>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#fff' },
  scroll: { padding: 20, paddingBottom: 120 },
  header: { fontSize: 20, fontWeight: 'bold', marginBottom: 16 },
  statusRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 16,
    padding: 8,
    backgroundColor: '#f0f0f0',
    borderRadius: 4,
  },
  badge: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: '#eee',
  },
  badgeLabel: { fontSize: 14 },
  badgeStatus: { fontSize: 14, fontWeight: 'bold' },
  pending: { color: '#999' },
  pass: { color: '#0a0' },
  fail: { color: '#c00' },
  buttons: { marginTop: 20 },
  lastResult: {
    marginHorizontal: 20,
    marginBottom: 20,
    padding: 12,
    borderRadius: 6,
    backgroundColor: '#f5f5f5',
    gap: 4,
  },
  button: {
    backgroundColor: '#e0e0e0',
    padding: 12,
    borderRadius: 6,
    marginBottom: 8,
    alignItems: 'center',
  },
});
