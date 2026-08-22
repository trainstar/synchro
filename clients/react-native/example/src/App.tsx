import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Platform,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';
import type {
  ConflictEvent,
  SyncEvent,
} from '@trainstar/synchro-react-native';
import { ConformanceHarness } from './conformance/ConformanceHarness';

const SYNCHRO_TEST_URL =
  Platform.OS === 'android'
    ? 'http://10.0.2.2:8091'
    : 'http://127.0.0.1:8091';

const USER1_JWT =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhMTExMTExMS0xMTExLTExMTEtMTExMS0xMTExMTExMTExMTEiLCJleHAiOjQxMDI0NDQ4MDB9.ZPjufmc-mgkQC6rc6GVNzH9V3jhqQZMl2AuF0Cleuz8';
const USER1_ID = 'a1111111-1111-1111-1111-111111111111';
const TEST_SYNC_INTERVAL_SECONDS = 300;
const TEST_PUSH_DEBOUNCE_SECONDS = 60;
interface AppProps {
  conformanceDetox?: boolean;
}

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
  | 'lifecycle'
  | 'pushPull'
  | 'conflict'
  | 'multiUser'
  | 'stop'
  | 'errorMap'
  | 'offlineFirst'
  | 'seedInit'
  | 'seedResume'
  | 'seedCorrupt';

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
    lifecycle: null,
    pushPull: null,
    conflict: null,
    multiUser: null,
    stop: null,
    errorMap: null,
    offlineFirst: null,
    seedInit: null,
    seedResume: null,
    seedCorrupt: null,
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

async function waitForSyncedTable(
  client: SynchroClient,
  tableName: string,
  timeoutMs = 15000
) {
  return waitForCondition(async () => {
    try {
      await client.query(`SELECT 1 FROM ${tableName} LIMIT 1`);
      return true;
    } catch {
      return false;
    }
  }, timeoutMs, 250);
}

async function releaseClient(client: SynchroClient | null) {
  try {
    await client?.close();
  } catch {
    // Ownership release remains best-effort during harness cleanup.
  }
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

export default function App({ conformanceDetox = false }: AppProps) {
  if (conformanceDetox) {
    return (
      <ConformanceHarness
        appVersion="1.0.0"
        authToken={USER1_JWT}
        serverURL={SYNCHRO_TEST_URL}
      />
    );
  }

  return <StandardApp />;
}

function StandardApp() {
  const [client, setClient] = useState(() => createClient());
  const clientRef = useRef(client);
  const initializedRef = useRef(false);
  const startedRef = useRef(false);
  const statusSubscriptionRef = useRef<(() => void) | null>(null);
  const conflictSubscriptionRef = useRef<(() => void) | null>(null);
  const conflictsRef = useRef<ConflictEvent[]>([]);
  const pendingConflictRecordRef = useRef<string | null>(null);
  const pendingMultiUserRecordRef = useRef<string | null>(null);
  const currentStepRef = useRef('idle');

  const [results, setResults] = useState<Results>(() => createEmptyResults());
  const [harnessGeneration, setHarnessGeneration] = useState(0);
  const [displayStatus, setDisplayStatus] = useState('uninitialized');
  const [currentStep, setCurrentStep] = useState('idle');
  const [pendingConflictRecordID, setPendingConflictRecordID] = useState<string | null>(null);
  const [pendingMultiUserRecordID, setPendingMultiUserRecordID] = useState<string | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<LastResult>({
    key: null,
    ok: null,
  });

  useEffect(() => {
    clientRef.current = client;
  }, [client]);

  useEffect(() => {
    return () => {
      const statusSubscription = statusSubscriptionRef.current;
      const conflictSubscription = conflictSubscriptionRef.current;
      statusSubscriptionRef.current = null;
      conflictSubscriptionRef.current = null;
      statusSubscription?.();
      conflictSubscription?.();
      void releaseClient(clientRef.current);
    };
  }, []);

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
    setDisplayStatus('local_ready');
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
      { name: 'note', type: 'TEXT' },
    ]);
  }, [client, ensureInitialized]);

  const releaseHarnessClient = useCallback(async () => {
    statusSubscriptionRef.current?.();
    statusSubscriptionRef.current = null;
    conflictSubscriptionRef.current?.();
    conflictSubscriptionRef.current = null;
    await releaseClient(client);
    initializedRef.current = false;
    startedRef.current = false;
  }, [client]);

  const resetHarness = useCallback(async () => {
    currentStepRef.current = 'reset:start';
    setCurrentStep('reset:start');
    setDisplayStatus('resetting');
    statusSubscriptionRef.current?.();
    statusSubscriptionRef.current = null;
    conflictSubscriptionRef.current?.();
    conflictSubscriptionRef.current = null;
    await releaseClient(client);

    conflictsRef.current = [];
    pendingConflictRecordRef.current = null;
    pendingMultiUserRecordRef.current = null;
    initializedRef.current = false;
    startedRef.current = false;
    setResults(createEmptyResults());
    setLastResult({ key: null, ok: null });
    setDisplayStatus('uninitialized');
    setCurrentStep('reset:complete');
    currentStepRef.current = 'reset:complete';
    setPendingConflictRecordID(null);
    setPendingMultiUserRecordID(null);
    setLastError(null);
    setClient(createClient());
    setHarnessGeneration((generation) => generation + 1);
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
      const recordID = uuid();
      const result = await client.execute(
        'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
        [recordID, 'test', null]
      );
      const row = await client.queryOne(
        'SELECT id FROM test_items WHERE id = ? AND note IS ?',
        [recordID, null]
      );
      update('execute', result.rowsAffected === 1 && row?.id === recordID);
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
          'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
          [recordID, 'txtest', null]
        );
        const rows = await tx.query(
          'SELECT name FROM test_items WHERE id = ? AND note IS ?',
          [recordID, null]
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
            'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
            [rollbackID, 'should-not-persist', null]
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
      const rejectedWriteID = uuid();
      let rejectedWrite = false;
      try {
        await client.readTransaction((tx) =>
          tx.query(
            'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?) RETURNING id',
            [rejectedWriteID, 'must-not-persist', null]
          )
        );
      } catch {
        rejectedWrite = true;
      }
      const rejectedWriteRow = await client.queryOne(
        'SELECT id FROM test_items WHERE id = ?',
        [rejectedWriteID]
      );

      const seedID = uuid();
      await client.execute(
        'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
        [seedID, 'read-seed', null]
      );
      const rows = await client.readTransaction((tx) =>
        tx.query(
          'SELECT * FROM test_items WHERE id = ? AND note IS ?',
          [seedID, null]
        )
      );
      update(
        'readTx',
        rejectedWrite &&
          rejectedWriteRow === null &&
          rows.length === 1 &&
          rows[0].id === seedID
      );
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
      const interruptWrite = async (
        recordID: string,
        interrupt: () => Promise<void>
      ) => {
        let releaseTransaction!: () => void;
        let markTransactionStarted!: () => void;
        const transactionGate = new Promise<void>((resolve) => {
          releaseTransaction = resolve;
        });
        const transactionStarted = new Promise<void>((resolve) => {
          markTransactionStarted = resolve;
        });
        const transaction = client.writeTransaction(async (tx) => {
          await tx.execute(
            'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
            [recordID, 'must-rollback', null]
          );
          markTransactionStarted();
          await transactionGate;
        });
        const rejected = transaction.then(
          () => false,
          () => true
        );

        await transactionStarted;
        try {
          await interrupt();
        } finally {
          releaseTransaction();
        }
        return rejected;
      };

      const closeID = uuid();
      const closeRejected = await interruptWrite(closeID, () => client.close());
      await client.initialize();

      const reinitializeID = uuid();
      const reinitializeRejected = await interruptWrite(
        reinitializeID,
        () => client.initialize()
      );

      const closeRow = await client.queryOne(
        'SELECT id FROM test_items WHERE id = ?',
        [closeID]
      );
      const reinitializeRow = await client.queryOne(
        'SELECT id FROM test_items WHERE id = ?',
        [reinitializeID]
      );

      const recoveryID = uuid();
      const result = await client.execute(
        'INSERT INTO test_items (id, name, note) VALUES (?, ?, ?)',
        [recoveryID, 'recovered', null]
      );
      const row = await client.queryOne(
        'SELECT name FROM test_items WHERE id = ? AND note IS ?',
        [recoveryID, null]
      );
      update(
        'txRecovery',
        closeRejected &&
          reinitializeRejected &&
          closeRow === null &&
          reinitializeRow === null &&
          result.rowsAffected === 1 &&
          row?.name === 'recovered'
      );
    } catch {
      update('txRecovery', false);
    }
  }, [client, ensureLocalTable, update]);

  const runStart = useCallback(async () => {
    try {
      setLastError(null);
      markStep('start:start');
      await ensureStarted();
      markStep('start:started');
      await stopSync();
      markStep('start:stopped');
      update('start', true);
    } catch (error) {
      captureError('start', error);
      update('start', false);
    }
  }, [captureError, ensureStarted, markStep, stopSync, update]);

  const runPushPull = useCallback(async () => {
    try {
      setLastError(null);
      markStep('pushPull:start');
      await ensureStarted();
      await client.syncNow();
      if (!(await waitForSyncedTable(client, 'customers'))) {
        throw new Error('customers table was not ready after starting sync');
      }
      markStep('pushPull:started');
      const customerID = uuid();
      await insertCustomer(client, customerID, USER1_ID, 'push-test-customer');
      markStep('pushPull:inserted');
      await client.syncNow();
      markStep('pushPull:synced');
      const pendingDrained = await waitForPendingDrain(client);
      const rejection = (await client.inspectRejectedMutations()).find(
        (mutation) => mutation.tableName === 'customers' && mutation.recordID === customerID
      );
      if (!pendingDrained || rejection !== undefined) {
        throw new Error('push/pull mutation did not complete without rejection');
      }
      const localRow = await client.queryOne(
        'SELECT name FROM customers WHERE id = ?',
        [customerID]
      );
      if (localRow?.name !== 'push-test-customer') {
        throw new Error('accepted push/pull customer row is missing locally');
      }
      update('pushPull', true);
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
      const pendingRecordID = pendingConflictRecordRef.current;
      if (pendingRecordID === null) {
        await ensureStarted();
        await client.syncNow();
        if (!(await waitForSyncedTable(client, 'customers'))) {
          throw new Error('customers table was not ready after starting sync');
        }
        conflictsRef.current = [];
        markStep('conflict:started');

        const recordID = uuid();
        await insertCustomer(client, recordID, USER1_ID, 'original');
        markStep('conflict:inserted');
        await client.syncNow();
        markStep('conflict:initial-sync');
        if (!(await waitForPendingDrain(client))) {
          throw new Error('initial conflict mutation did not drain');
        }

        await stopSync();
        markStep('conflict:stopped');

        const localVersion = '2026-01-01T00:00:00.000Z';
        await client.execute(
          'UPDATE customers SET name = ?, updated_at = ? WHERE id = ?',
          ['client-version', localVersion, recordID]
        );
        pendingConflictRecordRef.current = recordID;
        setPendingConflictRecordID(recordID);
        markStep('conflict:awaiting-server');
        return;
      }

      await ensureStarted();
      markStep('conflict:resync-started');
      await client.syncNow();
      markStep('conflict:resynced');

      const conflictResolved = await waitForCondition(async () => {
        const row = await client.queryOne(
          'SELECT name FROM customers WHERE id = ?',
          [pendingRecordID]
        );
        const conflictEvent = conflictsRef.current.find(
          (event) => event.recordID === pendingRecordID
        );
        return (
          conflictEvent?.serverData?.name === 'server-version' &&
          row?.name === 'server-version' &&
          (await client.pendingChangeCount()) === 0
        );
      }, 10000);
      if (!conflictResolved) {
        setLastError(
          JSON.stringify({
            conflicts: conflictsRef.current,
            row: await client.queryOne(
              'SELECT name, updated_at FROM customers WHERE id = ?',
              [pendingRecordID]
            ),
            pendingCount: await client.pendingChangeCount(),
          })
        );
      }
      pendingConflictRecordRef.current = null;
      setPendingConflictRecordID(null);
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
      const pendingRecordID = pendingMultiUserRecordRef.current;
      if (pendingRecordID === null) {
        await ensureStarted();
        await client.syncNow();
        if (!(await waitForSyncedTable(client, 'customers'))) {
          throw new Error('customers table was not ready after starting sync');
        }
        markStep('multiUser:started');

        const isolationID = uuid();
        await insertCustomer(client, isolationID, USER1_ID, 'user1-only');
        markStep('multiUser:inserted');
        await client.syncNow();
        markStep('multiUser:synced');
        const pendingDrained = await waitForPendingDrain(client);
        const rejection = (await client.inspectRejectedMutations()).find(
          (mutation) => mutation.tableName === 'customers' && mutation.recordID === isolationID
        );
        if (!pendingDrained || rejection !== undefined) {
          throw new Error('isolation mutation did not complete without rejection');
        }

        await stopSync();
        pendingMultiUserRecordRef.current = isolationID;
        setPendingMultiUserRecordID(isolationID);
        markStep('multiUser:awaiting-server');
        return;
      }

      const localRow = await client.queryOne(
        'SELECT user_id, name FROM customers WHERE id = ?',
        [pendingRecordID]
      );
      const localMutation = (await client.inspectRejectedMutations()).find(
        (mutation) => mutation.tableName === 'customers' && mutation.recordID === pendingRecordID
      );
      const isolationVerified =
        localRow?.user_id === USER1_ID &&
        localRow?.name === 'user1-only' &&
        localMutation === undefined;
      pendingMultiUserRecordRef.current = null;
      setPendingMultiUserRecordID(null);
      update('multiUser', isolationVerified);
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
      setLastError(null);
      markStep('stop:start');
      await ensureStarted();
      markStep('stop:started');
      await stopSync();
      markStep('stop:stopped');
      update('stop', true);
    } catch (error) {
      captureError('stop', error);
      update('stop', false);
    } finally {
      startedRef.current = false;
    }
  }, [captureError, ensureStarted, markStep, stopSync, update]);

  const runLifecycle = useCallback(async () => {
    try {
      setLastError(null);
      markStep('lifecycle:start');
      await ensureStarted();
      markStep('lifecycle:started');
      await client.enterBackground();
      markStep('lifecycle:background');
      await client.enterForeground();
      markStep('lifecycle:foreground');
      await client.syncNow();
      const resumedStatus = await client.getSyncStatus();
      if (resumedStatus.status !== 'ready') {
        throw new Error(`foreground did not resume ready state: ${resumedStatus.status}`);
      }
      await stopSync();
      const stoppedStatus = await client.getSyncStatus();
      if (stoppedStatus.status !== 'stopped') {
        throw new Error(`stop did not drain to stopped state: ${stoppedStatus.status}`);
      }
      markStep('lifecycle:stopped');
      update('lifecycle', true);
    } catch (error) {
      captureError('lifecycle', error);
      update('lifecycle', false);
    } finally {
      startedRef.current = false;
    }
  }, [captureError, client, ensureStarted, markStep, stopSync, update]);

  const runErrorMap = useCallback(async () => {
    try {
      await ensureInitialized();
      await client.query('SELECT * FROM nonexistent_table_xyz');
      update('errorMap', false);
    } catch (error: any) {
      update('errorMap', typeof error?.code === 'string' && error.code.length > 0);
    }
  }, [client, ensureInitialized, update]);

  const runOfflineFirst = useCallback(async () => {
    const runID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const dbPath = `synchro-offline-first-${runID}.db`;
    const clientID = `rn-offline-first-device-${runID}`;
    const customerID = uuid();
    let offlineClient: SynchroClient | null = null;
    let syncClient: SynchroClient | null = null;
    try {
      setLastError(null);
      markStep('offlineFirst:offline');
      await releaseHarnessClient();
      offlineClient = new SynchroClient({
        dbPath,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID,
        appVersion: '1.0.0',
        syncInterval: TEST_SYNC_INTERVAL_SECONDS,
        pushDebounce: TEST_PUSH_DEBOUNCE_SECONDS,
        seedDatabasePath: 'seed.db',
      });
      await offlineClient.initialize();
      await offlineClient.execute(
        "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
        [customerID, USER1_ID, 'offline-first-customer', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z']
      );
      const pendingBeforeRestart = await offlineClient.pendingChangeCount();
      const offlineRow = await offlineClient.queryOne(
        'SELECT name FROM customers WHERE id = ?',
        [customerID]
      );
      await offlineClient.close();
      offlineClient = null;

      markStep('offlineFirst:first-connect');
      syncClient = new SynchroClient({
        dbPath,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID,
        appVersion: '1.0.0',
        syncInterval: TEST_SYNC_INTERVAL_SECONDS,
        pushDebounce: TEST_PUSH_DEBOUNCE_SECONDS,
      });
      await syncClient.initialize();
      await syncClient.start();
      await syncClient.syncNow();

      const pendingAfterSync = await syncClient.pendingChangeCount();
      const localRow = await syncClient.queryOne(
        'SELECT name FROM customers WHERE id = ?',
        [customerID]
      );
      const rejectedAfterSync = await syncClient.inspectRejectedMutations();

      update(
        'offlineFirst',
        pendingBeforeRestart === 1 &&
          offlineRow?.name === 'offline-first-customer' &&
          pendingAfterSync === 0 &&
          localRow?.name === 'offline-first-customer' &&
          rejectedAfterSync.length === 0
      );
    } catch (error) {
      captureError('offlineFirst', error);
      update('offlineFirst', false);
    } finally {
      await releaseClient(offlineClient);
      await releaseClient(syncClient);
    }
  }, [captureError, markStep, releaseHarnessClient, update]);

  const runSeedInit = useCallback(async () => {
    const seedID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    let seedClient: SynchroClient | null = null;
    try {
      await releaseHarnessClient();
      const seededCategoryID = '10000000-0000-0000-0000-000000000006';
      const insertedCategoryID = uuid();
      seedClient = new SynchroClient({
        dbPath: `synchro-seed-test-${seedID}.db`,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID: `rn-seed-device-${seedID}`,
        appVersion: '1.0.0',
        seedDatabasePath: 'seed.db',
      });
      await seedClient.initialize();

      const initialStatus = await seedClient.getSyncStatus();
      const seededRow = await seedClient.queryOne(
        'SELECT id, name FROM categories WHERE id = ?',
        [seededCategoryID]
      );

      // Insert into a seeded synced table to prove local CDC is installed.
      await seedClient.execute(
        "INSERT INTO categories (id, name, sort_order, created_at, updated_at) VALUES (?, ?, ?, datetime('now'), datetime('now'))",
        [insertedCategoryID, 'Seed Init Category', 999]
      );

      const pending = (await seedClient.inspectPendingMutations()).find(
        (mutation) =>
          mutation.tableName === 'categories' && mutation.recordID === insertedCategoryID
      );
      const seedInitOK =
        (initialStatus.status === 'uninitialized' || initialStatus.status === 'local_ready') &&
        seededRow?.id === seededCategoryID &&
        seededRow?.name === 'Seed Category' &&
        pending?.tableName === 'categories' &&
        pending?.operation === 'insert';

      if (!seedInitOK) {
        setLastError(
          JSON.stringify({
            initialStatus,
            seededRow,
            pending,
          })
        );
      }

      update('seedInit', seedInitOK);
    } catch {
      update('seedInit', false);
    } finally {
      await releaseClient(seedClient);
    }
  }, [releaseHarnessClient, update]);

  const runSeedResume = useCallback(async () => {
    const seedID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    let seedClient: SynchroClient | null = null;
    let unsubscribe: (() => void) | null = null;
    try {
      await releaseHarnessClient();
      const seededCategoryID = '10000000-0000-0000-0000-000000000006';
      seedClient = new SynchroClient({
        dbPath: `synchro-seed-resume-${seedID}.db`,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID: `rn-seed-resume-device-${seedID}`,
        appVersion: '1.0.0',
        syncInterval: TEST_SYNC_INTERVAL_SECONDS,
        pushDebounce: TEST_PUSH_DEBOUNCE_SECONDS,
        seedDatabasePath: 'seed.db',
      });
      await seedClient.initialize();

      const initialStatus = await seedClient.getSyncStatus();
      const initialRow = await seedClient.queryOne(
        'SELECT name FROM categories WHERE id = ?',
        [seededCategoryID]
      );

      const syncEvents: SyncEvent[] = [];
      unsubscribe = seedClient.onSyncEvent((event) => syncEvents.push(event));
      await seedClient.start();
      await seedClient.syncNow();

      unsubscribe();
      unsubscribe = null;
      const resumedStatus = await seedClient.getSyncStatus();
      const resumedRow = await seedClient.queryOne(
        'SELECT name FROM categories WHERE id = ?',
        [seededCategoryID]
      );
      const pendingCount = await seedClient.pendingChangeCount();
      const rejectedMutations = await seedClient.inspectRejectedMutations();
      const rebuiltSharedScope = syncEvents.some(
        (event) =>
          (event.type === 'rebuild_requested' || event.type === 'rebuild_completed') &&
          event.scopeID === 'global'
      );
      const resumeOK =
        (initialStatus.status === 'uninitialized' || initialStatus.status === 'local_ready') &&
        resumedStatus.status === 'ready' &&
        initialRow?.name === 'Seed Category' &&
        resumedRow?.name === 'Seed Category' &&
        pendingCount === 0 &&
        rejectedMutations.length === 0 &&
        !rebuiltSharedScope;

      if (!resumeOK) {
        setLastError(
          JSON.stringify({
            initialStatus,
            resumedStatus,
            initialRow,
            resumedRow,
            pendingCount,
            rejectedMutations,
            syncEvents,
          })
        );
      }

      update('seedResume', resumeOK);
    } catch {
      update('seedResume', false);
    } finally {
      unsubscribe?.();
      await releaseClient(seedClient);
    }
  }, [releaseHarnessClient, update]);

  const runSeedCorrupt = useCallback(async () => {
    const seedID = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const dbPath = `synchro-seed-corrupt-${seedID}.db`;
    let corruptClient: SynchroClient | null = null;
    let recoveryClient: SynchroClient | null = null;
    try {
      setLastError(null);
      await releaseHarnessClient();
      markStep('seedCorrupt:start');
      corruptClient = new SynchroClient({
        dbPath,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID: `rn-seed-corrupt-device-${seedID}`,
        appVersion: '1.0.0',
        seedDatabasePath: 'corrupt-seed.db',
      });
      let initializationCode: string | null = null;
      try {
        await corruptClient.initialize();
      } catch (error: any) {
        initializationCode = typeof error?.code === 'string' ? error.code : null;
      }
      if (initializationCode !== 'INVALID_SEED') {
        throw new Error(`corrupt seed did not fail with INVALID_SEED: ${initializationCode ?? 'none'}`);
      }
      markStep('seedCorrupt:invalid-seed');

      recoveryClient = new SynchroClient({
        dbPath,
        serverURL: SYNCHRO_TEST_URL,
        authProvider: async () => USER1_JWT,
        clientID: `rn-seed-corrupt-recovery-${seedID}`,
        appVersion: '1.0.0',
      });
      await recoveryClient.initialize();
      markStep('seedCorrupt:recovery');

      let categoriesAvailable = true;
      try {
        await recoveryClient.query('SELECT 1 FROM categories LIMIT 1');
      } catch {
        categoriesAvailable = false;
      }
      const recoveryStatus = await recoveryClient.getSyncStatus();
      const pendingCount = await recoveryClient.pendingChangeCount();
      await recoveryClient.createTable('seed_corrupt_probe', [
        { name: 'id', type: 'TEXT', primaryKey: true },
        { name: 'value', type: 'TEXT' },
      ]);
      const writeResult = await recoveryClient.execute(
        'INSERT INTO seed_corrupt_probe (id, value) VALUES (?, ?)',
        ['probe', 'ordinary-local-sql']
      );
      const probeRow = await recoveryClient.queryOne(
        'SELECT value FROM seed_corrupt_probe WHERE id = ?',
        ['probe']
      );
      const recoveryOK =
        (recoveryStatus.status === 'uninitialized' || recoveryStatus.status === 'local_ready') &&
        !categoriesAvailable &&
        pendingCount === 0 &&
        writeResult.rowsAffected === 1 &&
        probeRow?.value === 'ordinary-local-sql';

      if (!recoveryOK) {
        setLastError(
          JSON.stringify({
            initializationCode,
            recoveryStatus,
            categoriesAvailable,
            pendingCount,
            writeResult,
            probeRow,
          })
        );
      }

      update('seedCorrupt', recoveryOK);
    } catch (error) {
      captureError('seedCorrupt', error);
      update('seedCorrupt', false);
    } finally {
      await releaseClient(corruptClient);
      await releaseClient(recoveryClient);
    }
  }, [captureError, markStep, releaseHarnessClient, update]);

  return (
    <SafeAreaView style={styles.container}>
      <TouchableOpacity
        style={[styles.button, styles.resetButton]}
        onPress={() => {
          void resetHarness();
        }}
        testID="btn-reset"
      >
        <Text>Reset Harness</Text>
      </TouchableOpacity>
      <ScrollView
        key={harnessGeneration}
        testID="test-scroll"
        style={styles.scroller}
        contentContainerStyle={styles.scroll}
      >
        <Text style={styles.header} testID="header">
          Synchro RN Test Harness
        </Text>

        <View style={styles.statusRow} testID="sync-status">
          <Text>Status</Text>
          <Text testID="status-value">{displayStatus}</Text>
        </View>
        <Text testID="step-value">{currentStep}</Text>
        <Text testID="error-value">{lastError ?? 'none'}</Text>
        <Text testID="conflict-record-id">{pendingConflictRecordID ?? 'none'}</Text>
        <Text testID="multi-user-record-id">{pendingMultiUserRecordID ?? 'none'}</Text>

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
          <TouchableOpacity style={styles.button} onPress={runLifecycle} testID="btn-lifecycle">
            <Text>Lifecycle</Text>
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
          <TouchableOpacity style={styles.button} onPress={runOfflineFirst} testID="btn-offlineFirst">
            <Text>Offline First</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runSeedInit} testID="btn-seedInit">
            <Text>Seed Init</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runSeedResume} testID="btn-seedResume">
            <Text>Seed Resume</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.button} onPress={runSeedCorrupt} testID="btn-seedCorrupt">
            <Text>Seed Corrupt</Text>
          </TouchableOpacity>
        </View>

        {Object.entries(results).map(([key, value]) => (
          <StatusBadge key={key} label={key} ok={value} />
        ))}
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
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#fff' },
  scroller: { flex: 1 },
  resetButton: { marginHorizontal: 20, marginTop: 20 },
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
    minHeight: 44,
    padding: 12,
    borderRadius: 6,
    marginBottom: 8,
    alignItems: 'center',
  },
});
