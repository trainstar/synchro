import type { TurboModule } from 'react-native';
import type {
  EventEmitter,
  UnsafeObject,
} from 'react-native/Libraries/Types/CodegenTypes';
import { TurboModuleRegistry } from 'react-native';

// Codegen does not support a discriminated union of object and primitive
// parameter types. UnsafeObject keeps the TurboModule boundary representable,
// while the public client validates the tagged values before crossing it.
type NativeSQLiteBindValue = string | number | boolean | null | UnsafeObject;
type NativeRow = UnsafeObject;
type NativeSQLStatement = {
  sql: string;
  params: ReadonlyArray<NativeSQLiteBindValue>;
};
type StatusEvent = {
  status: string;
  retryAt: string | null;
  operation: string | null;
  failure: UnsafeObject | null;
};
type ConflictEvent = {
  table: string;
  recordID: string;
  clientDataJson: string | null;
  serverDataJson: string | null;
};
type SyncEvent = {
  type: string;
  from: string | null;
  to: string | null;
  operation: string | null;
  attempt: number | null;
  retryAt: string | null;
  source: UnsafeObject | null;
  target: UnsafeObject | null;
  action: string | null;
  mutationID: string | null;
  tableID: string | null;
  mutationStatus: string | null;
  rejectionCode: string | null;
  scopeID: string | null;
  rebuildID: string | null;
  failure: UnsafeObject | null;
};
type AuthRequestEvent = { requestID: string };
type ChangeEvent = { observerID: string };
type QueryResultEvent = { observerID: string; rows: ReadonlyArray<NativeRow> };

export interface Spec extends TurboModule {
  // Lifecycle
  initialize(config: {
    dbPath: string;
    serverURL: string;
    clientID: string;
    platform: string;
    appVersion: string;
    syncInterval: number;
    pushDebounce: number;
    maxRetryAttempts: number;
    pullPageSize: number;
    pushBatchSize: number;
    seedDatabasePath?: string;
    transportObservationCapacity: number;
    requireNewDatabase: boolean;
  }): Promise<void>;
  close(): Promise<void>;
  getPath(): Promise<string>;

  // Core SQL
  query(
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<ReadonlyArray<NativeRow>>;
  queryOne(
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<NativeRow | null>;
  execute(
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<{ rowsAffected: number }>;
  executeBatch(
    statements: ReadonlyArray<NativeSQLStatement>
  ): Promise<{ totalRowsAffected: number }>;

  // Transactions
  beginWriteTransaction(): Promise<string>;
  beginReadTransaction(): Promise<string>;
  txQuery(
    txID: string,
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<ReadonlyArray<NativeRow>>;
  txQueryOne(
    txID: string,
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<NativeRow | null>;
  txExecute(
    txID: string,
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>
  ): Promise<{ rowsAffected: number }>;
  commitTransaction(txID: string): Promise<void>;
  rollbackTransaction(txID: string): Promise<void>;

  // Schema
  createTable(
    name: string,
    columnsJson: string,
    optionsJson: string | null
  ): Promise<void>;
  alterTable(name: string, columnsJson: string): Promise<void>;
  createIndex(
    table: string,
    columns: ReadonlyArray<string>,
    unique: boolean
  ): Promise<void>;

  // Observation
  addChangeObserver(
    observerID: string,
    tables: ReadonlyArray<string>
  ): Promise<void>;
  addQueryObserver(
    observerID: string,
    sql: string,
    params: ReadonlyArray<NativeSQLiteBindValue>,
    tables: ReadonlyArray<string>
  ): Promise<void>;
  removeObserver(observerID: string): Promise<void>;

  // Sync
  start(): Promise<void>;
  stop(): Promise<void>;
  enterBackground(): Promise<void>;
  enterForeground(): Promise<void>;
  retryAfterError(): Promise<void>;
  resetSchemaAndStart(): Promise<void>;
  syncNow(): Promise<void>;
  pendingChangeCount(): Promise<number>;
  getSyncStatus(): Promise<string>;
  inspectPendingMutations(): Promise<string>;
  inspectRejectedMutations(): Promise<string>;
  inspectClientState(): Promise<string>;
  inspectDurableState(tableName: string, recordID: string): Promise<string>;
  inspectTransportObservations(): Promise<string>;
  armTransportPause(operationClass: string): Promise<void>;
  awaitTransportPause(operationClass: string, timeoutMs: number): Promise<void>;
  resumeTransportPause(): Promise<void>;
  getProcessIdentity(): Promise<string>;
  clearRejectedMutations(): Promise<void>;

  // Bidirectional responses
  resolveAuthRequest(requestID: string, token: string): void;
  rejectAuthRequest(requestID: string, error: string): void;

  // Typed events
  readonly onStatusChange: EventEmitter<StatusEvent>;
  readonly onSyncEvent: EventEmitter<SyncEvent>;
  readonly onConflict: EventEmitter<ConflictEvent>;
  readonly onAuthRequest: EventEmitter<AuthRequestEvent>;
  readonly onChange: EventEmitter<ChangeEvent>;
  readonly onQueryResult: EventEmitter<QueryResultEvent>;

  // NativeEventEmitter compatibility
  addListener(eventName: string): void;
  removeListeners(count: number): void;
}

export default TurboModuleRegistry.getEnforcing<Spec>('SynchroModule');
