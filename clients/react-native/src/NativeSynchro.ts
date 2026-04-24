import type { TurboModule } from 'react-native';
import type {
  EventEmitter,
  UnsafeObject,
} from 'react-native/Libraries/Types/CodegenTypes';
import { TurboModuleRegistry } from 'react-native';

type SQLiteBindValue = string | number | boolean | null;
type NativeRow = UnsafeObject;
type NativeSQLStatement = {
  sql: string;
  params: ReadonlyArray<SQLiteBindValue>;
};
type StatusEvent = { status: string; retryAt: string | null };
type ConflictEvent = {
  table: string;
  recordID: string;
  clientDataJson: string | null;
  serverDataJson: string | null;
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
  }): Promise<void>;
  close(): Promise<void>;
  getPath(): Promise<string>;

  // Core SQL
  query(
    sql: string,
    params: ReadonlyArray<SQLiteBindValue>
  ): Promise<ReadonlyArray<NativeRow>>;
  queryOne(sql: string, params: ReadonlyArray<SQLiteBindValue>): Promise<NativeRow | null>;
  execute(
    sql: string,
    params: ReadonlyArray<SQLiteBindValue>
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
    params: ReadonlyArray<SQLiteBindValue>
  ): Promise<ReadonlyArray<NativeRow>>;
  txQueryOne(
    txID: string,
    sql: string,
    params: ReadonlyArray<SQLiteBindValue>
  ): Promise<NativeRow | null>;
  txExecute(
    txID: string,
    sql: string,
    params: ReadonlyArray<SQLiteBindValue>
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
    params: ReadonlyArray<SQLiteBindValue>,
    tables: ReadonlyArray<string>
  ): Promise<void>;
  removeObserver(observerID: string): Promise<void>;

  // Sync
  start(): Promise<void>;
  stop(): Promise<void>;
  syncNow(): Promise<void>;
  pendingChangeCount(): Promise<number>;

  // Bidirectional responses
  resolveAuthRequest(requestID: string, token: string): void;
  rejectAuthRequest(requestID: string, error: string): void;

  // Typed events
  readonly onStatusChange: EventEmitter<StatusEvent>;
  readonly onConflict: EventEmitter<ConflictEvent>;
  readonly onAuthRequest: EventEmitter<AuthRequestEvent>;
  readonly onChange: EventEmitter<ChangeEvent>;
  readonly onQueryResult: EventEmitter<QueryResultEvent>;

  // NativeEventEmitter compatibility
  addListener(eventName: string): void;
  removeListeners(count: number): void;
}

export default TurboModuleRegistry.getEnforcing<Spec>('SynchroModule');
