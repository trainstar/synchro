import type { TurboModule } from 'react-native';
import type { EventEmitter } from 'react-native/Libraries/Types/CodegenTypes';
import { TurboModuleRegistry } from 'react-native';

type StatusEvent = { status: string; retryAt: string | null };
type ConflictEvent = {
  table: string;
  recordID: string;
  clientDataJson: string | null;
  serverDataJson: string | null;
};
type AuthRequestEvent = { requestID: string };
type ChangeEvent = { observerID: string };
type QueryResultEvent = { observerID: string; rowsJson: string };

export interface Spec extends TurboModule {
  // -- Lifecycle --
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

  // -- Core SQL --
  query(sql: string, paramsJson: string): Promise<string>;
  queryOne(sql: string, paramsJson: string): Promise<string | null>;
  execute(
    sql: string,
    paramsJson: string
  ): Promise<{ rowsAffected: number }>;
  executeBatch(
    statementsJson: string
  ): Promise<{ totalRowsAffected: number }>;

  // -- Transactions --
  beginWriteTransaction(): Promise<string>;
  beginReadTransaction(): Promise<string>;
  txQuery(txID: string, sql: string, paramsJson: string): Promise<string>;
  txQueryOne(
    txID: string,
    sql: string,
    paramsJson: string
  ): Promise<string | null>;
  txExecute(
    txID: string,
    sql: string,
    paramsJson: string
  ): Promise<{ rowsAffected: number }>;
  commitTransaction(txID: string): Promise<void>;
  rollbackTransaction(txID: string): Promise<void>;

  // -- Schema --
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

  // -- Observation --
  addChangeObserver(
    observerID: string,
    tables: ReadonlyArray<string>
  ): Promise<void>;
  addQueryObserver(
    observerID: string,
    sql: string,
    paramsJson: string,
    tables: ReadonlyArray<string>
  ): Promise<void>;
  removeObserver(observerID: string): Promise<void>;

  // -- Sync --
  checkpoint(mode: string): Promise<void>;
  start(): Promise<void>;
  stop(): Promise<void>;
  syncNow(): Promise<void>;
  pendingChangeCount(): Promise<number>;

  // -- Bidirectional responses --
  resolveAuthRequest(requestID: string, token: string): void;
  rejectAuthRequest(requestID: string, error: string): void;

  // -- Typed events --
  readonly onStatusChange: EventEmitter<StatusEvent>;
  readonly onConflict: EventEmitter<ConflictEvent>;
  readonly onAuthRequest: EventEmitter<AuthRequestEvent>;
  readonly onChange: EventEmitter<ChangeEvent>;
  readonly onQueryResult: EventEmitter<QueryResultEvent>;

  // -- NativeEventEmitter compatibility --
  addListener(eventName: string): void;
  removeListeners(count: number): void;
}

export default TurboModuleRegistry.getEnforcing<Spec>('SynchroModule');
