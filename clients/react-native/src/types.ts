export type Row = Record<string, unknown>;

export type SQLiteBindValue = null | string | number | boolean;

export interface ExecResult {
  rowsAffected: number;
}

export interface BatchResult {
  totalRowsAffected: number;
}

export interface SQLStatement {
  sql: string;
  params?: readonly SQLiteBindValue[];
}

export interface ColumnDef {
  name: string;
  type: string;
  nullable?: boolean;
  primaryKey?: boolean;
  defaultValue?: string;
}

export interface TableOptions {
  ifNotExists?: boolean;
  withoutRowid?: boolean;
}

export interface Transaction {
  query(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row[]>;
  queryOne(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row | null>;
  execute(sql: string, params?: readonly SQLiteBindValue[]): Promise<ExecResult>;
}

export type SyncStatusType =
  | 'idle'
  | 'connecting'
  | 'syncing'
  | 'error'
  | 'stopped';

export interface SyncStatus {
  status: SyncStatusType;
  retryAt: Date | null;
}

export interface ConflictEvent {
  table: string;
  recordID: string;
  clientData: Row | null;
  serverData: Row | null;
}

export interface SynchroConfig {
  dbPath: string;
  serverURL: string;
  authProvider: () => Promise<string>;
  clientID: string;
  platform?: string;
  appVersion: string;
  syncInterval?: number;
  pushDebounce?: number;
  maxRetryAttempts?: number;
  pullPageSize?: number;
  pushBatchSize?: number;
  seedDatabasePath?: string;
}

export type Unsubscribe = () => void;
