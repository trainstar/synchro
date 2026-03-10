export type Row = Record<string, unknown>;

export interface ExecResult {
  rowsAffected: number;
}

export interface BatchResult {
  totalRowsAffected: number;
}

export interface SQLStatement {
  sql: string;
  params?: unknown[];
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
  query(sql: string, params?: unknown[]): Promise<Row[]>;
  queryOne(sql: string, params?: unknown[]): Promise<Row | null>;
  execute(sql: string, params?: unknown[]): Promise<ExecResult>;
}

export type CheckpointMode = 'passive' | 'full' | 'restart' | 'truncate';

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
  snapshotPageSize?: number;
}

export type Unsubscribe = () => void;
