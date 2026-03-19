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
  seedDatabasePath?: string;
}

export type Unsubscribe = () => void;

// -- Sync Protocol Types --
// These mirror the Go server and native SDK models.
// JSON keys use snake_case on the wire; TypeScript uses camelCase.

export interface PullRequest {
  clientId: string;
  checkpoint: number;
  bucketCheckpoints?: Record<string, number>;
  tables?: string[];
  limit?: number;
  knownBuckets?: string[];
  schemaVersion: number;
  schemaHash: string;
}

export interface PullResponse {
  changes: SyncRecord[];
  deletes: DeleteEntry[];
  checkpoint: number;
  bucketCheckpoints?: Record<string, number>;
  hasMore: boolean;
  rebuildBuckets?: string[];
  bucketChecksums?: Record<string, number>;
  bucketUpdates?: BucketUpdate;
  schemaVersion: number;
  schemaHash: string;
}

export interface SyncRecord {
  id: string;
  tableName: string;
  data: Record<string, unknown>;
  updatedAt: string;
  deletedAt?: string;
  bucketId?: string;
  checksum?: number;
}

export interface DeleteEntry {
  id: string;
  tableName: string;
}

export interface BucketUpdate {
  added?: string[];
  removed?: string[];
}

export interface RegisterResponse {
  id: string;
  serverTime: string;
  lastSyncAt?: string;
  checkpoint: number;
  bucketCheckpoints?: Record<string, number>;
  schemaVersion: number;
  schemaHash: string;
}

export interface RebuildRequest {
  clientId: string;
  bucketId: string;
  cursor?: string;
  limit?: number;
  schemaVersion: number;
  schemaHash: string;
}

export interface RebuildResponse {
  records: SyncRecord[];
  cursor?: string;
  checkpoint: number;
  hasMore: boolean;
  bucketChecksum?: number;
  schemaVersion: number;
  schemaHash: string;
}

// -- Debug Types --

export interface SynchroDebugInfo {
  clientID: string;
  buckets: BucketDebugInfo[];
  lastSyncCheckpoint: number;
  schemaVersion: number;
  schemaHash: string;
  pendingChangeCount: number;
  generatedAt: string;
}

export interface BucketDebugInfo {
  bucketID: string;
  checkpoint: number;
  memberCount: number;
  checksum: number;
}
