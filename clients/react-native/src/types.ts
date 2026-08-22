export type Row = Record<string, unknown>;

/** A lossless SQLite BLOB value at the React Native boundary. */
export interface SQLiteBytes {
  readonly type: 'bytes';
  readonly base64: string;
}

/** A lossless SQLite INTEGER value outside JavaScript's safe integer range. */
export interface SQLiteInt64 {
  readonly type: 'int64';
  readonly value: string;
}

export type SQLiteTaggedValue = SQLiteBytes | SQLiteInt64;

export type SQLiteBindValue = null | string | number | boolean | SQLiteTaggedValue;

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

export interface ReadTransaction {
  query(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row[]>;
  queryOne(sql: string, params?: readonly SQLiteBindValue[]): Promise<Row | null>;
}

export interface Transaction extends ReadTransaction {
  execute(sql: string, params?: readonly SQLiteBindValue[]): Promise<ExecResult>;
}

export const SYNC_STATUS_TYPES = [
  'uninitialized',
  'local_ready',
  'connecting',
  'schema_applying',
  'ready',
  'pushing',
  'pulling',
  'rebuilding',
  'backoff',
  'error',
  'stopped',
] as const;

export type SyncStatusType = (typeof SYNC_STATUS_TYPES)[number];

export const SYNC_OPERATION_KINDS = [
  'opening',
  'connecting',
  'schema',
  'pushing',
  'pulling',
  'rebuilding',
  'lifecycle',
  'database',
] as const;

export type SyncOperationKind = (typeof SYNC_OPERATION_KINDS)[number];

export const SYNC_RECOVERY_ACTIONS = ['retry', 'schema_reset', 'none'] as const;

export type SyncRecoveryAction = (typeof SYNC_RECOVERY_ACTIONS)[number];

export const SYNC_FAILURE_CODES = [
  'auth_required',
  'client_retired',
  'idempotency_conflict',
  'invalid_request',
  'invalid_response',
  'invalid_schema_reference',
  'invalid_state_transition',
  'local_database',
  'schema_application_failed',
  'sync_integrity_failure',
  'unsupported_schema',
  'upgrade_required',
  'retry_exhausted',
  'schema_mismatch',
  'server_error',
  'network_error',
  'database_error',
  'local_failure',
] as const;

export type SyncFailureCode = (typeof SYNC_FAILURE_CODES)[number];

export const PROTOCOL_ERROR_CODES = [
  'invalid_request',
  'invalid_schema_reference',
  'upgrade_required',
  'auth_required',
  'idempotency_conflict',
  'client_retired',
  'client_generation_expired',
  'rebuild_restart_required',
  'schema_mismatch',
  'retry_later',
  'sync_integrity_failure',
  'capture_pending',
  'temporary_unavailable',
] as const;

export type ProtocolErrorCode = (typeof PROTOCOL_ERROR_CODES)[number];

export interface SyncFailure {
  operation: SyncOperationKind;
  code: SyncFailureCode;
  retryable: boolean;
  message: string;
  recoveryAction: SyncRecoveryAction;
  metadata: Record<string, string>;
}

type DetailFreeSyncStatus = {
  [K in Exclude<SyncStatusType, 'backoff' | 'error'>]: {
    status: K;
    retryAt: null;
    operation: null;
    failure: null;
  };
}[Exclude<SyncStatusType, 'backoff' | 'error'>];

export type SyncStatus =
  | DetailFreeSyncStatus
  | {
      status: 'backoff';
      retryAt: Date;
      operation: SyncOperationKind;
      failure: null;
    }
  | {
      status: 'error';
      retryAt: null;
      operation: null;
      failure: SyncFailure;
    };

export type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };

export type MutationOperation = 'insert' | 'upsert' | 'update' | 'delete';

export type LocalMutationStatus =
  | 'pending'
  | 'sealed'
  | 'superseded_before_send'
  | 'cancelled_before_send'
  | 'blocked_by_predecessor';

export type MutationStatus = 'applied' | 'conflict' | 'rejected_terminal';

export type MutationRejectionCode =
  | 'version_conflict'
  | 'row_already_exists'
  | 'row_deleted'
  | 'row_not_found'
  | 'schema_incompatible'
  | 'policy_rejected'
  | 'validation_failed'
  | 'table_not_synced';

export interface SchemaRef {
  version: number;
  hash: string;
}

export interface AuthoredMutationField {
  fieldID: string;
  logicalType: string;
  value: JSONValue;
}

export interface PendingMutationInspection {
  mutationID: string;
  localOrder: number;
  tableID: string;
  tableName: string;
  recordID: string;
  primaryKeyFieldID: string;
  primaryKeyLogicalType: string;
  operation: MutationOperation;
  authoredSchema: SchemaRef;
  baseVersion: string | null;
  clientVersion: string;
  status: LocalMutationStatus;
  sourceKind: string;
  dependsOnMutationID: string | null;
  normalizedMutationID: string | null;
  sealedBatchID: string | null;
  sealedOrdinal: number | null;
  authoredFields: AuthoredMutationField[];
}

export interface RejectedMutationInspection {
  mutationID: string;
  tableName: string;
  recordID: string;
  status: MutationStatus;
  code: MutationRejectionCode;
  message: string | null;
  serverRowJSON: string | null;
  serverVersion: string | null;
  mutationJSON: string;
  rejectionJSON: string;
  createdAt: string;
  updatedAt: string;
}

export interface ScopeStateInspection {
  scopeID: string;
  cursor: string | null;
  checksum: string | null;
  localChecksum: string;
  generation: number;
}

export interface ScopeRowInspection {
  scopeID: string;
  tableName: string;
  recordID: string;
  checksum: string;
  generation: number;
}

export interface RebuildAttemptInspection {
  scopeID: string;
  rebuildID: string;
  clientGeneration: number;
  schemaVersion: number;
  schemaHash: string;
  generation: number;
  cursor: string | null;
  pageLimit: number;
}

export interface ClientStateInspection {
  schema: SchemaRef | null;
  scopeStates: ScopeStateInspection[];
  scopeRows: ScopeRowInspection[];
  rebuildAttempts: RebuildAttemptInspection[];
}

export type TransportOperationClass =
  | 'connect'
  | 'pull'
  | 'push'
  | 'checkpoint'
  | 'schemas'
  | 'rebuild'
  | 'other';

export interface TransportObservation {
  sequence: number;
  operationClass: TransportOperationClass;
  statusCode: number;
  durationNanoseconds: number;
  cursorFingerprints?: string[];
  cursorFingerprintsComplete?: boolean;
  requestFacts?: Record<string, JSONValue>;
  rebuildResponseFacts?: Record<string, JSONValue>;
  pullResponseFacts?: Record<string, JSONValue>;
}

export interface TransportObservationSnapshot {
  observations: TransportObservation[];
  overflowed: boolean;
  sequenceCheckpoint: number;
}

export type SchemaAction = 'none' | 'replace' | 'rebuild_local' | 'unsupported';

export interface SyncSchemaEvent {
  source: SchemaRef;
  target: SchemaRef;
  action: SchemaAction;
}

export interface SyncMutationEvent {
  mutationID: string;
  tableID: string;
  status: MutationStatus;
  rejectionCode: MutationRejectionCode | null;
}

export interface SyncRebuildEvent {
  scopeID: string;
  rebuildID: string;
}

export type SyncEvent =
  | {
      type: 'state_changed';
      from: SyncStatusType;
      to: SyncStatusType;
    }
  | ({ type: 'backoff' } & {
      operation: SyncOperationKind;
      attempt: number;
      retryAt: Date;
    })
  | ({ type: 'schema_applying' | 'schema_applied' } & SyncSchemaEvent)
  | ({ type: 'mutation_accepted' | 'mutation_rejected' } & SyncMutationEvent)
  | ({ type: 'rebuild_requested' | 'rebuild_completed' } & SyncRebuildEvent)
  | {
      type: 'failure';
      failure: SyncFailure;
    };

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
  /** Enables bounded transport observation and pause controls. */
  transportObservationCapacity?: number;
}

export type Unsubscribe = () => void;
export type AsyncUnsubscribe = () => Promise<void>;
