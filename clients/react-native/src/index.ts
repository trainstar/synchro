export { SynchroClient } from './SynchroClient';
export { useQuery } from './hooks/useQuery';
export { useSyncStatus } from './hooks/useSyncStatus';
export { usePendingChanges } from './hooks/usePendingChanges';

export type {
  Row,
  ExecResult,
  BatchResult,
  SQLiteBytes,
  SQLiteInt64,
  SQLiteTaggedValue,
  SQLiteBindValue,
  SQLStatement,
  ColumnDef,
  TableOptions,
  ReadTransaction,
  Transaction,
  SyncStatus,
  SyncStatusType,
  SyncOperationKind,
  SyncRecoveryAction,
  SyncFailureCode,
  SyncFailure,
  JSONValue,
  MutationOperation,
  LocalMutationStatus,
  MutationStatus,
  MutationRejectionCode,
  SchemaRef,
  AuthoredMutationField,
  PendingMutationInspection,
  RejectedMutationInspection,
  SchemaAction,
  SyncSchemaEvent,
  SyncMutationEvent,
  SyncRebuildEvent,
  SyncEvent,
  ProtocolErrorCode,
  ConflictEvent,
  SynchroConfig,
  Unsubscribe,
  AsyncUnsubscribe,
} from './types';

export {
  SYNC_STATUS_TYPES,
  SYNC_OPERATION_KINDS,
  SYNC_RECOVERY_ACTIONS,
  SYNC_FAILURE_CODES,
  PROTOCOL_ERROR_CODES,
} from './types';

export {
  SynchroError,
  NotConnectedError,
  SchemaNotLoadedError,
  TableNotSyncedError,
  UpgradeRequiredError,
  SchemaMismatchError,
  PushRejectedError,
  NetworkError,
  ServerError,
  ProtocolError,
  SyncBlockedError,
  UnsupportedSchemaError,
  InvalidStateTransitionError,
  DatabaseError,
  InvalidResponseError,
  InvalidSeedError,
  AlreadyStartedError,
  NotStartedError,
  TransactionTimeoutError,
  NATIVE_ERROR_CODES,
  mapNativeError,
} from './errors';

export type {
  NativeErrorCode,
  SchemaUnsupportedReason,
  PushRejectedMutation,
} from './errors';
