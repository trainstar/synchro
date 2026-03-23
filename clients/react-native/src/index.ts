export { SynchroClient } from './SynchroClient';
export { useQuery } from './hooks/useQuery';
export { useSyncStatus } from './hooks/useSyncStatus';
export { usePendingChanges } from './hooks/usePendingChanges';

export type {
  Row,
  ExecResult,
  BatchResult,
  SQLStatement,
  ColumnDef,
  TableOptions,
  Transaction,
  SyncStatus,
  SyncStatusType,
  ConflictEvent,
  SynchroConfig,
  Unsubscribe,
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
  DatabaseError,
  InvalidResponseError,
  AlreadyStartedError,
  NotStartedError,
  TransactionTimeoutError,
  mapNativeError,
} from './errors';

export type { PushRejectedMutation } from './errors';
