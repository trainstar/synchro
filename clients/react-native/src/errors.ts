export class SynchroError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.code = code;
    this.name = 'SynchroError';
  }
}

export class NotConnectedError extends SynchroError {
  constructor() {
    super('NOT_CONNECTED', 'Not connected to sync server');
    this.name = 'NotConnectedError';
  }
}

export class SchemaNotLoadedError extends SynchroError {
  constructor() {
    super('SCHEMA_NOT_LOADED', 'Schema has not been loaded from server');
    this.name = 'SchemaNotLoadedError';
  }
}

export class TableNotSyncedError extends SynchroError {
  readonly table: string;

  constructor(table: string) {
    super('TABLE_NOT_SYNCED', `Table '${table}' is not a synced table`);
    this.name = 'TableNotSyncedError';
    this.table = table;
  }
}

export class UpgradeRequiredError extends SynchroError {
  readonly currentVersion: string;
  readonly minimumVersion: string;

  constructor(currentVersion: string, minimumVersion: string) {
    super(
      'UPGRADE_REQUIRED',
      `App version ${currentVersion} is below minimum ${minimumVersion}`
    );
    this.name = 'UpgradeRequiredError';
    this.currentVersion = currentVersion;
    this.minimumVersion = minimumVersion;
  }
}

export class SchemaMismatchError extends SynchroError {
  readonly serverVersion: number;
  readonly serverHash: string;

  constructor(serverVersion: number, serverHash: string) {
    super(
      'SCHEMA_MISMATCH',
      `Schema mismatch: server version ${serverVersion}, hash ${serverHash}`
    );
    this.name = 'SchemaMismatchError';
    this.serverVersion = serverVersion;
    this.serverHash = serverHash;
  }
}

export class SnapshotRequiredError extends SynchroError {
  constructor() {
    super('SNAPSHOT_REQUIRED', 'Full snapshot required');
    this.name = 'SnapshotRequiredError';
  }
}

export interface PushResultItem {
  recordID: string;
  table: string;
  status: string;
  message?: string;
}

export class PushRejectedError extends SynchroError {
  readonly results: PushResultItem[];

  constructor(results: PushResultItem[]) {
    super('PUSH_REJECTED', `Push rejected: ${results.length} record(s)`);
    this.name = 'PushRejectedError';
    this.results = results;
  }
}

export class NetworkError extends SynchroError {
  constructor(message: string) {
    super('NETWORK_ERROR', `Network error: ${message}`);
    this.name = 'NetworkError';
  }
}

export class ServerError extends SynchroError {
  readonly status: number;

  constructor(status: number, message: string) {
    super('SERVER_ERROR', `Server error ${status}: ${message}`);
    this.name = 'ServerError';
    this.status = status;
  }
}

export class DatabaseError extends SynchroError {
  constructor(message: string) {
    super('DATABASE_ERROR', `Database error: ${message}`);
    this.name = 'DatabaseError';
  }
}

export class InvalidResponseError extends SynchroError {
  constructor(message: string) {
    super('INVALID_RESPONSE', `Invalid response: ${message}`);
    this.name = 'InvalidResponseError';
  }
}

export class AlreadyStartedError extends SynchroError {
  constructor() {
    super('ALREADY_STARTED', 'Sync has already been started');
    this.name = 'AlreadyStartedError';
  }
}

export class NotStartedError extends SynchroError {
  constructor() {
    super('NOT_STARTED', 'Sync has not been started');
    this.name = 'NotStartedError';
  }
}

export class TransactionTimeoutError extends SynchroError {
  constructor() {
    super('TRANSACTION_TIMEOUT', 'Transaction timed out due to inactivity');
    this.name = 'TransactionTimeoutError';
  }
}

interface NativeErrorLike {
  code?: string;
  message?: string;
  userInfo?: Record<string, string>;
}

export function mapNativeError(error: unknown): SynchroError {
  if (error instanceof SynchroError) {
    return error;
  }

  const nativeError = error as NativeErrorLike;
  const code = nativeError?.code;
  const message = nativeError?.message ?? 'Unknown error';
  const userInfo = nativeError?.userInfo ?? {};

  switch (code) {
    case 'NOT_CONNECTED':
      return new NotConnectedError();
    case 'SCHEMA_NOT_LOADED':
      return new SchemaNotLoadedError();
    case 'TABLE_NOT_SYNCED':
      return new TableNotSyncedError(userInfo.table ?? '');
    case 'UPGRADE_REQUIRED':
      return new UpgradeRequiredError(
        userInfo.currentVersion ?? '',
        userInfo.minimumVersion ?? ''
      );
    case 'SCHEMA_MISMATCH':
      return new SchemaMismatchError(
        parseInt(userInfo.serverVersion ?? '0', 10),
        userInfo.serverHash ?? ''
      );
    case 'SNAPSHOT_REQUIRED':
      return new SnapshotRequiredError();
    case 'PUSH_REJECTED': {
      let results: PushResultItem[] = [];
      try {
        results = JSON.parse(userInfo.results ?? '[]');
      } catch {
        // leave empty
      }
      return new PushRejectedError(results);
    }
    case 'NETWORK_ERROR':
      return new NetworkError(userInfo.message ?? message);
    case 'SERVER_ERROR':
      return new ServerError(
        parseInt(userInfo.status ?? '0', 10),
        userInfo.message ?? message
      );
    case 'DATABASE_ERROR':
      return new DatabaseError(userInfo.message ?? message);
    case 'INVALID_RESPONSE':
      return new InvalidResponseError(userInfo.message ?? message);
    case 'ALREADY_STARTED':
      return new AlreadyStartedError();
    case 'NOT_STARTED':
      return new NotStartedError();
    case 'TRANSACTION_TIMEOUT':
      return new TransactionTimeoutError();
    default:
      return new SynchroError(code ?? 'UNKNOWN', message);
  }
}
