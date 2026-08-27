import {
  PROTOCOL_ERROR_CODES,
  SYNC_FAILURE_CODES,
  SYNC_OPERATION_KINDS,
  SYNC_RECOVERY_ACTIONS,
  SYNC_STATUS_TYPES,
} from './types';
import type {
  ProtocolErrorCode,
  Row,
  SyncFailure,
  SyncStatusType,
} from './types';

export const NATIVE_ERROR_CODES = [
  'NOT_CONNECTED',
  'SCHEMA_NOT_LOADED',
  'TABLE_NOT_SYNCED',
  'UPGRADE_REQUIRED',
  'SCHEMA_MISMATCH',
  'PUSH_REJECTED',
  'NETWORK_ERROR',
  'SERVER_ERROR',
  'PROTOCOL_ERROR',
  'DATABASE_ERROR',
  'INVALID_RESPONSE',
  'INVALID_SEED',
  'SYNC_BLOCKED',
  'UNSUPPORTED_SCHEMA',
  'INVALID_STATE_TRANSITION',
  'ALREADY_STARTED',
  'NOT_STARTED',
  'TRANSACTION_TIMEOUT',
  'INVALID_CONFIG',
  'CLIENT_ALREADY_ACTIVE',
  'UNKNOWN',
] as const;

export type NativeErrorCode = (typeof NATIVE_ERROR_CODES)[number];

export class SynchroError extends Error {
  readonly code: NativeErrorCode;

  constructor(code: NativeErrorCode, message: string) {
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

export interface PushRejectedMutation {
  mutationID: string;
  table: string;
  pk: Row;
  status: string;
  code: string;
  message?: string;
  serverRow?: Row | null;
  serverVersion?: string | null;
}

export class PushRejectedError extends SynchroError {
  readonly results: PushRejectedMutation[];

  constructor(results: PushRejectedMutation[]) {
    super('PUSH_REJECTED', `Push rejected: ${results.length} mutation(s)`);
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

export const SCHEMA_UNSUPPORTED_REASONS = [
  'unknown_schema_lineage',
  'incompatible_schema_transition',
] as const;

export type SchemaUnsupportedReason = (typeof SCHEMA_UNSUPPORTED_REASONS)[number];

export class ProtocolError extends SynchroError {
  readonly status: number;
  readonly protocolCode: ProtocolErrorCode;

  constructor(status: number, protocolCode: ProtocolErrorCode, message: string) {
    super('PROTOCOL_ERROR', `Protocol error ${status} ${protocolCode}: ${message}`);
    this.name = 'ProtocolError';
    this.status = status;
    this.protocolCode = protocolCode;
  }
}

export class SyncBlockedError extends SynchroError {
  readonly failure: SyncFailure;

  constructor(failure: SyncFailure) {
    super('SYNC_BLOCKED', failure.message);
    this.name = 'SyncBlockedError';
    this.failure = failure;
  }
}

export class UnsupportedSchemaError extends SynchroError {
  readonly reason: SchemaUnsupportedReason;

  constructor(reason: SchemaUnsupportedReason) {
    super('UNSUPPORTED_SCHEMA', `Schema recovery is required: ${reason}`);
    this.name = 'UnsupportedSchemaError';
    this.reason = reason;
  }
}

export class InvalidStateTransitionError extends SynchroError {
  readonly from: SyncStatusType;
  readonly to: SyncStatusType;

  constructor(from: SyncStatusType, to: SyncStatusType) {
    super('INVALID_STATE_TRANSITION', `Invalid sync state transition from ${from} to ${to}`);
    this.name = 'InvalidStateTransitionError';
    this.from = from;
    this.to = to;
  }
}

export class InvalidSeedError extends SynchroError {
  constructor() {
    super('INVALID_SEED', 'Seed database failed validation');
    this.name = 'InvalidSeedError';
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
  userInfo?: Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseJSONValue(value: unknown, name: string): unknown {
  if (typeof value !== 'string') {
    return value;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new InvalidResponseError(`Native bridge returned invalid ${name}`);
  }
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
  }
  return value;
}

function requiredBoolean(value: unknown, name: string): boolean {
  if (typeof value === 'boolean') {
    return value;
  }
  if (value === 'true') {
    return true;
  }
  if (value === 'false') {
    return false;
  }
  throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
}

function optionalString(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function requiredEnum<T extends string>(value: unknown, values: readonly T[], name: string): T {
  if (typeof value === 'string' && values.includes(value as T)) {
    return value as T;
  }
  throw new InvalidResponseError(`Native bridge returned an invalid ${name}`);
}

export function parseSyncFailure(value: unknown): SyncFailure {
  const parsed = parseJSONValue(value, 'sync failure');
  if (!isRecord(parsed)) {
    throw new InvalidResponseError('Native bridge returned an invalid sync failure');
  }

  const message = requiredString(parsed.message, 'sync failure message');
  if (message.length > 256) {
    throw new InvalidResponseError('Native bridge returned an invalid sync failure message');
  }

  const metadataValue = parsed.metadata == null ? {} : parseJSONValue(parsed.metadata, 'sync failure metadata');
  if (!isRecord(metadataValue) || Object.keys(metadataValue).length > 8) {
    throw new InvalidResponseError('Native bridge returned invalid sync failure metadata');
  }
  const metadata: Record<string, string> = {};
  Object.entries(metadataValue).forEach(([key, item]) => {
    if (key.length === 0 || key.length > 64 || typeof item !== 'string' || item.length > 128) {
      throw new InvalidResponseError('Native bridge returned invalid sync failure metadata');
    }
    metadata[key] = item;
  });

  return {
    operation: requiredEnum(parsed.operation, SYNC_OPERATION_KINDS, 'sync failure operation'),
    code: requiredEnum(parsed.code, SYNC_FAILURE_CODES, 'sync failure code'),
    retryable: requiredBoolean(parsed.retryable, 'sync failure retry flag'),
    message,
    recoveryAction: requiredEnum(
      parsed.recoveryAction,
      SYNC_RECOVERY_ACTIONS,
      'sync failure recovery action'
    ),
    metadata,
  };
}

function syncFailureFromUserInfo(userInfo: Record<string, unknown>, fallbackMessage: string): SyncFailure {
  if (userInfo.failure != null) {
    return parseSyncFailure(userInfo.failure);
  }
  return parseSyncFailure({
    operation: userInfo.failureOperation,
    code: userInfo.failureCode,
    retryable: userInfo.failureRetryable,
    message: userInfo.failureMessage ?? fallbackMessage,
    recoveryAction: userInfo.failureRecoveryAction,
    metadata: userInfo.failureMetadata ?? {},
  });
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
      return new TableNotSyncedError(optionalString(userInfo.table));
    case 'UPGRADE_REQUIRED':
      return new UpgradeRequiredError(
        optionalString(userInfo.currentVersion),
        optionalString(userInfo.minimumVersion)
      );
    case 'SCHEMA_MISMATCH':
      return new SchemaMismatchError(
        parseInt(optionalString(userInfo.serverVersion), 10),
        optionalString(userInfo.serverHash)
      );
    case 'PUSH_REJECTED': {
      try {
        const parsed = parseJSONValue(userInfo.results ?? [], 'push rejection results');
        if (!Array.isArray(parsed)) {
          return new InvalidResponseError('Native bridge returned invalid push rejection results');
        }
        return new PushRejectedError(parsed as PushRejectedMutation[]);
      } catch (parseError) {
        return parseError instanceof SynchroError
          ? parseError
          : new InvalidResponseError('Native bridge returned invalid push rejection results');
      }
    }
    case 'NETWORK_ERROR':
      return new NetworkError(typeof userInfo.message === 'string' ? userInfo.message : message);
    case 'SERVER_ERROR':
      return new ServerError(
        Number.parseInt(String(userInfo.status ?? '0'), 10),
        typeof userInfo.message === 'string' ? userInfo.message : message
      );
    case 'PROTOCOL_ERROR': {
      const status = Number.parseInt(String(userInfo.status ?? '0'), 10);
      const protocolCode = requiredEnum(
        userInfo.protocolCode,
        PROTOCOL_ERROR_CODES,
        'protocol error code'
      );
      return new ProtocolError(
        status,
        protocolCode,
        typeof userInfo.message === 'string' ? userInfo.message : message
      );
    }
    case 'DATABASE_ERROR':
      return new DatabaseError(typeof userInfo.message === 'string' ? userInfo.message : message);
    case 'INVALID_RESPONSE':
      return new InvalidResponseError(typeof userInfo.message === 'string' ? userInfo.message : message);
    case 'INVALID_SEED':
      return new InvalidSeedError();
    case 'SYNC_BLOCKED':
      try {
        return new SyncBlockedError(syncFailureFromUserInfo(userInfo, message));
      } catch (parseError) {
        return parseError instanceof SynchroError
          ? parseError
          : new InvalidResponseError('Native bridge returned invalid blocking failure details');
      }
    case 'UNSUPPORTED_SCHEMA':
      try {
        return new UnsupportedSchemaError(
          requiredEnum(userInfo.reason, SCHEMA_UNSUPPORTED_REASONS, 'unsupported schema reason')
        );
      } catch (parseError) {
        return parseError instanceof SynchroError
          ? parseError
          : new InvalidResponseError('Native bridge returned invalid unsupported schema details');
      }
    case 'INVALID_STATE_TRANSITION':
      try {
        return new InvalidStateTransitionError(
          requiredEnum(userInfo.from, SYNC_STATUS_TYPES, 'state transition source'),
          requiredEnum(userInfo.to, SYNC_STATUS_TYPES, 'state transition target')
        );
      } catch (parseError) {
        return parseError instanceof SynchroError
          ? parseError
          : new InvalidResponseError('Native bridge returned invalid state transition details');
      }
    case 'ALREADY_STARTED':
      return new AlreadyStartedError();
    case 'NOT_STARTED':
      return new NotStartedError();
    case 'TRANSACTION_TIMEOUT':
      return new TransactionTimeoutError();
    case 'INVALID_CONFIG':
      return new SynchroError('INVALID_CONFIG', message);
    default:
      return new SynchroError('UNKNOWN', message);
  }
}
