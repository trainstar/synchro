import {
  mapNativeError,
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
  parseSyncFailure,
} from '../src/errors';

import { resetNativeModuleMockState } from './__mocks__/react-native';

beforeEach(() => {
  resetNativeModuleMockState();
});

describe('mapNativeError', () => {
  it('maps NOT_CONNECTED', () => {
    const err = mapNativeError({ code: 'NOT_CONNECTED', message: 'Not connected' });
    expect(err).toBeInstanceOf(NotConnectedError);
    expect(err.code).toBe('NOT_CONNECTED');
  });

  it('maps SCHEMA_NOT_LOADED', () => {
    const err = mapNativeError({ code: 'SCHEMA_NOT_LOADED', message: '' });
    expect(err).toBeInstanceOf(SchemaNotLoadedError);
  });

  it('maps TABLE_NOT_SYNCED with table name', () => {
    const err = mapNativeError({
      code: 'TABLE_NOT_SYNCED',
      message: '',
      userInfo: { table: 'users' },
    });
    expect(err).toBeInstanceOf(TableNotSyncedError);
    expect((err as TableNotSyncedError).table).toBe('users');
  });

  it('maps UPGRADE_REQUIRED with version info', () => {
    const err = mapNativeError({
      code: 'UPGRADE_REQUIRED',
      message: '',
      userInfo: { currentVersion: '1.0.0', minimumVersion: '2.0.0' },
    });
    expect(err).toBeInstanceOf(UpgradeRequiredError);
    const typed = err as UpgradeRequiredError;
    expect(typed.currentVersion).toBe('1.0.0');
    expect(typed.minimumVersion).toBe('2.0.0');
  });

  it('maps SCHEMA_MISMATCH with server info', () => {
    const err = mapNativeError({
      code: 'SCHEMA_MISMATCH',
      message: '',
      userInfo: { serverVersion: '5', serverHash: 'abc' },
    });
    expect(err).toBeInstanceOf(SchemaMismatchError);
    const typed = err as SchemaMismatchError;
    expect(typed.serverVersion).toBe(5);
    expect(typed.serverHash).toBe('abc');
  });

  it('maps PUSH_REJECTED with results', () => {
    const results = [
      {
        mutationID: 'm1',
        table: 'items',
        pk: { id: 'r1' },
        status: 'conflict',
        code: 'version_conflict',
        message: 'server version is newer',
        serverRow: { id: 'r1', name: 'server' },
        serverVersion: '2026-03-20T18:22:11Z',
      },
    ];
    const err = mapNativeError({
      code: 'PUSH_REJECTED',
      message: '',
      userInfo: { results: JSON.stringify(results) },
    });
    expect(err).toBeInstanceOf(PushRejectedError);
    expect((err as PushRejectedError).results).toEqual(results);
  });

  it('maps NETWORK_ERROR using userInfo.message (no double prefix)', () => {
    const err = mapNativeError({
      code: 'NETWORK_ERROR',
      message: 'Network error: timeout',
      userInfo: { message: 'timeout' },
    });
    expect(err).toBeInstanceOf(NetworkError);
    expect(err.message).toBe('Network error: timeout');
  });

  it('maps SERVER_ERROR using userInfo.message (no double prefix)', () => {
    const err = mapNativeError({
      code: 'SERVER_ERROR',
      message: 'Server error 502: bad gateway',
      userInfo: { status: '502', message: 'bad gateway' },
    });
    expect(err).toBeInstanceOf(ServerError);
    expect((err as ServerError).status).toBe(502);
    expect(err.message).toBe('Server error 502: bad gateway');
  });

  it('maps PROTOCOL_ERROR with its canonical protocol code', () => {
    const err = mapNativeError({
      code: 'PROTOCOL_ERROR',
      message: 'Protocol error',
      userInfo: {
        status: '422',
        protocolCode: 'invalid_schema_reference',
        message: 'schema reference is invalid',
      },
    });
    expect(err).toBeInstanceOf(ProtocolError);
    expect(err).toMatchObject({
      code: 'PROTOCOL_ERROR',
      status: 422,
      protocolCode: 'invalid_schema_reference',
    });
  });

  it('maps SYNC_BLOCKED and preserves recovery metadata', () => {
    const err = mapNativeError({
      code: 'SYNC_BLOCKED',
      message: 'sync is blocked',
      userInfo: {
        failure: JSON.stringify({
          operation: 'schema',
          code: 'unsupported_schema',
          retryable: false,
          message: 'reset required',
          recoveryAction: 'schema_reset',
          metadata: { reason: 'incompatible_schema_transition' },
        }),
      },
    });
    expect(err).toBeInstanceOf(SyncBlockedError);
    expect((err as SyncBlockedError).failure).toEqual({
      operation: 'schema',
      code: 'unsupported_schema',
      retryable: false,
      message: 'reset required',
      recoveryAction: 'schema_reset',
      metadata: { reason: 'incompatible_schema_transition' },
    });
  });

  it('maps unsupported schema and invalid state transitions', () => {
    const unsupported = mapNativeError({
      code: 'UNSUPPORTED_SCHEMA',
      userInfo: { reason: 'unknown_schema_lineage' },
    });
    const transition = mapNativeError({
      code: 'INVALID_STATE_TRANSITION',
      userInfo: { from: 'ready', to: 'connecting' },
    });

    expect(unsupported).toBeInstanceOf(UnsupportedSchemaError);
    expect((unsupported as UnsupportedSchemaError).reason).toBe('unknown_schema_lineage');
    expect(transition).toBeInstanceOf(InvalidStateTransitionError);
    expect(transition).toMatchObject({ from: 'ready', to: 'connecting' });
  });

  it('maps DATABASE_ERROR using userInfo.message (no double prefix)', () => {
    const err = mapNativeError({
      code: 'DATABASE_ERROR',
      message: 'Database error: SQLITE_CONSTRAINT',
      userInfo: { message: 'SQLITE_CONSTRAINT' },
    });
    expect(err).toBeInstanceOf(DatabaseError);
    expect(err.message).toBe('Database error: SQLITE_CONSTRAINT');
  });

  it('maps INVALID_RESPONSE using userInfo.message (no double prefix)', () => {
    const err = mapNativeError({
      code: 'INVALID_RESPONSE',
      message: 'Invalid response: bad json',
      userInfo: { message: 'bad json' },
    });
    expect(err).toBeInstanceOf(InvalidResponseError);
    expect(err.message).toBe('Invalid response: bad json');
  });

  it('maps INVALID_SEED to a typed initialization error', () => {
    const err = mapNativeError({ code: 'INVALID_SEED', message: 'invalid seed' });
    expect(err).toBeInstanceOf(InvalidSeedError);
    expect(err.code).toBe('INVALID_SEED');
  });

  it('maps ALREADY_STARTED', () => {
    const err = mapNativeError({ code: 'ALREADY_STARTED', message: '' });
    expect(err).toBeInstanceOf(AlreadyStartedError);
  });

  it('maps NOT_STARTED', () => {
    const err = mapNativeError({ code: 'NOT_STARTED', message: '' });
    expect(err).toBeInstanceOf(NotStartedError);
  });

  it('maps TRANSACTION_TIMEOUT', () => {
    const err = mapNativeError({ code: 'TRANSACTION_TIMEOUT', message: '' });
    expect(err).toBeInstanceOf(TransactionTimeoutError);
  });

  it('returns SynchroError for unknown codes', () => {
    const err = mapNativeError({ code: 'WEIRD_ERROR', message: 'something' });
    expect(err).toBeInstanceOf(SynchroError);
    expect(err.code).toBe('UNKNOWN');
  });

  it('passes through existing SynchroError instances', () => {
    const original = new NotConnectedError();
    const err = mapNativeError(original);
    expect(err).toBe(original);
  });
});

describe('parseSyncFailure', () => {
  it('rejects the unreachable retry_exhausted failure code', () => {
    expect(() => parseSyncFailure({
      operation: 'pulling',
      code: 'retry_exhausted',
      retryable: true,
      message: 'retry attempts ended',
      recoveryAction: 'retry',
      metadata: {},
    })).toThrow('invalid sync failure code');
  });
});
