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
  DatabaseError,
  InvalidResponseError,
  AlreadyStartedError,
  NotStartedError,
  TransactionTimeoutError,
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
    const results = [{ recordID: 'r1', table: 'items', status: 'rejected' }];
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
    expect(err.code).toBe('WEIRD_ERROR');
  });

  it('passes through existing SynchroError instances', () => {
    const original = new NotConnectedError();
    const err = mapNativeError(original);
    expect(err).toBe(original);
  });
});
