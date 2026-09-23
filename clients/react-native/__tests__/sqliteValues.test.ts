import { SynchroClient } from '../src/SynchroClient';
import type { SQLiteBindValue } from '../src/types';
import { mockNativeModule, resetNativeModuleMockState } from './__mocks__/react-native';

function makeClient(): SynchroClient {
  return new SynchroClient({
    dbPath: '/test.db',
    serverURL: 'http://localhost:8080',
    authProvider: async () => 'test-token',
    clientID: 'test-client',
    appVersion: '1.0.0',
  });
}

beforeEach(() => {
  resetNativeModuleMockState();
});

describe('typed SQLite bridge values', () => {
  it('passes null, empty bytes, nonempty bytes, and signed int64 tags unchanged', async () => {
    const params: SQLiteBindValue[] = [
      null,
      { type: 'bytes', base64: '' },
      { type: 'bytes', base64: 'AP8' },
      { type: 'int64', value: '-9223372036854775808' },
      { type: 'int64', value: '9223372036854775807' },
    ];

    const client = makeClient();
    await client.query('SELECT ?, ?, ?, ?, ?', params);

    expect(mockNativeModule.query).toHaveBeenCalledWith(
      'SELECT ?, ?, ?, ?, ?',
      params
    );
  });

  it('keeps safe integer bind values as JavaScript numbers', async () => {
    const params = [Number.MIN_SAFE_INTEGER, 0, Number.MAX_SAFE_INTEGER];

    await makeClient().execute('SELECT ?', params);

    expect(mockNativeModule.execute).toHaveBeenCalledWith('SELECT ?', params);
  });

  it('preserves tagged row values returned by native', async () => {
    const row = {
      empty: { type: 'bytes', base64: '' },
      bytes: { type: 'bytes', base64: 'AP8' },
      minimum: { type: 'int64', value: '-9223372036854775808' },
      maximum: { type: 'int64', value: '9223372036854775807' },
      safe: Number.MAX_SAFE_INTEGER,
      nullable: null,
    };
    mockNativeModule.queryOne.mockResolvedValueOnce(row);

    await expect(makeClient().queryOne('SELECT typed_values')).resolves.toEqual(row);
  });

  it.each([
    { type: 'bytes', base64: 'AP8=' },
    { type: 'bytes', base64: 'AP+8' },
    { type: 'bytes', base64: 'AB' },
    { type: 'int64', value: '01' },
    { type: 'int64', value: '-0' },
    { type: 'int64', value: '9223372036854775808' },
    { type: 'int64', value: '-9223372036854775809' },
  ])('rejects malformed tagged bind values without crossing the bridge: %p', async (value) => {
    await expect(
      makeClient().query('SELECT ?', [value] as SQLiteBindValue[])
    ).rejects.toThrow('Invalid SQL bind value');

    expect(mockNativeModule.query).not.toHaveBeenCalled();
  });
});
