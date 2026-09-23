jest.mock('detox', () => ({
  device: { terminateApp: jest.fn().mockResolvedValue(undefined) },
  by: { id: (id: string) => id },
  element: jest.fn(),
}), { virtual: true });

import {
  coordinatorConfiguration,
  coordinatorCount,
  exchange,
  pollCorpusResult,
  runCorpusCommandLoop,
} from '../example/e2e/corpus-harness';

const detox = jest.requireMock<{
  device: { terminateApp: jest.Mock };
  element: jest.Mock;
}>('detox');
const stateAttributes = jest.fn();
const resultAttributes = jest.fn();
const environment = { ...process.env };
const passed = { schema_version: 1, outcome: 'passed', result: {}, error_code: null, error_detail: null };
const failed = { schema_version: 1, outcome: 'error', result: null, error_code: 'invalid_command', error_detail: null };

beforeEach(() => {
  jest.clearAllMocks();
  stateAttributes.mockReset().mockResolvedValue({ text: 'ok' });
  resultAttributes.mockReset().mockResolvedValue({ text: JSON.stringify(passed) });
  detox.element.mockImplementation((id: string) => ({
    getAttributes: id === 'conformance-command-state' ? stateAttributes : resultAttributes,
  }));
  process.env.SYNCHRO_RN_COORDINATOR_URL = 'http://127.0.0.1:12345';
  process.env.SYNCHRO_RN_COORDINATOR_TOKEN = 'a'.repeat(43);
});

afterEach(() => {
  process.env = { ...environment };
  jest.restoreAllMocks();
  jest.useRealTimers();
});

it('terminates a faulted corpus app without waiting for network idleness', async () => {
  const failure = new Error('held request did not complete');
  await expect(runCorpusCommandLoop(async () => { throw failure; })).rejects.toBe(failure);
  expect(detox.device.terminateApp).toHaveBeenCalledTimes(1);
});

it('requires coordinator credentials and a bare HTTP origin', () => {
  expect(coordinatorConfiguration().endpoint).toBe('http://127.0.0.1:12345/exchange');
  for (const url of ['invalid', 'file:///tmp/test', 'http://user:password@localhost', 'http://localhost/path', 'http://localhost/?q=1', 'http://localhost/#hash']) {
    process.env.SYNCHRO_RN_COORDINATOR_URL = url;
    expect(() => coordinatorConfiguration()).toThrow();
  }
  process.env.SYNCHRO_RN_COORDINATOR_URL = 'https://localhost/';
  process.env.SYNCHRO_RN_COORDINATOR_TOKEN = 'invalid';
  expect(() => coordinatorConfiguration()).toThrow();
  delete process.env.SYNCHRO_RN_COORDINATOR_TOKEN;
  expect(() => coordinatorConfiguration()).toThrow();
});

it('keeps stage and exchange counts separate and enforces their bounds', () => {
  process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT = '1';
  process.env.SYNCHRO_RN_COORDINATOR_EXCHANGE_COUNT = '3';
  expect(coordinatorCount('STAGE_COUNT', 1)).toBe(1);
  expect(coordinatorCount('EXCHANGE_COUNT', 2)).toBe(3);
  expect(() => coordinatorCount('STAGE_COUNT', 2)).toThrow();
  for (const invalid of ['', '0', '-1', '1.5', 'NaN', '9007199254740992', '65']) {
    process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT = invalid;
    expect(() => coordinatorCount('STAGE_COUNT', 1, 64)).toThrow();
  }
  process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT = '64';
  expect(coordinatorCount('STAGE_COUNT', 1, 64)).toBe(64);
  delete process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT;
  expect(() => coordinatorCount('STAGE_COUNT', 1)).toThrow();
});

it('forwards result bytes without reencoding and decodes command and completion responses', async () => {
  const command = { nested: { action: 'unchanged' } };
  const fetchMock = jest.spyOn(globalThis, 'fetch')
    .mockResolvedValueOnce(new Response(JSON.stringify({ schema_version: 1, sequence: 1, state: 'command', command })))
    .mockResolvedValueOnce(new Response('{"schema_version":1,"sequence":2,"state":"complete","command":null}'));
  const raw = '{ "schema_version":1,"outcome":"passed","result":{"integer":9223372036854775807},"error_code":null,"error_detail":null }';
  const { endpoint, token } = coordinatorConfiguration();
  expect(await exchange(endpoint, token, 1, raw)).toEqual({ schema_version: 1, sequence: 1, state: 'command', command });
  const request = fetchMock.mock.calls[0][1];
  expect(request?.method).toBe('POST');
  expect(request?.headers).toEqual({ 'Content-Type': 'application/json', Authorization: `Bearer ${token}` });
  expect(request?.body).toBe(`{"schema_version":1,"sequence":1,"result":${raw}}`);
  expect(await exchange(endpoint, token, 2, 'null')).toEqual({
    schema_version: 1, sequence: 2, state: 'complete', command: null,
  });
});

it.each([
  'not JSON',
  'null',
  '[]',
  '{"schema_version":1,"sequence":1,"state":"complete"}',
  '{"schema_version":2,"sequence":1,"state":"complete","command":null}',
  '{"schema_version":1,"sequence":2,"state":"complete","command":null}',
  '{"schema_version":1,"sequence":1,"state":"other","command":null}',
  '{"schema_version":1,"sequence":1,"state":"complete","command":{}}',
  '{"schema_version":1,"sequence":1,"state":"command","command":[]}',
  '{"schema_version":1,"sequence":1,"state":"complete","command":null,"extra":true}',
])('rejects an invalid closed exchange response: %s', async (raw) => {
  jest.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(raw));
  await expect(exchange('http://localhost/exchange', 'test', 1, 'null')).rejects.toThrow();
});

it('rejects HTTP errors without exposing the response body', async () => {
  jest.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('private response', { status: 503 }));
  const failure = exchange('http://localhost/exchange', 'test', 1, 'null');
  await expect(failure).rejects.toThrow('HTTP 503');
  await expect(failure).rejects.not.toThrow('private response');
});

it('aborts a stalled exchange after 30 seconds and clears its timer', async () => {
  jest.useFakeTimers();
  const fetchMock = jest.spyOn(globalThis, 'fetch').mockImplementation((_url, options) =>
    new Promise((_resolve, reject) => {
      options?.signal?.addEventListener('abort', () => reject(new Error('aborted')));
    })
  );
  const pending = exchange('http://localhost/exchange', 'test', 1, 'null');
  const rejected = expect(pending).rejects.toThrow();
  await jest.advanceTimersByTimeAsync(29999);
  expect(fetchMock.mock.calls[0][1]?.signal?.aborted).toBe(false);
  await jest.advanceTimersByTimeAsync(1);
  await rejected;
  expect(fetchMock.mock.calls[0][1]?.signal?.aborted).toBe(true);
  expect(jest.getTimerCount()).toBe(0);
});

it('clears the exchange deadline after a response', async () => {
  jest.useFakeTimers();
  jest.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('{"schema_version":1,"sequence":1,"state":"complete","command":null}'));
  await exchange('http://localhost/exchange', 'test', 1, 'null');
  expect(jest.getTimerCount()).toBe(0);
});

it('polls at 100 milliseconds and returns the original result text', async () => {
  jest.useFakeTimers();
  const raw = ` \n${JSON.stringify(passed)}\n`;
  stateAttributes.mockResolvedValueOnce({ text: 'running' }).mockResolvedValueOnce({ text: 'ok' });
  resultAttributes.mockResolvedValue({ text: raw });
  const pending = pollCorpusResult(45000, 'command timed out');
  await jest.advanceTimersByTimeAsync(99);
  expect(resultAttributes).not.toHaveBeenCalled();
  await jest.advanceTimersByTimeAsync(1);
  await expect(pending).resolves.toEqual({ raw, envelope: passed });
});

it.each([null, 'expected native failure'])('returns error envelopes for journey-specific handling: %s', async (detail) => {
  const envelope = { ...failed, error_detail: detail };
  const raw = JSON.stringify(envelope);
  stateAttributes.mockResolvedValue({ text: 'error' });
  resultAttributes.mockResolvedValue({ text: raw });
  await expect(pollCorpusResult(45000, 'command timed out')).resolves.toEqual({ raw, envelope });
});

it.each([
  null, [], {}, { ...passed, extra: true }, { ...passed, schema_version: 2 },
  { ...passed, outcome: 'unknown' }, { ...passed, result: null },
  { ...passed, error_code: 'invalid' }, { ...passed, error_detail: 'invalid' },
  { ...failed, result: {} }, { ...failed, error_code: null },
  { ...failed, error_code: 7 }, { ...failed, error_detail: {} },
])('rejects malformed conformance envelopes: %j', async (envelope) => {
  resultAttributes.mockResolvedValue({ text: JSON.stringify(envelope) });
  await expect(pollCorpusResult(45000, 'command timed out')).rejects.toThrow();
});

it('rejects malformed or missing result text', async () => {
  for (const text of ['not JSON', undefined, 7]) {
    resultAttributes.mockResolvedValue({ text });
    await expect(pollCorpusResult(45000, 'command timed out')).rejects.toThrow();
  }
});

it('fails at the requested poll deadline instead of accepting a late result', async () => {
  jest.useFakeTimers();
  stateAttributes.mockResolvedValue({ text: 'running' });
  const pending = pollCorpusResult(120000, 'journey timed out');
  const rejected = expect(pending).rejects.toThrow('journey timed out');
  await jest.advanceTimersByTimeAsync(119999);
  expect(resultAttributes).not.toHaveBeenCalled();
  await jest.advanceTimersByTimeAsync(1);
  await rejected;
});
