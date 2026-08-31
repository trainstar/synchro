import { by, device, element, expect } from 'detox';

type ExchangeResponse = { schema_version: number; sequence: number } & (
  | { state: 'command'; command: Record<string, unknown> }
  | { state: 'complete'; command: null }
);

type ConformanceEnvelope = {
  schema_version: number;
  outcome: 'passed' | 'error';
  result: unknown;
  error_code: string | null;
  error_detail: string | null;
};

const exchangeMembers = ['command', 'schema_version', 'sequence', 'state'];
const envelopeMembers = ['error_code', 'error_detail', 'outcome', 'result', 'schema_version'];

function exactObject(value: unknown, members: string[]): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify(members)
  );
}

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function objectMembers(value: unknown): string {
  if (!isJSONObject(value)) return Array.isArray(value) ? 'array' : typeof value;
  return `object keys=${Object.keys(value).sort().join(',')}`;
}

function configuration(): { endpoint: string; token: string; stageCount: number } {
  const url = process.env.SYNCHRO_RN_COORDINATOR_URL;
  const token = process.env.SYNCHRO_RN_COORDINATOR_TOKEN;
  const stageCount = Number(process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT);
  if (
    !url ||
    !token ||
    !/^[A-Za-z0-9_-]{43}$/.test(token) ||
    !Number.isSafeInteger(stageCount) ||
    stageCount < 2
  ) {
    throw new Error(`React Native schema-check coordinator configuration is invalid: stage_count=${stageCount}`);
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error('React Native schema-check coordinator URL is invalid');
  }
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    !parsed.hostname ||
    parsed.username !== '' ||
    parsed.password !== '' ||
    (parsed.pathname !== '' && parsed.pathname !== '/') ||
    parsed.search !== '' ||
    parsed.hash !== ''
  ) {
    throw new Error(
      `React Native schema-check coordinator URL is invalid: protocol=${parsed.protocol} host_present=${Boolean(parsed.hostname)}`
    );
  }
  return { endpoint: new URL('/exchange', parsed).toString(), token, stageCount };
}

function response(raw: string, sequence: number): ExchangeResponse {
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new Error(`React Native schema-check coordinator response is not JSON: sequence=${sequence}`);
  }
  if (
    !exactObject(value, exchangeMembers) ||
    value.schema_version !== 1 ||
    value.sequence !== sequence ||
    (value.state !== 'command' && value.state !== 'complete')
  ) {
    throw new Error(
      `React Native schema-check coordinator response is invalid: sequence=${sequence} ${objectMembers(value)}`
    );
  }
  if (value.state === 'command') {
    if (!isJSONObject(value.command)) {
      throw new Error(
        `React Native schema-check command is invalid: sequence=${sequence} ${objectMembers(value.command)}`
      );
    }
  } else if (value.command !== null) {
    throw new Error(
      `React Native schema-check completion command is invalid: sequence=${sequence} command_type=${typeof value.command}`
    );
  }
  return value as unknown as ExchangeResponse;
}

async function exchange(
  endpoint: string,
  token: string,
  sequence: number,
  result: string
): Promise<ExchangeResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const resultResponse = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: `{"schema_version":1,"sequence":${sequence},"result":${result}}`,
      signal: controller.signal,
    });
    const body = await resultResponse.text();
    if (!resultResponse.ok) throw new Error(`HTTP status=${resultResponse.status} body_bytes=${body.length}`);
    return response(body, sequence);
  } finally {
    clearTimeout(timeout);
  }
}

function conformanceEnvelope(raw: string): ConformanceEnvelope {
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new Error('React Native conformance result is not JSON');
  }
  if (
    !exactObject(value, envelopeMembers) ||
    value.schema_version !== 1 ||
    (value.outcome !== 'passed' && value.outcome !== 'error') ||
    (value.outcome === 'passed' &&
      (value.result === null ||
        value.error_code !== null ||
        value.error_detail !== null)) ||
    (value.outcome === 'error' &&
      (value.result !== null ||
        typeof value.error_code !== 'string' ||
        (value.error_detail !== null && typeof value.error_detail !== 'string')))
  ) {
    throw new Error(`React Native conformance result is invalid: ${objectMembers(value)}`);
  }
  return value as unknown as ConformanceEnvelope;
}

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await element(by.id('conformance-command-input')).replaceText(serialized);
  const input = await element(by.id('conformance-command-input')).getAttributes();
  if (input.text !== serialized) {
    throw new Error(
      `React Native conformance command input changed: observed_bytes=${typeof input.text === 'string' ? input.text.length : 0} want_bytes=${serialized.length}`
    );
  }
  await element(by.id('conformance-command-input')).tapReturnKey();
  await element(by.id('btn-conformance-execute')).tap();
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    const state = await element(by.id('conformance-command-state')).getAttributes();
    if (state.text === 'ok' || state.text === 'error') {
      const raw = String((await element(by.id('conformance-result')).getAttributes()).text ?? '');
      const envelope = conformanceEnvelope(raw);
      if (envelope.outcome === 'error') {
        throw new Error(
          `React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`
        );
      }
      return raw;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('React Native schema-check command did not finish');
}

it('executes the schema-check coordinator sequence', async () => {
  const { endpoint, token, stageCount } = configuration();
  await device.launchApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });
  await expect(element(by.id('conformance-harness'))).toBeVisible();
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) {
        throw new Error(
          `React Native schema-check coordinator completed at sequence ${sequence} after ${commands} commands`
        );
      }
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error(`React Native schema-check coordinator did not complete after ${stageCount} exchanges`);
}, 120000);
