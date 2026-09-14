import { by, device, element, expect } from 'detox';

type ExchangeResponse = {
  schema_version: number;
  sequence: number;
} & (
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

function coordinatorConfiguration(): { endpoint: string; token: string; exchanges: number } {
  const configuredURL = process.env.SYNCHRO_RN_COORDINATOR_URL;
  const token = process.env.SYNCHRO_RN_COORDINATOR_TOKEN;
  const exchanges = Number(process.env.SYNCHRO_RN_COORDINATOR_EXCHANGE_COUNT);
  if (!configuredURL || !token || !/^[A-Za-z0-9_-]{43}$/.test(token) || !Number.isSafeInteger(exchanges) || exchanges < 2) {
    throw new Error('React Native coordinator configuration is invalid');
  }

  let parsed: URL;
  try {
    parsed = new URL(configuredURL);
  } catch {
    throw new Error('React Native coordinator configuration is invalid');
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
    throw new Error('React Native coordinator configuration is invalid');
  }
  return { endpoint: new URL('/exchange', parsed).toString(), token, exchanges };
}

function parseExchangeResponse(raw: string, sequence: number): ExchangeResponse {
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new Error('React Native coordinator response is invalid');
  }
  if (
    !exactObject(value, exchangeMembers) ||
    value.schema_version !== 1 ||
    value.sequence !== sequence ||
    (value.state !== 'command' && value.state !== 'complete')
  ) {
    throw new Error('React Native coordinator response is invalid');
  }
  if (value.state === 'command') {
    if (!isJSONObject(value.command)) {
      throw new Error('React Native coordinator response is invalid');
    }
  } else if (value.command !== null) {
    throw new Error('React Native coordinator response is invalid');
  }
  return value as unknown as ExchangeResponse;
}

function parseConformanceEnvelope(raw: string): ConformanceEnvelope {
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new Error('React Native conformance envelope is invalid');
  }
  if (
    !exactObject(value, envelopeMembers) ||
    value.schema_version !== 1 ||
    (value.outcome !== 'passed' && value.outcome !== 'error')
  ) {
    throw new Error('React Native conformance envelope is invalid');
  }
  if (
    (value.outcome === 'passed' && (value.result === null || value.error_code !== null)) ||
    (value.outcome === 'error' && (value.result !== null || typeof value.error_code !== 'string'))
  ) {
    throw new Error('React Native conformance envelope is invalid');
  }
  return value as unknown as ConformanceEnvelope;
}

async function exchange(
  endpoint: string,
  token: string,
  sequence: number,
  rawResult: string
): Promise<ExchangeResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: `{"schema_version":1,"sequence":${sequence},"result":${rawResult}}`,
      signal: controller.signal,
    });
    const responseBody = await response.text();
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${responseBody}`);
    }
    return parseExchangeResponse(responseBody, sequence);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`React Native coordinator exchange ${sequence} failed: ${detail}`);
  } finally {
    clearTimeout(timeout);
  }
}

function isDeviceRestart(command: Record<string, unknown>): boolean {
  const action = command.action;
  if (!isJSONObject(action)) return false;
  const manifest = action.action;
  return isJSONObject(manifest) && manifest.actor === 'device' && manifest.command === 'restart';
}

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  if (isDeviceRestart(command)) {
    await device.launchApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });
    await expect(element(by.id('conformance-harness'))).toBeVisible();
    return 'null';
  }
  const serialized = JSON.stringify(command);
  await element(by.id('conformance-command-input')).replaceText(serialized);
  const input = await element(by.id('conformance-command-input')).getAttributes();
  if (input.text !== serialized) {
    throw new Error('React Native conformance command input changed');
  }
  await element(by.id('conformance-command-input')).tapReturnKey();
  await element(by.id('btn-conformance-execute')).tap();

  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    const state = await element(by.id('conformance-command-state')).getAttributes();
    if (state.text === 'ok' || state.text === 'error') {
      const result = await element(by.id('conformance-result')).getAttributes();
      const raw = typeof result.text === 'string' ? result.text : '';
      const envelope = parseConformanceEnvelope(raw);
      if (envelope.outcome === 'error') {
        throw new Error(`React Native conformance command failed: ${envelope.error_code}`);
      }
      return raw;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('React Native conformance command did not finish');
}

it('executes the pending-cycle coordinator sequence', async () => {
  const { endpoint, token, exchanges } = coordinatorConfiguration();
  await device.launchApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });
  await expect(element(by.id('conformance-harness'))).toBeVisible();

  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= exchanges; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== exchanges || commandCount !== exchanges - 1) {
        throw new Error('React Native pending-cycle coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount >= exchanges) {
      throw new Error('React Native pending-cycle coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native pending-cycle coordinator did not complete');
});
