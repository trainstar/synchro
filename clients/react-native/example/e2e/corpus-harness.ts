import { by, device, element, waitFor } from 'detox';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

type LaunchOptions = Parameters<typeof device.launchApp>[0];

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

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function exactObject(value: unknown, members: readonly string[]): value is Record<string, unknown> {
  return isJSONObject(value) &&
    Object.keys(value).length === members.length &&
    members.every((member) => Object.hasOwn(value, member));
}

export function coordinatorConfiguration(): { endpoint: string; token: string } {
  const url = process.env.SYNCHRO_RN_COORDINATOR_URL;
  const token = process.env.SYNCHRO_RN_COORDINATOR_TOKEN;
  if (!url || !token || !/^[A-Za-z0-9_-]{43}$/.test(token)) {
    throw new Error('React Native coordinator configuration is invalid');
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error('React Native coordinator URL is invalid');
  }
  if (
    (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
    !parsed.hostname || parsed.username || parsed.password ||
    (parsed.pathname !== '' && parsed.pathname !== '/') ||
    parsed.search || parsed.hash
  ) {
    throw new Error('React Native coordinator URL is invalid');
  }
  return { endpoint: new URL('/exchange', parsed).toString(), token };
}

export function coordinatorCount(
  name: 'STAGE_COUNT' | 'EXCHANGE_COUNT',
  minimum: number,
  maximum = Number.MAX_SAFE_INTEGER
): number {
  const count = Number(process.env[`SYNCHRO_RN_COORDINATOR_${name}`]);
  if (!Number.isSafeInteger(count) || count < minimum || count > maximum) {
    throw new Error(`React Native coordinator ${name} is invalid`);
  }
  return count;
}

function parseExchangeResponse(raw: string, sequence: number): ExchangeResponse {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error('React Native coordinator response is not JSON');
  }
  if (
    !exactObject(value, ['command', 'schema_version', 'sequence', 'state']) ||
    value.schema_version !== 1 || value.sequence !== sequence
  ) {
    throw new Error('React Native coordinator response is invalid');
  }
  if (value.state === 'command' && isJSONObject(value.command)) {
    return { schema_version: 1, sequence, state: 'command', command: value.command };
  }
  if (value.state === 'complete' && value.command === null) {
    return { schema_version: 1, sequence, state: 'complete', command: null };
  }
  throw new Error('React Native coordinator response is invalid');
}

function parseConformanceEnvelope(raw: string): ConformanceEnvelope {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error('React Native conformance envelope is not JSON');
  }
  if (
    !exactObject(value, ['error_code', 'error_detail', 'outcome', 'result', 'schema_version']) ||
    value.schema_version !== 1
  ) {
    throw new Error('React Native conformance envelope is invalid');
  }
  if (
    value.outcome === 'passed' && value.result !== null &&
    value.error_code === null && value.error_detail === null
  ) {
    return { schema_version: 1, outcome: 'passed', result: value.result, error_code: null, error_detail: null };
  }
  if (
    value.outcome === 'error' && value.result === null &&
    typeof value.error_code === 'string' &&
    (value.error_detail === null || typeof value.error_detail === 'string')
  ) {
    return { schema_version: 1, outcome: 'error', result: null, error_code: value.error_code, error_detail: value.error_detail };
  }
  throw new Error('React Native conformance envelope is invalid');
}

export async function exchange(
  endpoint: string,
  token: string,
  sequence: number,
  rawResult: string
): Promise<ExchangeResponse> {
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new Error('React Native coordinator sequence is invalid');
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), WAIT_TIMEOUT_MS);
  try {
    let response: Response;
    try {
      response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: `{"schema_version":1,"sequence":${sequence},"result":${rawResult}}`,
        signal: controller.signal,
      });
    } catch {
      throw new Error(`React Native coordinator exchange ${sequence} failed`);
    }
    if (!response.ok) {
      throw new Error(`React Native coordinator exchange ${sequence} failed: HTTP ${response.status}`);
    }
    return parseExchangeResponse(await response.text(), sequence);
  } finally {
    clearTimeout(timeout);
  }
}

export async function pollCorpusResult(
  timeoutMs: number,
  timeoutMessage: string
): Promise<{ raw: string; envelope: ConformanceEnvelope }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = await element(by.id('conformance-command-state')).getAttributes();
    if (!('text' in state)) {
      throw new Error('React Native conformance command state is invalid');
    }
    if (state.text === 'ok' || state.text === 'error') {
      const result = await element(by.id('conformance-result')).getAttributes();
      if (!('text' in result) || typeof result.text !== 'string') {
        throw new Error('React Native conformance result text is invalid');
      }
      const raw = result.text;
      return { raw, envelope: parseConformanceEnvelope(raw) };
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(timeoutMessage);
}

export async function runCorpusCommandLoop(execute: () => Promise<void>): Promise<void> {
  try {
    return await execute();
  } finally {
    // Fault scenarios can leave requests held until the host closes.
    await device.terminateApp();
  }
}

export async function launchCorpusApp(options: LaunchOptions): Promise<void> {
  await device.launchApp(options);
  await device.disableSynchronization();
  await waitFor(element(by.id('conformance-harness'))).toBeVisible().withTimeout(WAIT_TIMEOUT_MS);
}

export async function submitCorpusCommand(serialized: string): Promise<void> {
  const input = element(by.id('conformance-command-input'));
  const state = element(by.id('conformance-command-state'));
  await input.replaceText('');
  await waitFor(state).toHaveText('idle').withTimeout(WAIT_TIMEOUT_MS);
  await input.replaceText(serialized);
  await waitFor(state).toHaveText('ready').withTimeout(WAIT_TIMEOUT_MS);
  const attributes = await input.getAttributes();
  if (!('text' in attributes) || attributes.text !== serialized) {
    throw new Error('React Native conformance command input changed');
  }
  await input.tapReturnKey();
  await element(by.id('btn-conformance-execute')).tap();
}
