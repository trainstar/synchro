import { by, device, element, expect } from 'detox';

type ExchangeResponse = { schema_version: number; sequence: number } & (
  | { state: 'command'; command: Record<string, unknown> }
  | { state: 'complete'; command: null }
);

function configuration(): { endpoint: string; token: string; stageCount: number } {
  const url = process.env.SYNCHRO_RN_COORDINATOR_URL;
  const token = process.env.SYNCHRO_RN_COORDINATOR_TOKEN;
  const stageCount = Number(process.env.SYNCHRO_RN_COORDINATOR_STAGE_COUNT);
  if (!url || !token || !/^[A-Za-z0-9_-]{43}$/.test(token) || !Number.isSafeInteger(stageCount) || stageCount < 2) {
    throw new Error('React Native rebuild-apply coordinator configuration is invalid');
  }
  const parsed = new URL(url);
  if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname || parsed.username || parsed.password || (parsed.pathname !== '' && parsed.pathname !== '/') || parsed.search || parsed.hash) {
    throw new Error('React Native rebuild-apply coordinator configuration is invalid');
  }
  return { endpoint: new URL('/exchange', parsed).toString(), token, stageCount };
}

function response(raw: string, sequence: number): ExchangeResponse {
  const value = JSON.parse(raw) as Record<string, unknown>;
  if (JSON.stringify(Object.keys(value).sort()) !== JSON.stringify(['command', 'schema_version', 'sequence', 'state']) || value.schema_version !== 1 || value.sequence !== sequence || (value.state !== 'command' && value.state !== 'complete') || (value.state === 'command' && (typeof value.command !== 'object' || value.command === null || Array.isArray(value.command))) || (value.state === 'complete' && value.command !== null)) {
    throw new Error('React Native rebuild-apply coordinator response is invalid');
  }
  return value as unknown as ExchangeResponse;
}

async function exchange(endpoint: string, token: string, sequence: number, result: string): Promise<ExchangeResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const resultResponse = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: `{"schema_version":1,"sequence":${sequence},"result":${result}}`, signal: controller.signal });
    const body = await resultResponse.text();
    if (!resultResponse.ok) throw new Error(`HTTP ${resultResponse.status}: ${body}`);
    return response(body, sequence);
  } finally { clearTimeout(timeout); }
}

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await element(by.id('conformance-command-input')).replaceText(serialized);
  if ((await element(by.id('conformance-command-input')).getAttributes()).text !== serialized) throw new Error('React Native conformance command input changed');
  await element(by.id('conformance-command-input')).tapReturnKey();
  await element(by.id('btn-conformance-execute')).tap();
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    const state = await element(by.id('conformance-command-state')).getAttributes();
    if (state.text === 'ok' || state.text === 'error') {
      const raw = String((await element(by.id('conformance-result')).getAttributes()).text ?? '');
      const envelope = JSON.parse(raw) as { outcome: string; error_code: string | null; error_detail: string | null };
      if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
      return raw;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('React Native rebuild-apply command did not finish');
}

it('executes the rebuild-apply coordinator sequence', async () => {
  const { endpoint, token, stageCount } = configuration();
  await device.launchApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  await expect(element(by.id('conformance-harness'))).toBeVisible();
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) throw new Error('React Native rebuild-apply coordinator completed at an invalid stage');
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error('React Native rebuild-apply coordinator did not complete');
});
