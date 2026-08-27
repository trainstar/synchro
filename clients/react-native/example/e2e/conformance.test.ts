import { by, device, element, expect, waitFor } from 'detox';

type ConformanceEnvelope = {
  schema_version: number;
  outcome: string;
  result: unknown;
  error_code: string | null;
};

type ProcessIdentity = {
  process_id: string;
  database_identity_fingerprint: string;
};

function command(
  actor: string,
  action: string,
  databasePath: string,
  databaseMode: 'create' | 'reuse' = 'create'
) {
  const clientKey = 'detox-client';
  return {
    schema_version: 1,
    action: {
      action: {
        actor,
        command: action,
        parameters: actor === 'client' && action === 'open'
          ? { client_key: clientKey, database_mode: databaseMode, seed_step_id: null }
          : { client_key: clientKey },
      },
      steps: [],
    },
    runtime: {
      client_key: clientKey,
      database_path: databasePath,
      client_id: 'detox-client-id',
    },
  };
}

async function executeCommand(value: object): Promise<ConformanceEnvelope> {
  await expect(element(by.id('conformance-harness'))).toBeVisible();
  const serialized = JSON.stringify(value);
  await element(by.id('conformance-command-input')).replaceText(serialized);
  const input = await element(by.id('conformance-command-input')).getAttributes();
  if (input.text !== serialized) {
    throw new Error(`conformance command input mismatch: ${String(input.text)}`);
  }
  await element(by.id('btn-conformance-execute')).tap();
  const deadline = Date.now() + 15000;
  while (true) {
    const state = await element(by.id('conformance-command-state')).getAttributes();
    if (state.text === 'ok' || state.text === 'error') {
      break;
    }
    if (Date.now() >= deadline) {
      throw new Error(`conformance command did not finish: ${String(state.text)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const attributes = await element(by.id('conformance-result')).getAttributes();
  return JSON.parse(String(attributes.text)) as ConformanceEnvelope;
}

async function executeRequiredCommand(value: object): Promise<unknown> {
  const envelope = await executeCommand(value);
  if (envelope.outcome !== 'passed') {
    throw new Error(`required conformance command failed: ${String(envelope.error_code)}`);
  }
  return envelope.result;
}

function processIdentity(value: unknown): ProcessIdentity {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('process' in value) ||
    typeof value.process !== 'object' ||
    value.process === null ||
    !('process_id' in value.process) ||
    typeof value.process.process_id !== 'string' ||
    !('database_identity_fingerprint' in value.process) ||
    typeof value.process.database_identity_fingerprint !== 'string'
  ) {
    throw new Error('conformance result omitted process identity');
  }
  return value.process as ProcessIdentity;
}

describe('React Native conformance host', () => {
  beforeAll(async () => {
    await device.launchApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });
  });

  it('emits the strict error envelope for an invalid command', async () => {
    await expect(element(by.id('conformance-harness'))).toBeVisible();
    await element(by.id('conformance-command-input')).replaceText('{');
    await element(by.id('btn-conformance-execute')).tap();
    await waitFor(element(by.id('conformance-result')))
      .toHaveText('{"schema_version":1,"outcome":"error","result":null,"error_code":"invalid_command"}')
      .withTimeout(5000);
  });

  it('rejects create mode when the database survives a process relaunch', async () => {
    await device.launchApp({
      newInstance: true,
      delete: true,
      launchArgs: { synchroConformance: '1' },
    });
    const open = command('client', 'open', `rn-create-isolation-${Date.now()}.db`);
    const firstProcess = processIdentity(await executeRequiredCommand(open));

    await device.launchApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });

    const envelope = await executeCommand(open);
    if (envelope.outcome !== 'error' || envelope.error_code !== 'invalid_command') {
      throw new Error(`create mode reused a persisted database: ${JSON.stringify(envelope)}`);
    }

    const secondProcess = processIdentity(
      await executeRequiredCommand(command('client', 'open', open.runtime.database_path, 'reuse'))
    );
    if (secondProcess.process_id === firstProcess.process_id) {
      throw new Error('conformance relaunch preserved the native process identity');
    }
    if (
      secondProcess.database_identity_fingerprint !==
      firstProcess.database_identity_fingerprint
    ) {
      throw new Error('conformance relaunch changed the database identity');
    }
  });

  it('treats unavailable as a failed required command', async () => {
    let commandError: unknown;
    try {
      await executeRequiredCommand(command('controller', 'unsupported', 'unused.db'));
    } catch (error) {
      commandError = error;
    }
    if (
      !(commandError instanceof Error) ||
      commandError.message !== 'required conformance command failed: unavailable'
    ) {
      throw new Error(`required unavailable command did not fail: ${String(commandError)}`);
    }
  });
});
