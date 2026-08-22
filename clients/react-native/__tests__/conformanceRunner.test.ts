jest.mock('@trainstar/synchro-react-native', () => jest.requireActual('../src/index'), {
  virtual: true,
});

import { PublicConformanceRunner } from '../example/src/conformance/runner';
import type {
  ConformanceCommand,
  JSONObject,
  ScenarioStep,
} from '../example/src/conformance/types';
import { mockNativeModule, resetNativeModuleMockState } from './__mocks__/react-native';

const READY_STATUS = JSON.stringify({
  status: 'ready',
  retryAt: null,
  operation: null,
  failure: null,
});

function command(
  actor: string,
  action: string,
  clientKey: string,
  parameters: JSONObject,
  steps?: ScenarioStep[]
): ConformanceCommand {
  return {
    schema_version: 1,
    action: {
      action: {
        actor,
        command: action,
        parameters: { client_key: clientKey, ...parameters },
      },
      steps: steps ?? [],
    },
    runtime: {
      client_key: clientKey,
      database_path: `${clientKey}.db`,
      client_id: `${clientKey}-id`,
    },
  };
}

function protocolStep(): ScenarioStep {
  return {
    operation: { contract_operation: 'connect', name: 'request', payload: {} },
  };
}

describe('PublicConformanceRunner call lifecycle', () => {
  beforeEach(() => {
    resetNativeModuleMockState();
    mockNativeModule.getSyncStatus.mockResolvedValue(READY_STATUS);
  });

  it('uses one retry-after-error call for progress observation and completion', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });
    await runner.execute(command('client', 'open', 'client-a', { database_mode: 'create', seed_step_id: null }));

    await expect(
      runner.execute(command('client', 'begin-call', 'client-a', { call_id: 'retry-call', method: 'retry-after-error' }))
    ).resolves.toMatchObject({ kind: 'call-begun', call_id: 'retry-call', state: 'in_flight' });

    await expect(
      runner.execute(
        command('observer', 'await-step', 'client-a', { call_id: 'retry-call' }, [protocolStep()])
      )
    ).resolves.toMatchObject({ kind: 'awaited', status: { state: 'ready' } });

    await expect(
      runner.execute(
        command('client', 'await-call', 'client-a', { call_id: 'retry-call', completion: 'idle' })
      )
    ).resolves.toMatchObject({
      kind: 'call-completed',
      call_id: 'retry-call',
      state: 'completed',
      completion: 'idle',
      status: { state: 'ready' },
    });

    expect(mockNativeModule.retryAfterError).toHaveBeenCalledTimes(1);
    await runner.close();
  });

  it('rejects duplicate and mismatched calls', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });
    await runner.execute(command('client', 'open', 'client-a', { database_mode: 'create', seed_step_id: null }));
    await runner.execute(
      command('client', 'begin-call', 'client-a', { call_id: 'shared-call', method: 'start' })
    );

    await expect(
      runner.execute(
        command('client', 'begin-call', 'client-a', { call_id: 'shared-call', method: 'sync-now' })
      )
    ).rejects.toMatchObject({ code: 'invalid_command' });
    await expect(
      runner.execute(
        command('client', 'await-call', 'client-b', { call_id: 'shared-call', completion: 'idle' })
      )
    ).rejects.toMatchObject({ code: 'invalid_command' });

    await runner.close();
  });
});
