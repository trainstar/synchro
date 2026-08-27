jest.mock('@trainstar/synchro-react-native', () => jest.requireActual('../src/index'), {
  virtual: true,
});

import { PublicConformanceRunner } from '../example/src/conformance/runner';
import { parseConformanceCommand } from '../example/src/conformance/types';
import type {
  ConformanceCommand,
  JSONObject,
  ScenarioStep,
} from '../example/src/conformance/types';
import {
  emitNativeEvent,
  mockNativeModule,
  resetNativeModuleMockState,
} from './__mocks__/react-native';

const READY_STATUS = JSON.stringify({
  status: 'ready',
  retryAt: null,
  operation: null,
  failure: null,
});

const CLIENT_STATE_COUNTS = {
  application_row_count: 0,
  mutation_ledger_count: 0,
  mutation_outcome_count: 0,
  sealed_batch_count: 0,
  rejected_mutation_count: 0,
  scope_state_count: 0,
  scope_row_count: 0,
  provenance_count: 0,
  row_metadata_count: 0,
  rebuild_attempt_count: 0,
  rebuild_receipt_count: 0,
};

it('rejects duplicate command members before execution', () => {
  expect(() => parseConformanceCommand('{"schema_version":1,"schema_version":1}')).toThrow();
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
    mockNativeModule.initialize.mockResolvedValue(undefined);
    mockNativeModule.getSyncStatus.mockResolvedValue(READY_STATUS);
  });

  it('rejects create mode when the database survives a runner relaunch', async () => {
    const databases = new Set<string>();
    mockNativeModule.initialize.mockImplementation(async (config: {
      dbPath: string;
      requireNewDatabase: boolean;
    }) => {
      if (config.requireNewDatabase && databases.has(config.dbPath)) {
        throw { code: 'INVALID_CONFIG', message: 'Database already exists' };
      }
      databases.add(config.dbPath);
    });
    const options = {
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    };
    const firstRunner = new PublicConformanceRunner(options);
    const open = command('client', 'open', 'client-a', {
      database_mode: 'create',
      seed_step_id: null,
    });

    await expect(firstRunner.execute(open)).resolves.toMatchObject({ kind: 'opened' });
    await firstRunner.close();

    const relaunchedRunner = new PublicConformanceRunner(options);
    await expect(relaunchedRunner.execute(open)).rejects.toMatchObject({ code: 'invalid_command' });
    expect(mockNativeModule.initialize).toHaveBeenCalledTimes(2);
    expect(databases).toEqual(new Set(['client-a.db']));
    await relaunchedRunner.close();
  });

  it('reports unsupported required actions as unavailable errors', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });

    await expect(
      runner.execute(command('controller', 'unsupported', 'client-a', {}))
    ).rejects.toMatchObject({ code: 'unavailable' });
    await runner.close();
  });

  it('uses paired runtime connection values instead of constructor defaults', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://default.invalid',
      authToken: 'default-token',
      appVersion: '1.0.0',
    });
    const open = command('client', 'open', 'client-a', {
      database_mode: 'create',
      seed_step_id: null,
    });
    open.runtime.server_url = 'http://runtime.invalid';
    open.runtime.auth_token = 'runtime-token';

    await runner.execute(open);
    expect(mockNativeModule.initialize).toHaveBeenCalledWith(
      expect.objectContaining({ serverURL: 'http://runtime.invalid' })
    );

    emitNativeEvent('onAuthRequest', { requestID: 'runtime-auth' });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(mockNativeModule.resolveAuthRequest).toHaveBeenCalledWith(
      'runtime-auth',
      'runtime-token'
    );
    await runner.close();
  });

  it.each(['server_url', 'auth_token'] as const)(
    'rejects an unpaired runtime %s',
    async (field) => {
      const runner = new PublicConformanceRunner({
        serverURL: 'http://localhost:8091',
        authToken: 'test-token',
        appVersion: '1.0.0',
      });
      const open = command('client', 'open', 'client-a', {
        database_mode: 'create',
        seed_step_id: null,
      });
      open.runtime[field] = field === 'server_url' ? 'http://runtime.invalid' : 'runtime-token';

      await expect(runner.execute(open)).rejects.toMatchObject({ code: 'invalid_command' });
      expect(mockNativeModule.initialize).not.toHaveBeenCalled();
      await runner.close();
    }
  );

  it('reuses a created database when its session becomes active again', async () => {
    const databases = new Set<string>();
    mockNativeModule.initialize.mockImplementation(async (config: {
      dbPath: string;
      requireNewDatabase: boolean;
    }) => {
      if (config.requireNewDatabase && databases.has(config.dbPath)) {
        throw { code: 'INVALID_CONFIG', message: 'Database already exists' };
      }
      databases.add(config.dbPath);
    });
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });

    await runner.execute(command('client', 'open', 'client-a', {
      database_mode: 'create',
      seed_step_id: null,
    }));
    await runner.execute(command('client', 'open', 'client-b', {
      database_mode: 'create',
      seed_step_id: null,
    }));
    await expect(
      runner.execute(command('observer', 'await-step', 'client-a', {}))
    ).resolves.toMatchObject({ kind: 'awaited' });

    expect(mockNativeModule.initialize.mock.calls.map(([config]) => ({
      dbPath: config.dbPath,
      requireNewDatabase: config.requireNewDatabase,
    }))).toEqual([
      { dbPath: 'client-a.db', requireNewDatabase: true },
      { dbPath: 'client-b.db', requireNewDatabase: true },
      { dbPath: 'client-a.db', requireNewDatabase: false },
    ]);
    await runner.close();
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

  it('captures normalized durable proof through the conformance facade', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });
    await runner.execute(command('client', 'open', 'client-a', { database_mode: 'create', seed_step_id: null }));
    mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
      schema: { version: 1, hash: 'a'.repeat(64) },
      scope_states: [],
      scope_rows: [{
        scope_id: 'scope-runtime',
        table_name: 'cf_items',
        record_id: 'runtime-row-a',
        checksum: 'checksum-runtime',
        generation: 1,
      }],
      rebuild_attempts: [],
      ...CLIENT_STATE_COUNTS,
      provenance_maintenance_work_cursor: '1',
    }));
    mockNativeModule.inspectDurableState.mockResolvedValueOnce(JSON.stringify({
      row_metadata: {
        table_name: 'cf_items',
        record_id: 'runtime-row-a',
        server_version: 'version-runtime',
        row_checksum: '{"sha256":"checksum-runtime"}',
      },
      rebuild_receipts: [{
        rebuild_id_fingerprint: 'b'.repeat(64),
        page_count: 1,
        returned_record_count: 0,
        request_chain_expected: ['final'],
        request_chain_observed: ['final'],
        record_identities_hex: [],
        received_row_checksums: [],
        computed_row_checksums: [],
        computed_scope_checksum: 'checksum-runtime',
        final_scope_checksum: 'checksum-runtime',
        stored_scope_checksum: 'checksum-runtime',
        local_scope_checksum: 'different-checksum',
      }],
    }));

    await expect(
      runner.execute(command('observer', 'capture', 'client-a', {
        client_keys: ['client-a'],
        sources: ['durable-proof'],
      }))
    ).resolves.toMatchObject({
      kind: 'capture',
      capture: {
        durable_proof: {
          row_metadata: { record_id: 'runtime-row-a' },
          rebuild_receipt_proofs: [{ page_count: 1 }],
        },
      },
    });
    expect(mockNativeModule.inspectDurableState).toHaveBeenCalledWith(
      'cf_items',
      'runtime-row-a'
    );
    await runner.close();
  });

  it('rejects malformed durable proof facts', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });
    await runner.execute(command('client', 'open', 'client-a', { database_mode: 'create', seed_step_id: null }));
    mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
      schema: null,
      scope_states: [],
      scope_rows: [{
        scope_id: 'scope-runtime',
        table_name: 'cf_items',
        record_id: 'runtime-row-a',
        checksum: 'checksum-runtime',
        generation: 1,
      }],
      rebuild_attempts: [],
      ...CLIENT_STATE_COUNTS,
      provenance_maintenance_work_cursor: '0',
    }));
    mockNativeModule.inspectDurableState.mockResolvedValueOnce('{}');

    await expect(
      runner.execute(command('observer', 'capture', 'client-a', {
        client_keys: ['client-a'],
        sources: ['durable-proof'],
      }))
    ).rejects.toMatchObject({ code: 'capture_inspection_failed' });
    await runner.close();
  });

  it('captures bootstrap rebuild proof before any scope row exists', async () => {
    const runner = new PublicConformanceRunner({
      serverURL: 'http://localhost:8091',
      authToken: 'test-token',
      appVersion: '1.0.0',
    });
    await runner.execute(command('client', 'open', 'client-a', { database_mode: 'create', seed_step_id: null }));
    mockNativeModule.inspectClientState.mockResolvedValueOnce(JSON.stringify({
      schema: { version: 1, hash: 'a'.repeat(64) },
      scope_states: [],
      scope_rows: [],
      rebuild_attempts: [],
      ...CLIENT_STATE_COUNTS,
      provenance_maintenance_work_cursor: '1',
    }));
    mockNativeModule.inspectDurableState.mockResolvedValueOnce(JSON.stringify({
      row_metadata: null,
      rebuild_receipts: [],
    }));

    await expect(
      runner.execute(command('observer', 'capture', 'client-a', {
        client_keys: ['client-a'],
        sources: ['durable-proof'],
        durable_proof_identity: {
          table_name: 'cf_items',
          record_id: 'bootstrap-absent-row',
        },
      }))
    ).resolves.toMatchObject({
      kind: 'capture',
      capture: { durable_proof: { row_metadata: null, rebuild_receipt_proofs: [] } },
    });
    expect(mockNativeModule.inspectDurableState).toHaveBeenCalledWith(
      'cf_items',
      'bootstrap-absent-row'
    );
    await runner.close();
  });
});
