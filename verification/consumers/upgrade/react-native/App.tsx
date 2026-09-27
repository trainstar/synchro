import React, { useEffect, useState } from 'react';
import { SafeAreaView, Text } from 'react-native';
import {
  SynchroClient,
  type PendingMutationInspection,
  type SQLiteBindValue,
} from '@trainstar/synchro-react-native';
import { controlURL, packageVersion } from './upgradeControl';

// Runs the steps that conformance/upgrade publishes for one package phase
// and reports each observation. The application uses only the public SDK.

type Step = {
  op: string;
  database?: string;
  client_id?: string;
  sql?: string;
  params?: SQLiteBindValue[];
  name?: string;
};

type PhaseConfig = {
  phase: string;
  server_url: string;
  token: string;
  app_version: string;
  snapshots: { name: string; sql: string }[];
  steps: Step[];
};

function inspection(mutation: PendingMutationInspection) {
  return {
    mutation_id: mutation.mutationID,
    local_order: mutation.localOrder,
    table_id: mutation.tableID,
    table_name: mutation.tableName,
    record_id: mutation.recordID,
    primary_key_field_id: mutation.primaryKeyFieldID,
    primary_key_logical_type: mutation.primaryKeyLogicalType,
    operation: mutation.operation,
    schema_version: mutation.authoredSchema.version,
    schema_hash: mutation.authoredSchema.hash,
    base_version: mutation.baseVersion,
    client_version: mutation.clientVersion,
    status: mutation.status,
    source_kind: mutation.sourceKind,
    depends_on_mutation_id: mutation.dependsOnMutationID,
    normalized_mutation_id: mutation.normalizedMutationID,
    sealed_batch_id: mutation.sealedBatchID,
    sealed_ordinal: mutation.sealedOrdinal,
    fields: mutation.authoredFields.map(field => ({
      field_id: field.fieldID,
      logical_type: field.logicalType,
      value: field.value,
    })),
  };
}

const sleep = (milliseconds: number) =>
  new Promise<void>(resolve => setTimeout(resolve, milliseconds));

async function awaitReady(client: SynchroClient, failOnError: boolean) {
  for (let attempt = 0; attempt < 600; attempt += 1) {
    const status = await client.getSyncStatus();
    if (status.status === 'ready') {
      return true;
    }
    if (status.status === 'error' || (!failOnError && status.status === 'stopped')) {
      if (failOnError) {
        throw new Error(`sync engine entered error: ${JSON.stringify(status)}`);
      }
      return false;
    }
    await sleep(100);
  }
  throw new Error('sync engine did not reach ready within 60 seconds');
}

// syncNow requires a connected engine. A retryable failure moves the engine
// to backoff, and the step waits for its scheduled retry. A sync that does
// not finish fails the step, so the earlier observations still report.
class SyncTimeout extends Error {}

async function synchronize(client: SynchroClient) {
  await awaitReady(client, true);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      client.syncNow(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new SyncTimeout('sync did not finish within 120 seconds')),
          120000
        );
      }),
    ]);
  } catch (error) {
    if (error instanceof SyncTimeout || !(await awaitReady(client, false))) {
      throw error;
    }
  } finally {
    clearTimeout(timer);
  }
}

// Observations collect in order, so a failed step still reports the earlier ones.
async function run(config: PhaseConfig, observations: unknown[]) {
  let client: SynchroClient | null = null;
  const current = () => {
    if (client === null) {
      throw new Error('no database is open');
    }
    return client;
  };
  for (const [index, step] of config.steps.entries()) {
    try {
      switch (step.op) {
        case 'open':
          client = new SynchroClient({
            dbPath: step.database ?? '',
            serverURL: config.server_url,
            authProvider: async () => config.token,
            clientID: step.client_id ?? '',
            appVersion: config.app_version,
            syncInterval: 3600,
            pushDebounce: 3600,
          });
          await client.initialize();
          break;
        case 'start':
          await current().start();
          break;
        case 'sync':
          await synchronize(current());
          break;
        case 'stop':
          await current().stop();
          break;
        case 'close':
          await current().close();
          client = null;
          break;
        case 'create_local_table':
          await current().createTable('local_notes', [
            { name: 'id', type: 'TEXT', nullable: false, primaryKey: true },
            { name: 'body', type: 'TEXT', nullable: false },
          ]);
          break;
        case 'execute':
          await current().execute(step.sql ?? '', step.params ?? []);
          break;
        case 'observe': {
          const open = current();
          const snapshots: Record<string, unknown> = {};
          for (const snapshot of config.snapshots) {
            snapshots[snapshot.name] = await open.query(snapshot.sql);
          }
          observations.push({
            name: step.name,
            snapshots,
            pending: (await open.inspectPendingMutations()).map(inspection),
            pending_count: await open.pendingChangeCount(),
            rejected_count: (await open.inspectRejectedMutations()).length,
          });
          break;
        }
        default:
          throw new Error(`unknown step ${step.op}`);
      }
    } catch (error) {
      throw new Error(`step ${index} ${step.op} failed: ${String(error)}`);
    }
  }
}

async function runPhase() {
  const response = await fetch(`${controlURL}/config`);
  if (!response.ok) {
    throw new Error(`control config request failed with ${response.status}`);
  }
  const config = (await response.json()) as PhaseConfig;
  const observations: unknown[] = [];
  let failure = '';
  try {
    await run(config, observations);
  } catch (error) {
    failure = String(error);
  }
  const posted = await fetch(`${controlURL}/result`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      phase: config.phase,
      package_version: packageVersion,
      error: failure,
      observations,
    }),
  });
  if (!posted.ok) {
    throw new Error(`control result request failed with ${posted.status}`);
  }
  return failure === '' ? 'passed' : 'failed';
}

export default function App(): React.JSX.Element {
  const [state, setState] = useState('running');
  useEffect(() => {
    runPhase().then(setState, error => setState(`control failed: ${String(error)}`));
  }, []);
  return (
    <SafeAreaView>
      <Text>{state}</Text>
    </SafeAreaView>
  );
}
