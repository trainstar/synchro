import React, { useEffect, useState } from 'react';
import { SafeAreaView, Text } from 'react-native';
import { SynchroClient, type SQLiteInt64 } from '@trainstar/synchro-react-native';
import { packagedSmokeConfig } from './packagedSmokeConfig';

const client = new SynchroClient({
  dbPath: 'consumer.db',
  serverURL: packagedSmokeConfig.server_url,
  authProvider: async () => packagedSmokeConfig.token,
  clientID: packagedSmokeConfig.client_id,
  platform: packagedSmokeConfig.platform,
  // The application version, not the package version. The test adapter
  // gates clients below MIN_CLIENT_VERSION 1.0.0.
  appVersion: '1.0.0',
  syncInterval: 3600,
  pushDebounce: 3600,
  maxRetryAttempts: 1,
});

async function waitForCondition(
  condition: () => Promise<boolean>,
  timeoutMs = 15000,
  intervalMs = 250
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await condition()) {
      return true;
    }
    await new Promise<void>(resolve => setTimeout(resolve, intervalMs));
  }
  return false;
}

async function runAndWaitForScheduledPullRetry(
  operation: () => Promise<void>
) {
  try {
    await operation();
    return;
  } catch (error) {
    const status = await client.getSyncStatus();
    if (status.status !== 'backoff' || status.operation !== 'pulling') {
      throw error;
    }
    // The same 30 second bound as the Swift and Kotlin consumers. A capture_pending
    // retry waits at least its 5 second Retry-After, and a loaded host can need more than one.
    const retryCompleted = await waitForCondition(async () => {
      const current = await client.getSyncStatus();
      if (current.status === 'error' || current.status === 'stopped') {
        throw error;
      }
      return current.status === 'ready';
    }, 30000);
    if (!retryCompleted) {
      throw new Error('scheduled pull retry did not return to ready state');
    }
  }
}

async function startAndWaitForScheduledPullRetry() {
  await runAndWaitForScheduledPullRetry(() => client.start());
}

async function syncAndWaitForScheduledPullRetry() {
  await runAndWaitForScheduledPullRetry(() => client.syncNow());
}

type SmokePhase = 'initial' | 'resume';

type ObservedRows = Record<string, string>;

interface AppPhaseResult {
  schema_version: 1;
  phase: SmokePhase;
  status: 'passed' | 'failed';
  pending_change_count: number | null;
  observed: ObservedRows | null;
  error: string | null;
}

// Reports each observation column as the text of the value that the public
// query path returned. An INTEGER outside the safe range arrives tagged.
async function observe(): Promise<ObservedRows> {
  const row = await client.queryOne(packagedSmokeConfig.observe_sql);
  if (row === null) {
    throw new Error('packaged observation row is missing');
  }
  const observed: ObservedRows = {};
  for (const [field, value] of Object.entries(row)) {
    if (field === 'converged') {
      continue;
    }
    if (typeof value === 'string') {
      observed[field] = value;
    } else if (typeof value === 'number' && Number.isSafeInteger(value)) {
      observed[field] = String(value);
    } else if (isInt64(value)) {
      observed[field] = value.value;
    } else {
      throw new Error(`packaged ${field} has an unexpected value type`);
    }
  }
  return observed;
}

function isInt64(value: unknown): value is SQLiteInt64 {
  return typeof value === 'object' && value !== null && (value as SQLiteInt64).type === 'int64';
}

async function executeAll(statements: readonly string[]) {
  for (const statement of statements) {
    await client.execute(statement);
  }
}

// Waits until the queue is empty and the pulled server total equals the sum
// of the local sets. Only a server rollup and a pull can make them equal.
async function awaitConvergence() {
  const deadline = Date.now() + 90000;
  for (;;) {
    await syncAndWaitForScheduledPullRetry();
    const row = await client.queryOne(packagedSmokeConfig.observe_sql);
    if ((await client.pendingChangeCount()) === 0 && row?.converged === '1') {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error('client did not converge within 90 seconds');
    }
    await new Promise<void>(resolve => setTimeout(resolve, 500));
  }
}

async function reportPhaseResult(result: AppPhaseResult) {
  const response = await fetch(packagedSmokeConfig.result_url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${packagedSmokeConfig.result_token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(result),
  });
  if (!response.ok) {
    throw new Error(`application result collector rejected status ${response.status}`);
  }
}

async function runPackagedSmokePhase(): Promise<AppPhaseResult> {
  let phase: SmokePhase = packagedSmokeConfig.phase;
  let pendingChangeCount: number | null = null;
  let observed: ObservedRows | null = null;
  try {
    await client.initialize();
    const pendingAtLaunch = await client.pendingChangeCount();
    pendingChangeCount = pendingAtLaunch;
    if (pendingAtLaunch > 0) {
      phase = 'resume';
      await startAndWaitForScheduledPullRetry();
      // The harness authors remote rows while this process is dead. Only
      // ordinary synchronization can deliver them to the local query path.
      await awaitConvergence();
      pendingChangeCount = await client.pendingChangeCount();
      observed = await observe();
      await client.stop();
      await client.close();
    } else {
      await startAndWaitForScheduledPullRetry();
      // start() returns after local recovery and runs the first cycle in
      // the background, so the server schema is not applied yet. The
      // dataset inserts require that schema.
      await syncAndWaitForScheduledPullRetry();
      await executeAll(packagedSmokeConfig.initial_sql);
      await awaitConvergence();
      await executeAll(packagedSmokeConfig.durable_sql);
      pendingChangeCount = await client.pendingChangeCount();
      if (pendingChangeCount !== packagedSmokeConfig.durable_sql.length) {
        throw new Error('durable packaged work was not queued');
      }
      observed = await observe();
    }
    return {
      schema_version: 1,
      phase,
      status: 'passed',
      pending_change_count: pendingChangeCount,
      observed,
      error: null,
    };
  } catch (error) {
    return {
      schema_version: 1,
      phase,
      status: 'failed',
      pending_change_count: pendingChangeCount,
      observed: null,
      error: (error instanceof Error ? error.message : String(error)).slice(0, 512),
    };
  }
}

export default function App(): React.JSX.Element {
  const [status, setStatus] = useState('running');

  useEffect(() => {
    void (async () => {
      const result = await runPackagedSmokePhase();
      try {
        await reportPhaseResult(result);
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        setStatus(`application result delivery failed: ${detail}`.slice(0, 1024));
        return;
      }
      if (result.status === 'failed') {
        setStatus(result.error ?? 'packaged smoke failed');
        return;
      }
      setStatus(result.phase === 'initial' ? 'initial-ready' : 'resumed');
    })();
  }, []);

  return (
    <SafeAreaView>
      <Text accessibilityLiveRegion="polite" testID="synchro-consumer-status">
        {status}
      </Text>
    </SafeAreaView>
  );
}
