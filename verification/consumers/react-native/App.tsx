import React, { useEffect, useState } from 'react';
import { SafeAreaView, Text } from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';
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

const AUTHORED_CUSTOMER_NAME = 'Packaged Consumer';

interface ObservedRows {
  customer_name: string;
  ship_address: string;
}

interface AppPhaseResult {
  schema_version: 1;
  phase: SmokePhase;
  status: 'passed' | 'failed';
  pending_change_count: number | null;
  observed: ObservedRows | null;
  error: string | null;
}

async function observe(): Promise<ObservedRows> {
  const customer = await client.queryOne('SELECT name FROM customers WHERE id = ?', [
    packagedSmokeConfig.customer_id,
  ]);
  const order = await client.queryOne('SELECT ship_address FROM orders WHERE id = ?', [
    packagedSmokeConfig.order_id,
  ]);
  if (typeof customer?.name !== 'string' || typeof order?.ship_address !== 'string') {
    throw new Error('packaged rows are missing from the local query path');
  }
  return { customer_name: customer.name, ship_address: order.ship_address };
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
      const durable = await client.queryOne(
        'SELECT ship_address FROM orders WHERE id = ?',
        [packagedSmokeConfig.order_id]
      );
      if (durable?.ship_address !== '{"street":"Packaged Durable"}') {
        throw new Error('durable packaged row was not restored');
      }
      await startAndWaitForScheduledPullRetry();
      // The harness authors a remote customer name while this process is
      // dead. Only ordinary synchronization can deliver it to the local query path.
      const deadline = Date.now() + 90000;
      pendingChangeCount = await client.pendingChangeCount();
      observed = await observe();
      while (pendingChangeCount !== 0 || observed.customer_name === AUTHORED_CUSTOMER_NAME) {
        if (Date.now() >= deadline) {
          throw new Error('resumed client did not converge within 90 seconds');
        }
        await syncAndWaitForScheduledPullRetry();
        pendingChangeCount = await client.pendingChangeCount();
        observed = await observe();
        if (pendingChangeCount !== 0 || observed.customer_name === AUTHORED_CUSTOMER_NAME) {
          await new Promise<void>(resolve => setTimeout(resolve, 500));
        }
      }
      await client.stop();
      await client.close();
    } else {
      await startAndWaitForScheduledPullRetry();
      // start() returns after local recovery and runs the first cycle in
      // the background, so the server schema is not applied yet. The
      // customers insert requires that schema.
      await syncAndWaitForScheduledPullRetry();
      const timestamp = new Date().toISOString();
      await client.execute(
        'INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)',
        [
          packagedSmokeConfig.customer_id,
          packagedSmokeConfig.user_id,
          AUTHORED_CUSTOMER_NAME,
          timestamp,
          timestamp,
        ]
      );
      await client.execute(
        "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
        [
          packagedSmokeConfig.order_id,
          packagedSmokeConfig.customer_id,
          packagedSmokeConfig.user_id,
          '{"street":"Packaged Initial"}',
          timestamp,
          timestamp,
        ]
      );
      await syncAndWaitForScheduledPullRetry();
      pendingChangeCount = await client.pendingChangeCount();
      if (pendingChangeCount !== 0) {
        throw new Error('initial packaged work was not pushed');
      }
      await client.execute(
        'UPDATE orders SET ship_address = ?, updated_at = ? WHERE id = ?',
        [
          '{"street":"Packaged Durable"}',
          new Date().toISOString(),
          packagedSmokeConfig.order_id,
        ]
      );
      const durable = await client.queryOne(
        'SELECT ship_address FROM orders WHERE id = ?',
        [packagedSmokeConfig.order_id]
      );
      if (durable?.ship_address !== '{"street":"Packaged Durable"}') {
        throw new Error('durable packaged row was not queued');
      }
      pendingChangeCount = await client.pendingChangeCount();
      if (pendingChangeCount !== 1) {
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
