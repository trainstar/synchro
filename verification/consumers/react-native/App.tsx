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
    const retryCompleted = await waitForCondition(async () => {
      const current = await client.getSyncStatus();
      if (current.status === 'error' || current.status === 'stopped') {
        throw error;
      }
      return current.status === 'ready';
    });
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

export default function App(): React.JSX.Element {
  const [status, setStatus] = useState('running');

  useEffect(() => {
    void (async () => {
      try {
        await client.initialize();
        const pendingAtLaunch = await client.pendingChangeCount();
        if (pendingAtLaunch > 0) {
          const durable = await client.queryOne(
            'SELECT ship_address FROM orders WHERE id = ?',
            [packagedSmokeConfig.order_id]
          );
          if (durable?.ship_address !== '{"street":"Packaged Durable"}') {
            throw new Error('durable packaged row was not restored');
          }
          await startAndWaitForScheduledPullRetry();
          await syncAndWaitForScheduledPullRetry();
          if ((await client.pendingChangeCount()) !== 0) {
            throw new Error('durable packaged work was not drained');
          }
          await client.stop();
          await client.close();
          setStatus('resumed');
          return;
        }

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
            'Packaged Consumer',
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
        if ((await client.pendingChangeCount()) !== 0) {
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
        if ((await client.pendingChangeCount()) !== 1) {
          throw new Error('durable packaged work was not queued');
        }
        setStatus('initial-ready');
      } catch (error) {
        setStatus(error instanceof Error ? error.message : String(error));
      }
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
