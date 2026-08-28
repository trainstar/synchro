import React, { useEffect, useState } from 'react';
import { SafeAreaView, Text } from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';
import { SynchroInspection } from '@trainstar/synchro-react-native/inspection';
import { packagedSmokeConfig } from './packagedSmokeConfig';

const client = new SynchroClient({
  dbPath: 'consumer.db',
  serverURL: packagedSmokeConfig.server_url,
  authProvider: async () => packagedSmokeConfig.token,
  clientID: packagedSmokeConfig.client_id,
  platform: packagedSmokeConfig.platform,
  appVersion: '0.3.0',
  syncInterval: 3600,
  pushDebounce: 3600,
  maxRetryAttempts: 1,
});
const inspection = new SynchroInspection(client, {
  transportObservationCapacity: 256,
});

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
          await client.start();
          await client.syncNow();
          if ((await client.pendingChangeCount()) !== 0) {
            throw new Error('durable packaged work was not drained');
          }
          await client.stop();
          await client.close();
          setStatus('resumed');
          return;
        }

        await client.start();
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
        await client.syncNow();
        if ((await client.pendingChangeCount()) !== 0) {
          throw new Error('initial packaged work was not pushed');
        }
        const snapshot = await inspection.transportObservations();
        if (snapshot.overflowed) {
          throw new Error('packaged transport observations overflowed');
        }
        for (const operation of ['connect', 'push', 'pull'] as const) {
          if (
            !snapshot.observations.some(
              observation =>
                observation.operationClass === operation &&
                observation.statusCode >= 200 &&
                observation.statusCode < 300
            )
          ) {
            throw new Error(`packaged ${operation} was not observed`);
          }
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
