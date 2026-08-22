import React, { useEffect, useState } from 'react';
import { SafeAreaView, Text } from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';

const client = new SynchroClient({
  dbPath: 'consumer.db',
  serverURL: 'http://127.0.0.1',
  authProvider: async () => 'unused',
  clientID: '00000000-0000-4000-8000-000000000003',
  appVersion: 'consumer',
});

export default function App(): React.JSX.Element {
  const [status, setStatus] = useState('running');

  useEffect(() => {
    void (async () => {
      try {
        await client.initialize();
        await client.execute(
          'CREATE TABLE IF NOT EXISTS consumer_probe (id TEXT PRIMARY KEY, value TEXT NOT NULL)'
        );
        await client.execute(
          'INSERT OR REPLACE INTO consumer_probe (id, value) VALUES (?, ?)',
          ['probe', 'packaged']
        );
        const row = await client.queryOne(
          'SELECT value FROM consumer_probe WHERE id = ?',
          ['probe']
        );
        if (row?.value !== 'packaged') {
          throw new Error('packaged row was not readable');
        }
        await client.close();
        setStatus('passed');
      } catch (error) {
        setStatus(error instanceof Error ? error.message : String(error));
      }
    })();
  }, []);

  return (
    <SafeAreaView>
      <Text testID="synchro-consumer-status">{status}</Text>
    </SafeAreaView>
  );
}
