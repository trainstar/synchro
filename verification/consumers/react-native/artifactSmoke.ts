import { SynchroClient } from '@trainstar/synchro-react-native';

export function makePackagedClient(): SynchroClient {
  return new SynchroClient({
    dbPath: 'consumer.db',
    serverURL: 'http://127.0.0.1',
    authProvider: async () => 'unused',
    clientID: '00000000-0000-4000-8000-000000000003',
    appVersion: 'consumer',
  });
}
