import { useState, useEffect } from 'react';
import type { SynchroClient } from '../SynchroClient';
import type { SyncStatus } from '../types';
import { mapNativeError, type SynchroError } from '../errors';

const uninitialized: SyncStatus = {
  status: 'uninitialized',
  retryAt: null,
  operation: null,
  failure: null,
};

export function useSyncStatus(client: SynchroClient): SyncStatus {
  const [snapshot, setSnapshot] = useState<{
    client: SynchroClient;
    status: SyncStatus;
    error: SynchroError | null;
  }>({ client, status: uninitialized, error: null });

  useEffect(() => {
    let active = true;
    let receivedEvent = false;
    setSnapshot({ client, status: uninitialized, error: null });
    const unsubscribe = client.onStatusChange((status) => {
      if (!active) return;
      receivedEvent = true;
      setSnapshot({ client, status, error: null });
    });
    client.getSyncStatus().then(
      (status) => {
        if (active && !receivedEvent) {
          setSnapshot({ client, status, error: null });
        }
      },
      (error: unknown) => {
        if (active && !receivedEvent) {
          setSnapshot({ client, status: uninitialized, error: mapNativeError(error) });
        }
      }
    );
    return () => {
      active = false;
      unsubscribe();
    };
  }, [client]);

  if (snapshot.client !== client) return uninitialized;
  if (snapshot.error) throw snapshot.error;
  return snapshot.status;
}
