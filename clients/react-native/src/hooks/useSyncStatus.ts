import { useState, useEffect } from 'react';
import type { SynchroClient } from '../SynchroClient';
import type { SyncStatus } from '../types';

export function useSyncStatus(client: SynchroClient): SyncStatus {
  const [status, setStatus] = useState<SyncStatus>({
    status: 'uninitialized',
    retryAt: null,
    operation: null,
    failure: null,
  });

  useEffect(() => {
    const unsubscribe = client.onStatusChange(setStatus);
    return unsubscribe;
  }, [client]);

  return status;
}
