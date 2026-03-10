import { useState, useEffect } from 'react';
import type { SynchroClient } from '../SynchroClient';

export function usePendingChanges(
  client: SynchroClient,
  pollInterval = 2000
): number {
  const [count, setCount] = useState(0);

  useEffect(() => {
    let active = true;

    const poll = async () => {
      try {
        const c = await client.pendingChangeCount();
        if (active) setCount(c);
      } catch {
        // ignore poll errors
      }
    };

    poll();
    const interval = setInterval(poll, pollInterval);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [client, pollInterval]);

  return count;
}
