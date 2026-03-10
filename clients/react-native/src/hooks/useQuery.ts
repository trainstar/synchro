import { useState, useEffect, useCallback, useRef } from 'react';
import type { SynchroClient } from '../SynchroClient';
import type { Row } from '../types';
import type { SynchroError } from '../errors';

interface UseQueryResult {
  data: Row[];
  loading: boolean;
  error: SynchroError | null;
  refresh: () => void;
}

function arraysEqual<T>(
  a?: readonly T[],
  b?: readonly T[]
): boolean {
  if (a === b) return true;
  if (!a || !b) return !a && !b;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function useStableArray<T>(value?: readonly T[]): readonly T[] | undefined {
  const ref = useRef<readonly T[] | undefined>(value);
  if (!arraysEqual(ref.current, value)) {
    ref.current = value ? [...value] : value;
  }
  return ref.current;
}

export function useQuery(
  client: SynchroClient,
  sql: string,
  params?: unknown[],
  tables?: string[]
): UseQueryResult {
  const [data, setData] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<SynchroError | null>(null);
  const refreshCounter = useRef(0);
  const [, setRefreshTrigger] = useState(0);
  const stableParams = useStableArray(params);
  const stableTables = useStableArray(tables);

  const refresh = useCallback(() => {
    refreshCounter.current += 1;
    setRefreshTrigger(refreshCounter.current);
  }, []);

  useEffect(() => {
    if (stableTables && stableTables.length > 0) {
      // Reactive mode: use watch()
      setLoading(true);
      let firstResult = true;
      const unsubscribe = client.watch(sql, stableParams as unknown[] | undefined, stableTables as string[], (rows) => {
        setData(rows);
        setError(null);
        if (firstResult) {
          setLoading(false);
          firstResult = false;
        }
      });
      return unsubscribe;
    } else {
      // One-shot mode: use query()
      let cancelled = false;
      setLoading(true);
      client
        .query(sql, stableParams as unknown[] | undefined)
        .then((rows) => {
          if (!cancelled) {
            setData(rows);
            setError(null);
            setLoading(false);
          }
        })
        .catch((err) => {
          if (!cancelled) {
            setError(err);
            setLoading(false);
          }
        });
      return () => {
        cancelled = true;
      };
    }
  }, [client, sql, stableParams, stableTables, refreshCounter.current]);

  return { data, loading, error, refresh };
}
