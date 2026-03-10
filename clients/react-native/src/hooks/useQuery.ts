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

  const refresh = useCallback(() => {
    refreshCounter.current += 1;
    setRefreshTrigger(refreshCounter.current);
  }, []);

  useEffect(() => {
    if (tables && tables.length > 0) {
      // Reactive mode: use watch()
      setLoading(true);
      let firstResult = true;
      const unsubscribe = client.watch(sql, params, tables, (rows) => {
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
        .query(sql, params)
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, sql, JSON.stringify(params), JSON.stringify(tables), refreshCounter.current]);

  return { data, loading, error, refresh };
}
