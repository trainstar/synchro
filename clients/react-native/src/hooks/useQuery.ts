import { useState, useEffect, useCallback, useRef } from 'react';
import type { SynchroClient } from '../SynchroClient';
import type { AsyncUnsubscribe, Row, SQLiteBindValue } from '../types';
import type { SynchroError } from '../errors';

interface UseQueryResult {
  data: Row[];
  loading: boolean;
  error: SynchroError | null;
  refresh: () => void;
}

function arraysEqual(
  a?: readonly SQLiteBindValue[],
  b?: readonly SQLiteBindValue[]
): boolean {
  if (a === b) return true;
  if (!a || !b) return !a && !b;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    const left = a[i];
    const right = b[i];
    if (left === right) continue;
    if (
      typeof left !== 'object' || left === null ||
      typeof right !== 'object' || right === null
    ) return false;
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    if (
      leftKeys.length !== 2 || rightKeys.length !== 2 ||
      !leftKeys.includes('type') || !rightKeys.includes('type')
    ) return false;
    if (left.type === 'int64' && right.type === 'int64') {
      if (!leftKeys.includes('value') || !rightKeys.includes('value')) return false;
      if (typeof left.value !== 'string' || left.value !== right.value) return false;
    } else if (left.type === 'bytes' && right.type === 'bytes') {
      if (!leftKeys.includes('base64') || !rightKeys.includes('base64')) return false;
      if (typeof left.base64 !== 'string' || left.base64 !== right.base64) return false;
    } else {
      return false;
    }
  }
  return true;
}

function useStableArray<T extends SQLiteBindValue>(value?: readonly T[]): readonly T[] | undefined {
  const ref = useRef<readonly T[] | undefined>(value);
  if (!arraysEqual(ref.current, value)) {
    ref.current = value ? [...value] : value;
  }
  return ref.current;
}

export function useQuery(
  client: SynchroClient,
  sql: string,
  params?: SQLiteBindValue[],
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
      let disposed = false;
      let unsubscribe: AsyncUnsubscribe | null = null;
      client
        .watch(sql, stableParams, stableTables as string[], (rows) => {
          if (disposed) return;
          setData(rows);
          setError(null);
          if (firstResult) {
            setLoading(false);
            firstResult = false;
          }
        })
        .then((remove) => {
          if (disposed) {
            void remove().catch(() => {});
          } else {
            unsubscribe = remove;
          }
        })
        .catch((err) => {
          if (!disposed) {
            setError(err);
            setLoading(false);
          }
        });
      return () => {
        disposed = true;
        if (unsubscribe) {
          void unsubscribe().catch(() => {});
        }
      };
    } else {
      // One-shot mode: use query()
      let cancelled = false;
      setLoading(true);
      client
        .query(sql, stableParams)
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
