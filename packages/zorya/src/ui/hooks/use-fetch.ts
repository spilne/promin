import { useEffect, useRef, useState } from "preact/hooks";

export interface FetchState<T> {
  data?: T;
  error?: Error;
  loading: boolean;
  refresh: () => void;
}

export function useFetch<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = [],
  pollMs?: number,
): FetchState<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const counter = useRef(0);

  const run = async () => {
    const my = ++counter.current;
    setLoading(true);
    try {
      const d = await fetcher();
      if (counter.current === my) {
        setData(d);
        setError(undefined);
      }
    } catch (e) {
      if (counter.current === my) {
        setError(e instanceof Error ? e : new Error(String(e)));
      }
    } finally {
      if (counter.current === my) setLoading(false);
    }
  };

  useEffect(() => {
    void run();
    if (!pollMs) return;
    const handle = setInterval(() => void run(), pollMs);
    return () => clearInterval(handle);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, pollMs]);

  return { data, error, loading, refresh: run };
}
