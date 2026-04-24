import { useEffect } from "preact/hooks";

export function useSse<T = unknown>(url: string | undefined, onEvent: (ev: T) => void): void {
  useEffect(() => {
    if (!url) return;
    const es = new EventSource(url);
    es.onmessage = (e) => {
      try {
        onEvent(JSON.parse(e.data) as T);
      } catch {
        // ignore malformed frame
      }
    };
    es.onerror = () => {
      // Let browser retry automatically; close on permanent errors after short delay.
    };
    return () => es.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url]);
}
