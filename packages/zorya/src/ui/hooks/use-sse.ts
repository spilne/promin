import { useEffect } from "preact/hooks";
import { api } from "../api/client.ts";

/**
 * Live events for one run. Reads the SSE stream through `fetch` so the
 * dashboard's API key is sent (`EventSource` cannot send headers).
 */
export function useRunEvents<T extends { type?: string } = { type?: string }>(
  runId: string | undefined,
  onEvent: (ev: T) => void,
): void {
  useEffect(() => {
    if (!runId) return;
    return api.subscribeRunEvents<T>(runId, onEvent);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);
}
