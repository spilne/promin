// ---------------------------------------------------------------------------
// `startScanLoop` — the polling loop every folder scanner (agents, skills,
// fragments) runs on. One scan at a time: a tick that fires while the
// previous scan is still running joins it instead of starting another. A
// failed timer tick is reported to `onError` and the loop keeps polling —
// it never surfaces as an unhandled rejection. `tick()` runs a scan now
// and rejects with its error, for callers that await the first scan.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";

export interface ScanLoopHandle<TTick> {
  /** Stop polling. Idempotent. */
  stop(): void;
  /** Run one scan now (or join the one in flight). Rejects when it fails. */
  tick(): Promise<TTick>;
}

/** What one scan-loop tick did. Shared by the agent, skill and fragment loops. */
export interface ScanLoopTick {
  readonly added: string[];
  /** Every id the scan found — the file-managed set. */
  readonly upserted: string[];
  /** Ids whose file changed (a new file version was stored). */
  readonly changed: string[];
  readonly deleted: string[];
  readonly warnings: string[];
  /** Files that could not be read or imported; sync deletes were skipped. */
  readonly errors: string[];
  readonly durationMs: number;
}

export interface ScanLoopConfig<TTick> {
  /** One scan + reconcile. */
  readonly run: () => Promise<TTick>;
  /** Poll interval in ms. Default: `5_000`. */
  readonly intervalMs?: number;
  /** Timer source. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /** A failed timer tick lands here. Default: `console.error` with `label`. */
  readonly onError?: (err: unknown) => void;
  /** Names the loop in the default error log. */
  readonly label?: string;
}

export function startScanLoop<TTick>(config: ScanLoopConfig<TTick>): ScanLoopHandle<TTick> {
  const clock = config.clock ?? SystemWallClock;
  const onError =
    config.onError ??
    ((err: unknown) => console.error(`[${config.label ?? "scan-loop"}] scan failed`, err));
  let inFlight: Promise<TTick> | null = null;
  let stopped = false;

  function tick(): Promise<TTick> {
    if (inFlight) return inFlight;
    const p = config.run().finally(() => {
      inFlight = null;
    });
    inFlight = p;
    return p;
  }

  const handle = clock.setInterval(() => {
    if (stopped) return;
    tick().catch((err: unknown) => {
      try {
        onError(err);
      } catch {
        // A throwing error hook must not turn into an unhandled rejection.
      }
    });
  }, config.intervalMs ?? 5_000);
  handle.unref?.();

  return {
    stop() {
      if (stopped) return;
      stopped = true;
      handle.clear();
    },
    tick,
  };
}
