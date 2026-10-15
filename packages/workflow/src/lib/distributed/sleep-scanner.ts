// ---------------------------------------------------------------------------
// SleepScanner — resumes suspended workflows whose sleep has expired
//
// Periodically queries for workflows with status "suspended" and steps
// with status "sleeping" whose wakeAt has passed. Resumes them by
// re-running the workflow (engine skips completed steps automatically).
//
// This enables durable sleeps of any duration — minutes to years.
// The workflow process doesn't need to stay running during the sleep.
// ---------------------------------------------------------------------------

import type { WorkflowStorage } from "../durable/workflow-storage.ts";
import type { Workflow } from "../durable/durable-pipeline.ts";
import type { WorkflowRunner } from "../durable/workflow-runner.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import {
  PollLoop,
  describeError,
  isNetworkError,
  type PollLoopErrorInfo,
} from "../shared/poll-loop.ts";

export interface SleepScannerConfig {
  /** Workflow storage to scan for expired sleeps. */
  storage: WorkflowStorage;
  /**
   * Runner used to resume woken workflows. Scanner calls
   * `runner.run({ workflow, workflowId, input })` after `resolveWorkflow`
   * returns a definition.
   */
  runner: WorkflowRunner;
  /** How often to scan (ms). Default: 10_000 (10 seconds). */
  scanIntervalMs?: number;
  /**
   * Resolve a pure `Workflow` definition by name. The scanner passes the
   * returned definition back to the runner for resumption; it doesn't bind
   * storage itself.
   */
  resolveWorkflow: (workflowName: string) => Workflow<unknown, unknown> | undefined;
  /** Called when a workflow is resumed. */
  onResume?: (workflowId: string) => void;
  /** Called when resumption fails. */
  onError?: (workflowId: string, error: unknown) => void;
  /**
   * Time source. Drives the scan-loop cadence + the `wakeAt <= now`
   * check for expired sleeps. Default: `SystemWallClock`. Tests pass a
   * `FakeWallClock` so `advance(ms)` both moves the wake-threshold and
   * re-ticks the scan loop deterministically.
   */
  clock?: WallClock;
}

export interface SleepScanner {
  /**
   * Start scanning for expired sleeps. A failed scan is reported through
   * `onError` and retried with backoff; it never ends the loop. Resolves
   * once the scanner has stopped.
   */
  start(): Promise<void>;
  /** Stop scanning. Resolves once the in-flight scan (if any) has finished. */
  stop(): Promise<void>;
}

export class DefaultSleepScanner implements SleepScanner {
  private readonly storage: WorkflowStorage;
  private readonly runner: WorkflowRunner;
  private readonly scanIntervalMs: number;
  private readonly resolveWorkflow: SleepScannerConfig["resolveWorkflow"];
  private readonly onResume?: SleepScannerConfig["onResume"];
  private readonly onError?: SleepScannerConfig["onError"];
  private readonly clock: WallClock;
  private readonly loop: PollLoop;

  constructor(config: SleepScannerConfig) {
    this.storage = config.storage;
    this.runner = config.runner;
    this.scanIntervalMs = config.scanIntervalMs ?? 10_000;
    this.resolveWorkflow = config.resolveWorkflow;
    this.onResume = config.onResume;
    this.onError = config.onError;
    this.clock = config.clock ?? SystemWallClock;
    this.loop = new PollLoop({
      name: "sleep-scanner",
      intervalMs: this.scanIntervalMs,
      clock: this.clock,
      tick: () => this.scan(),
      onError: (err, info) => this.reportScanError(err, info),
    });
  }

  start(): Promise<void> {
    return this.loop.start();
  }

  /** Stop scanning. Resolves once the in-flight scan (if any) has finished. */
  stop(): Promise<void> {
    return this.loop.stop();
  }

  /**
   * Scan-loop failure: most often storage is briefly unreachable (dev
   * hot-reload, restart). Network blips log tersely for the first few
   * failures, then stay quiet; anything else logs loudly every time.
   * `onError` gets a synthetic `"(scan-loop)"` id with the real error.
   */
  private reportScanError(err: unknown, info: PollLoopErrorInfo): void {
    if (isNetworkError(err)) {
      if (info.consecutiveFailures <= 3) {
        console.warn(`[sleep-scanner] storage unreachable, retrying — ${describeError(err)}`);
      }
    } else {
      console.error("[sleep-scanner] scan failed:", err);
    }
    this.onError?.("(scan-loop)", err);
  }

  private async scan(): Promise<void> {
    const now = this.clock.now();
    let offset = 0;
    const pageSize = 100;

    while (!this.loop.stopRequested) {
      const suspended = await this.storage.listWorkflows({
        status: "suspended",
        limit: pageSize,
        offset,
      });

      for (const wf of suspended) {
        if (this.loop.stopRequested) return;
        for (const step of Object.values(wf.steps)) {
          // Sleeping steps wake on `wakeAt`. Signal-waiting steps with a
          // configured `signalTimeoutAt` also wake — the body's
          // `ctx.signal({ timeout })` self-heals on replay (sees the
          // timeout has passed, completes the journal entry with the
          // timeout outcome, returns).
          const sleepDue = step.status === "sleeping" && step.wakeAt && step.wakeAt <= now;
          const signalTimedOut =
            step.status === "waiting_for_signal" &&
            step.signalTimeoutAt &&
            step.signalTimeoutAt <= now;
          if (sleepDue || signalTimedOut) {
            await this.resumeWorkflow(wf.workflowId, wf.workflowName, wf.input);
            break; // one resume per workflow per scan
          }
        }
      }

      if (suspended.length < pageSize) break;
      offset += pageSize;
    }
  }

  private async resumeWorkflow(
    workflowId: string,
    workflowName: string,
    input: unknown,
  ): Promise<void> {
    const definition = this.resolveWorkflow(workflowName);
    if (!definition) return;

    try {
      await this.runner.run({ workflow: definition, workflowId, input });
      this.onResume?.(workflowId);
    } catch (err) {
      // WorkflowSuspendedError is expected if another sleep follows
      if ((err as any)?._tag === "WorkflowSuspendedError") return;
      this.onError?.(workflowId, err);
    }
  }
}

export function createSleepScanner(config: SleepScannerConfig): SleepScanner {
  return new DefaultSleepScanner(config);
}
