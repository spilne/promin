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
import { SystemClock, type Clock } from "@promin/core";

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
   * check for expired sleeps. Default: `SystemClock`. Tests pass a
   * `FakeClock` so `advance(ms)` both moves the wake-threshold and
   * re-ticks the scan loop deterministically.
   */
  clock?: Clock;
}

export interface SleepScanner {
  /** Start scanning for expired sleeps. */
  start(): Promise<void>;
  /** Stop scanning. */
  stop(): Promise<void>;
}

export class DefaultSleepScanner implements SleepScanner {
  private readonly storage: WorkflowStorage;
  private readonly runner: WorkflowRunner;
  private readonly scanIntervalMs: number;
  private readonly resolveWorkflow: SleepScannerConfig["resolveWorkflow"];
  private readonly onResume?: SleepScannerConfig["onResume"];
  private readonly onError?: SleepScannerConfig["onError"];
  private readonly clock: Clock;
  private running = false;

  constructor(config: SleepScannerConfig) {
    this.storage = config.storage;
    this.runner = config.runner;
    this.scanIntervalMs = config.scanIntervalMs ?? 10_000;
    this.resolveWorkflow = config.resolveWorkflow;
    this.onResume = config.onResume;
    this.onError = config.onError;
    this.clock = config.clock ?? SystemClock;
  }

  async start(): Promise<void> {
    this.running = true;
    let consecutiveFailures = 0;
    while (this.running) {
      try {
        await this.scan();
        consecutiveFailures = 0;
      } catch (err) {
        // Don't kill the scan loop on a transient error — most often
        // the server is briefly down (dev hot-reload, restart) and the
        // remote storage RPC throws ConnectionRefused. Log once,
        // optionally bubble to `onError`, and keep polling.
        consecutiveFailures += 1;
        const isNetwork = isNetworkError(err);
        if (isNetwork) {
          // Network blip: terse one-liner the first few times, then
          // stay silent so a long server outage doesn't spam logs.
          if (consecutiveFailures <= 3) {
            console.warn(`[sleep-scanner] storage unreachable, retrying — ${describeError(err)}`);
          }
        } else {
          // Real bug-shaped failure: log loudly every time.
          console.error("[sleep-scanner] scan failed:", err);
        }
        // `onError` is documented for resume-failure attribution. Pass
        // a synthetic id so existing consumers still work, but include
        // the real error.
        this.onError?.("(scan-loop)", err);
      }
      await new Promise<void>((r) => this.clock.setTimeout(() => r(), this.scanIntervalMs));
    }
  }

  async stop(): Promise<void> {
    this.running = false;
  }

  private async scan(): Promise<void> {
    const now = this.clock.now();
    let offset = 0;
    const pageSize = 100;

    while (true) {
      const suspended = await this.storage.listWorkflows({
        status: "suspended",
        limit: pageSize,
        offset,
      });

      for (const wf of suspended) {
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

/**
 * Heuristic — is this error a transport/network blip vs a real bug?
 * Bun and Node throw a few canonical shapes; check both `code` and
 * the message text. Used to choose between terse-log and loud-log.
 */
function isNetworkError(err: unknown): boolean {
  const code = (err as { code?: string })?.code;
  if (code === "ConnectionRefused" || code === "ECONNREFUSED") return true;
  if (code === "ECONNRESET" || code === "ETIMEDOUT") return true;
  if (code === "ENOTFOUND" || code === "EHOSTUNREACH") return true;
  const msg = (err as { message?: string })?.message ?? "";
  return /unable to connect|connection refused|fetch failed|socket hang up/i.test(msg);
}

/** One-line description for the warning log, no stack noise. */
function describeError(err: unknown): string {
  const code = (err as { code?: string })?.code;
  const msg = (err as { message?: string })?.message ?? String(err);
  return code ? `${code}: ${msg.split("\n")[0]}` : (msg.split("\n")[0] ?? "");
}
