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
    while (this.running) {
      await this.scan();
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
          if (step.status === "sleeping" && step.wakeAt && step.wakeAt <= now) {
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
