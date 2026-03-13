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
import type { WorkflowDefinition } from "../durable/durable-pipeline.ts";

export interface SleepScannerConfig {
  /** Workflow storage to scan for expired sleeps. */
  storage: WorkflowStorage;
  /** How often to scan (ms). Default: 10_000 (10 seconds). */
  scanIntervalMs?: number;
  /**
   * Resolve a workflow definition by name.
   * The scanner needs the definition to call .run() for resumption.
   */
  resolveWorkflow: (workflowName: string) => WorkflowDefinition<unknown, unknown> | undefined;
  /** Called when a workflow is resumed. */
  onResume?: (workflowId: string) => void;
  /** Called when resumption fails. */
  onError?: (workflowId: string, error: unknown) => void;
}

export interface SleepScanner {
  /** Start scanning for expired sleeps. */
  start(): Promise<void>;
  /** Stop scanning. */
  stop(): Promise<void>;
}

export class DefaultSleepScanner implements SleepScanner {
  private readonly storage: WorkflowStorage;
  private readonly scanIntervalMs: number;
  private readonly resolveWorkflow: SleepScannerConfig["resolveWorkflow"];
  private readonly onResume?: SleepScannerConfig["onResume"];
  private readonly onError?: SleepScannerConfig["onError"];
  private running = false;

  constructor(config: SleepScannerConfig) {
    this.storage = config.storage;
    this.scanIntervalMs = config.scanIntervalMs ?? 10_000;
    this.resolveWorkflow = config.resolveWorkflow;
    this.onResume = config.onResume;
    this.onError = config.onError;
  }

  async start(): Promise<void> {
    this.running = true;
    while (this.running) {
      await this.scan();
      await new Promise((r) => setTimeout(r, this.scanIntervalMs));
    }
  }

  async stop(): Promise<void> {
    this.running = false;
  }

  private async scan(): Promise<void> {
    const now = new Date();
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
      await definition.run({ workflowId, input });
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
