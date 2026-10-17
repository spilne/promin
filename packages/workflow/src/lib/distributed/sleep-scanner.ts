// ---------------------------------------------------------------------------
// SleepScanner — resumes suspended workflows whose sleep has expired
//
// Periodically asks storage for suspended runs with a due timer: a sleeping
// step whose `wakeAt` has passed, or a signal wait whose `signalTimeoutAt`
// has passed. Resumes them by re-running the workflow (the engine skips
// completed steps automatically), several at once.
//
// This enables durable sleeps of any duration — minutes to years.
// The workflow process doesn't need to stay running during the sleep.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowWakeup } from "../durable/workflow-storage.ts";
import type { Workflow } from "../durable/durable-pipeline.ts";
import type { WorkflowRunner } from "../durable/workflow-runner.ts";
import type { WorkflowState } from "../durable/workflow-state.ts";
import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { LeaderElection } from "./leader-election.ts";
import { ResumeScanner } from "./resume-scanner.ts";

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
   * storage itself. A name it can't resolve is reported once through
   * `onError` and its runs are skipped.
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
  /**
   * Resumes run at once, at most. A resume lasts until the run next
   * suspends or finishes, so one long resume holds one slot instead of
   * every other wake-up. Default: 10.
   */
  resumeConcurrency?: number;
  /**
   * Only the leader scans. Pass a `LeaseLeaderElection` with key
   * `scannerLeaderKey({ scanner: "sleep", namespace })` when several
   * instances run the scanner. Default: every instance scans.
   */
  leaderElection?: LeaderElection;
  /** Runs fetched per storage query. Default: 100. */
  pageSize?: number;
}

export interface SleepScanner {
  /**
   * Start scanning for expired sleeps. A failed scan is reported through
   * `onError` and retried with backoff; it never ends the loop. Resolves
   * once the scanner has stopped.
   */
  start(): Promise<void>;
  /**
   * Stop scanning. Resolves once the in-flight scan and the resumes it
   * started have finished, and leadership (if any) has been released.
   */
  stop(): Promise<void>;
}

export class DefaultSleepScanner implements SleepScanner {
  private readonly scanner: ResumeScanner<WorkflowWakeup>;

  constructor(config: SleepScannerConfig) {
    const storage = config.storage;
    const clock = config.clock ?? SystemWallClock;
    const pageSize = config.pageSize ?? 100;
    this.scanner = new ResumeScanner<WorkflowWakeup>({
      name: "sleep-scanner",
      runner: config.runner,
      intervalMs: config.scanIntervalMs ?? 10_000,
      clock,
      resolveWorkflow: config.resolveWorkflow,
      onResume: config.onResume,
      onError: config.onError,
      leaderElection: config.leaderElection,
      resumeConcurrency: config.resumeConcurrency,
      find: async ({ afterWorkflowId }) => {
        const now = clock.now();
        if (storage.listDueTimers) {
          const rows = await storage.listDueTimers({
            now,
            limit: pageSize,
            ...(afterWorkflowId !== undefined && { afterWorkflowId }),
          });
          return { rows, done: rows.length < pageSize };
        }
        return { rows: await dueTimersByListing({ storage, now, pageSize }), done: true };
      },
    });
  }

  start(): Promise<void> {
    return this.scanner.start();
  }

  stop(): Promise<void> {
    return this.scanner.stop();
  }
}

/**
 * Fallback for storages without `listDueTimers`: list every suspended run
 * and test its steps. Collects every page before anything is resumed, so
 * resumes changing statuses can't make offset paging skip rows.
 */
async function dueTimersByListing(params: {
  readonly storage: WorkflowStorage;
  readonly now: Date;
  readonly pageSize: number;
}): Promise<WorkflowWakeup[]> {
  const { storage, now, pageSize } = params;
  const due: WorkflowWakeup[] = [];
  for (let offset = 0; ; offset += pageSize) {
    const page = await storage.listWorkflows({
      status: "suspended",
      limit: pageSize,
      offset,
      orderBy: "createdAt",
      orderDir: "asc",
    });
    for (const wf of page) {
      const wakeup = dueTimerOf(wf, now);
      if (wakeup) due.push(wakeup);
    }
    if (page.length < pageSize) break;
  }
  return due;
}

function dueTimerOf(wf: WorkflowState, now: Date): WorkflowWakeup | undefined {
  const base = {
    workflowId: wf.workflowId,
    workflowName: wf.workflowName,
    input: wf.input,
    ...(wf.version !== undefined && { version: wf.version }),
  };
  for (const step of Object.values(wf.steps)) {
    if (step.status === "sleeping" && step.wakeAt && step.wakeAt <= now) {
      return { ...base, stepName: step.stepName, reason: "sleep" };
    }
    if (
      step.status === "waiting_for_signal" &&
      step.signalTimeoutAt &&
      step.signalTimeoutAt <= now
    ) {
      return {
        ...base,
        stepName: step.stepName,
        reason: "signal-timeout",
        ...(step.signalName !== undefined && { signalName: step.signalName }),
      };
    }
  }
  return undefined;
}

export function createSleepScanner(config: SleepScannerConfig): SleepScanner {
  return new DefaultSleepScanner(config);
}
