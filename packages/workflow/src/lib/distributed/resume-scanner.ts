// ---------------------------------------------------------------------------
// ResumeScanner — the loop the sleep and signal scanners share.
//
// Each scan (on a PollLoop) asks a `find` function for due runs page by page
// (keyset, so resuming rows between pages never skips one) and hands each
// run to a bounded pool of resumes. A resume is `runner.run(...)`, which
// lasts until the run next suspends or finishes, so resumes run concurrently
// up to `resumeConcurrency`: one long resume holds one slot, not the scan.
// A run already being resumed is not resumed again. With a `leaderElection`
// only the leader scans, so N instances don't run N scans that collide on
// run locks.
// ---------------------------------------------------------------------------

import type { Workflow } from "../durable/durable-pipeline.ts";
import type { WorkflowRunner } from "../durable/workflow-runner.ts";
import type { WorkflowWakeup } from "../durable/workflow-storage.ts";
import { PollLoop, describeError, isNetworkError } from "../shared/poll-loop.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import type { LeaderElection } from "./leader-election.ts";

/** A run a scanner found due, before its definition is resolved. */
export type DueRun = Pick<WorkflowWakeup, "workflowId" | "workflowName" | "input">;

/** Default number of resumes a scanner runs at once. */
export const DEFAULT_RESUME_CONCURRENCY = 10;

export class ResumeScanner<Row extends DueRun> {
  private readonly name: string;
  private readonly runner: WorkflowRunner;
  private readonly resolveWorkflow: (
    workflowName: string,
  ) => Workflow<unknown, unknown> | undefined;
  private readonly find: (params: { afterWorkflowId?: string }) => Promise<{
    readonly rows: readonly Row[];
    readonly done: boolean;
  }>;
  private readonly prepare?: (row: Row) => Promise<boolean>;
  private readonly onResume?: (workflowId: string) => void;
  private readonly onError?: (workflowId: string, error: unknown) => void;
  private readonly leaderElection?: LeaderElection;
  private readonly concurrency: number;
  private readonly loop: PollLoop;
  private isLeader = false;
  /** Runs being resumed now, by workflow id. */
  private readonly inFlight = new Map<string, Promise<void>>();
  private slotWaiters: (() => void)[] = [];
  /** Workflow names already reported as unresolvable. */
  private readonly unknownReported = new Set<string>();

  constructor(params: {
    readonly name: string;
    readonly runner: WorkflowRunner;
    readonly intervalMs: number;
    readonly clock: WallClock;
    readonly resolveWorkflow: (workflowName: string) => Workflow<unknown, unknown> | undefined;
    /**
     * One page of due runs after `afterWorkflowId` (keyset), with `done`
     * set on the last page.
     */
    readonly find: (params: { afterWorkflowId?: string }) => Promise<{
      readonly rows: readonly Row[];
      readonly done: boolean;
    }>;
    /**
     * Work to do before resuming (e.g. completing a journaled signal
     * entry). Returning false skips the resume.
     */
    readonly prepare?: (row: Row) => Promise<boolean>;
    readonly onResume?: (workflowId: string) => void;
    readonly onError?: (workflowId: string, error: unknown) => void;
    readonly leaderElection?: LeaderElection;
    readonly resumeConcurrency?: number;
  }) {
    this.name = params.name;
    this.runner = params.runner;
    this.resolveWorkflow = params.resolveWorkflow;
    this.find = params.find;
    this.prepare = params.prepare;
    this.onResume = params.onResume;
    this.onError = params.onError;
    this.leaderElection = params.leaderElection;
    this.concurrency = Math.max(1, params.resumeConcurrency ?? DEFAULT_RESUME_CONCURRENCY);
    this.loop = new PollLoop({
      name: params.name,
      intervalMs: params.intervalMs,
      clock: params.clock,
      tick: () => this.scan(),
      onError: (err, info) => {
        if (isNetworkError(err)) {
          if (info.consecutiveFailures <= 3) {
            console.warn(`[${this.name}] storage unreachable, retrying — ${describeError(err)}`);
          }
        } else {
          console.error(`[${this.name}] scan failed:`, err);
        }
        this.onError?.("(scan-loop)", err);
      },
    });
  }

  /** Runs being resumed right now. */
  get resuming(): number {
    return this.inFlight.size;
  }

  async start(): Promise<void> {
    try {
      await this.loop.start();
    } finally {
      await this.releaseLeadership();
    }
  }

  /**
   * Stop scanning: resolves once the in-flight scan and the resumes it
   * started have finished, and leadership (if any) has been released.
   */
  async stop(): Promise<void> {
    await this.loop.stop();
    await Promise.all(this.inFlight.values());
    await this.releaseLeadership();
  }

  private async releaseLeadership(): Promise<void> {
    if (!this.isLeader || !this.leaderElection) return;
    this.isLeader = false;
    try {
      await this.leaderElection.release();
    } catch (err) {
      this.onError?.("(scan-loop)", err);
    }
  }

  private async scan(): Promise<void> {
    if (this.leaderElection) {
      this.isLeader = await this.leaderElection.tryAcquire();
      if (!this.isLeader) return;
    }
    let afterWorkflowId: string | undefined;
    while (!this.loop.stopRequested) {
      const page = await this.find(afterWorkflowId === undefined ? {} : { afterWorkflowId });
      for (const row of page.rows) {
        if (this.loop.stopRequested) return;
        await this.dispatch(row);
      }
      const last = page.rows[page.rows.length - 1];
      if (page.done || !last) return;
      afterWorkflowId = last.workflowId;
    }
  }

  /** Start resuming `row` once a slot is free; doesn't wait for the resume. */
  private async dispatch(row: Row): Promise<void> {
    if (this.inFlight.has(row.workflowId)) return;
    const definition = this.resolveWorkflow(row.workflowName);
    if (!definition) {
      if (!this.unknownReported.has(row.workflowName)) {
        this.unknownReported.add(row.workflowName);
        this.onError?.(
          row.workflowId,
          new Error(
            `[${this.name}] no definition for workflow "${row.workflowName}"; its due runs ` +
              `are skipped until resolveWorkflow knows it`,
          ),
        );
      }
      return;
    }
    while (this.inFlight.size >= this.concurrency) {
      await new Promise<void>((resolve) => this.slotWaiters.push(resolve));
    }
    if (this.inFlight.has(row.workflowId)) return;
    const done = this.resume(row, definition).finally(() => {
      this.inFlight.delete(row.workflowId);
      const waiters = this.slotWaiters;
      this.slotWaiters = [];
      for (const wake of waiters) wake();
    });
    this.inFlight.set(row.workflowId, done);
  }

  private async resume(row: Row, definition: Workflow<unknown, unknown>): Promise<void> {
    try {
      if (this.prepare && !(await this.prepare(row))) return;
      await this.runner.run({ workflow: definition, workflowId: row.workflowId, input: row.input });
      this.onResume?.(row.workflowId);
    } catch (err) {
      // Expected when the run suspends again (another sleep / signal).
      if ((err as { _tag?: string } | null)?._tag === "WorkflowSuspendedError") return;
      this.onError?.(row.workflowId, err);
    }
  }
}
