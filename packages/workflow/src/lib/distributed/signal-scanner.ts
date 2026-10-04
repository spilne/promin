// ---------------------------------------------------------------------------
// SignalScanner — resumes suspended workflows whose signal wait has a
// delivered signal in storage.
//
// Mirror of SleepScanner: SleepScanner wakes runs whose timer is due;
// SignalScanner wakes runs whose `waiting_for_signal` step has a delivered
// signal under the awaited name. Both share the same loop (`ResumeScanner`):
// keyset-paged storage queries, bounded-concurrency resumes, optional leader
// gating, FakeWallClock-driven cadence in tests.
//
// Why a scanner instead of caller responsibility
// ----------------------------------------------
// `WorkflowStorage.deliverSignal` only records the signal; for
// journaled-suspend storages the pending journal entry must also be
// completed (`completeSignal`) and the workflow re-run before the body
// observes the delivery. Without this scanner, every code path that
// calls `deliverSignal` (server routes, custom resolvers, agent-loop
// approval path) has to do those extra two steps itself — easy to
// forget, easy to half-implement. The scanner closes the loop generically
// so a `deliverSignal` call from anywhere just works.
//
// Repeated signal names
// ---------------------
// Signals are a named value on the run: a second delivery under the same
// name replaces the first (last delivery wins), and a fresh run starts with
// none. The scanner resumes a waiting run with the value stored when it
// scans. `completeSignal` returns false when the pending journal entry was
// already completed (concurrent scanner, an earlier in-line resume); the
// run is resumed anyway, and replay picks the completed entry up.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import { isActivityJournalStorage } from "../durable/activity-journal.ts";
import { isJournaledSuspendStorage } from "../durable/activity-journal.ts";
import type { Workflow } from "../durable/durable-pipeline.ts";
import { completeSignal } from "../durable/journaled-step.ts";
import type { WorkflowRunner } from "../durable/workflow-runner.ts";
import type { WorkflowStorage, WorkflowWakeup } from "../durable/workflow-storage.ts";
import type { LeaderElection } from "./leader-election.ts";
import { ResumeScanner } from "./resume-scanner.ts";

export interface SignalScannerConfig {
  /** Workflow storage to scan. */
  readonly storage: WorkflowStorage;
  /** Runner used to resume workflows whose signal has been delivered. */
  readonly runner: WorkflowRunner;
  /** How often to scan (ms). Default 5_000 (5 seconds). */
  readonly scanIntervalMs?: number;
  /**
   * Resolve a pure `Workflow` definition by name. Passed back to the runner
   * for resumption; the scanner doesn't bind storage itself. A name it
   * can't resolve is reported once through `onError` and its runs are
   * skipped.
   */
  readonly resolveWorkflow: (workflowName: string) => Workflow<unknown, unknown> | undefined;
  readonly onResume?: (workflowId: string) => void;
  readonly onError?: (workflowId: string, error: unknown) => void;
  /** Time source. Default `SystemWallClock`. Tests pass `FakeWallClock`. */
  readonly clock?: WallClock;
  /** Resumes run at once, at most. Default: 10. */
  readonly resumeConcurrency?: number;
  /**
   * Only the leader scans. Pass a `LeaseLeaderElection` with key
   * `scannerLeaderKey({ scanner: "signal", namespace })` when several
   * instances run the scanner. Default: every instance scans.
   */
  readonly leaderElection?: LeaderElection;
  /** Runs fetched per storage query. Default: 100. */
  readonly pageSize?: number;
}

export interface SignalScanner {
  /**
   * Start scanning for delivered signals. A failed scan is reported through
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

export class DefaultSignalScanner implements SignalScanner {
  private readonly scanner: ResumeScanner<WorkflowWakeup>;

  constructor(config: SignalScannerConfig) {
    const storage = config.storage;
    const pageSize = config.pageSize ?? 100;
    const journalStorage = isActivityJournalStorage(storage) ? storage : undefined;
    const journaledSuspend =
      journalStorage && isJournaledSuspendStorage(journalStorage) ? journalStorage : undefined;

    this.scanner = new ResumeScanner<WorkflowWakeup>({
      name: "signal-scanner",
      runner: config.runner,
      intervalMs: config.scanIntervalMs ?? 5_000,
      clock: config.clock ?? SystemWallClock,
      resolveWorkflow: config.resolveWorkflow,
      onResume: config.onResume,
      onError: config.onError,
      leaderElection: config.leaderElection,
      resumeConcurrency: config.resumeConcurrency,
      find: async ({ afterWorkflowId }) => {
        if (storage.listSignalWakeups) {
          const rows = await storage.listSignalWakeups({
            limit: pageSize,
            ...(afterWorkflowId !== undefined && { afterWorkflowId }),
          });
          return { rows, done: rows.length < pageSize };
        }
        return { rows: await signalWakeupsByListing({ storage, pageSize }), done: true };
      },
      prepare: async (row) => {
        // Journaled `ctx.signal` waits read the value from their pending
        // journal entry, so complete it before re-running the body. DAG
        // `waitForSignal` steps read `loadSignals` directly.
        if (journaledSuspend && row.signalName !== undefined) {
          await completeSignal({
            storage: journaledSuspend,
            workflowId: row.workflowId,
            stepName: row.stepName,
            signalName: row.signalName,
            value: row.signalPayload,
          });
        }
        return true;
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

export function createSignalScanner(config: SignalScannerConfig): SignalScanner {
  return new DefaultSignalScanner(config);
}

/**
 * Fallback for storages without `listSignalWakeups`: list every suspended
 * run, find its signal wait and look up its signals. Collects every page
 * before anything is resumed, so offset paging can't skip rows.
 */
async function signalWakeupsByListing(params: {
  readonly storage: WorkflowStorage;
  readonly pageSize: number;
}): Promise<WorkflowWakeup[]> {
  const { storage, pageSize } = params;
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
      const waiting = Object.values(wf.steps).find(
        (s) => s.status === "waiting_for_signal" && s.signalName !== undefined,
      );
      if (!waiting) continue;
      const signals = await storage.loadSignals(wf.workflowId);
      const signal = signals.find((s) => s.signalName === waiting.signalName);
      if (!signal) continue;
      due.push({
        workflowId: wf.workflowId,
        workflowName: wf.workflowName,
        input: wf.input,
        ...(wf.version !== undefined && { version: wf.version }),
        stepName: waiting.stepName,
        reason: "signal",
        signalName: signal.signalName,
        signalPayload: signal.payload,
      });
    }
    if (page.length < pageSize) break;
  }
  return due;
}
