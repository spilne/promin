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
// calls `deliverSignal` (server routes, custom resolvers, agent
// approval path) has to do those extra two steps itself — easy to
// forget, easy to half-implement. The scanner closes the loop generically
// so a `deliverSignal` call from anywhere just works.
//
// Updates
// -------
// A run holding an unanswered update request (`workflow.update:<id>`, see
// `updates/update-protocol.ts`) is resumed too, when the storage can list
// them (`listUpdateWakeups`): the journaled body applies the update at its
// next update point and answers it, so the run is found only until then. A
// storage without that query serves updates to runs that are running, and
// to suspended ones once something else resumes them.
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
import type { Wakeups } from "../shared/wakeups.ts";
import { hasCapability } from "../durable/storage/capabilities.ts";
import { completeSignal } from "../durable/journaled-step.ts";
import type { WorkflowStorage, WorkflowWakeup } from "../durable/workflow-storage.ts";
import type { LeaderElection } from "./leader-election.ts";
import { ResumeScanner, scannerTargetOf, type ScannerTarget } from "./resume-scanner.ts";

/**
 * Signal scanner config: the common knobs plus what to do with a run whose
 * signal arrived — `{ runner, resolveWorkflow }` resumes it in this
 * process, `{ wake }` hands it over (e.g. enqueues it for a worker that
 * hosts the workflow). A journaled signal wait's pending entry is completed
 * before either.
 */
export type SignalScannerConfig = SignalScannerOptions & ScannerTarget;

export interface SignalScannerOptions {
  /** Workflow storage to scan. */
  readonly storage: WorkflowStorage;
  /** How often to scan (ms). Default 5_000 (5 seconds). */
  readonly scanIntervalMs?: number;
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
  /**
   * A `"signals"` wake (a signal was delivered) scans now instead of after
   * `scanIntervalMs`. A latency hint only: the interval scan still finds
   * every delivery.
   */
  readonly wakeups?: Wakeups;
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
  /** Scan now instead of after the current wait (e.g. right after a delivery). */
  wake(): void;
}

export class DefaultSignalScanner implements SignalScanner {
  private readonly scanner: ResumeScanner<WorkflowWakeup>;
  private readonly wakeups: Wakeups | undefined;

  constructor(config: SignalScannerConfig) {
    const storage = config.storage;
    this.wakeups = config.wakeups;
    const pageSize = config.pageSize ?? 100;
    const journaledSuspend = hasCapability(storage, "journal") ? storage : undefined;

    this.scanner = new ResumeScanner<WorkflowWakeup>({
      name: "signal-scanner",
      target: scannerTargetOf(config),
      intervalMs: config.scanIntervalMs ?? 5_000,
      clock: config.clock ?? SystemWallClock,
      onResume: config.onResume,
      onError: config.onError,
      leaderElection: config.leaderElection,
      resumeConcurrency: config.resumeConcurrency,
      find: async ({ afterWorkflowId }) => {
        // Runs holding an update request nobody has applied are woken too,
        // so the journaled body applies it at its next update point. Both
        // queries page by `workflowId`; their rows are merged in that order
        // (a run in both is resumed once, as a signal wake).
        const updates = await findUpdateWakeups({ storage, pageSize, afterWorkflowId, config });
        if (hasCapability(storage, "signalWakeups")) {
          const signals = await storage.listSignalWakeups({
            limit: pageSize,
            ...(afterWorkflowId !== undefined && { afterWorkflowId }),
          });
          if (updates.length === 0) return { rows: signals, done: signals.length < pageSize };
          const merged = mergeByWorkflowId({ primary: signals, secondary: updates });
          return {
            rows: merged.slice(0, pageSize),
            done:
              signals.length < pageSize && updates.length < pageSize && merged.length <= pageSize,
          };
        }
        const rows = await signalWakeupsByListing({ storage, pageSize });
        if (updates.length === 0) return { rows, done: true };
        const merged = mergeByWorkflowId({ primary: rows, secondary: updates });
        return {
          rows: merged.slice(0, pageSize),
          done: updates.length < pageSize && merged.length <= pageSize,
        };
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

  async start(): Promise<void> {
    const unsubscribe = this.wakeups?.onWake({ topic: "signals", handler: () => this.wake() });
    try {
      await this.scanner.start();
    } finally {
      unsubscribe?.();
    }
  }

  stop(): Promise<void> {
    return this.scanner.stop();
  }

  wake(): void {
    this.scanner.wake();
  }
}

export function createSignalScanner(config: SignalScannerConfig): SignalScanner {
  return new DefaultSignalScanner(config);
}

/**
 * Runs with an unapplied update request. A failing query (a remote server
 * that predates updates) is reported and skipped, so signal wakes go on.
 */
async function findUpdateWakeups(params: {
  readonly storage: WorkflowStorage;
  readonly pageSize: number;
  readonly afterWorkflowId: string | undefined;
  readonly config: SignalScannerOptions;
}): Promise<WorkflowWakeup[]> {
  const { storage, pageSize, afterWorkflowId } = params;
  if (!hasCapability(storage, "updateWakeups")) return [];
  try {
    return await storage.listUpdateWakeups({
      limit: pageSize,
      ...(afterWorkflowId !== undefined && { afterWorkflowId }),
    });
  } catch (error) {
    params.config.onError?.("(update-wakeups)", error);
    return [];
  }
}

/** `primary` and `secondary` rows in `workflowId` order, one row per run (the primary's wins). */
function mergeByWorkflowId(params: {
  readonly primary: readonly WorkflowWakeup[];
  readonly secondary: readonly WorkflowWakeup[];
}): WorkflowWakeup[] {
  const byRun = new Map<string, WorkflowWakeup>();
  for (const row of params.secondary) byRun.set(row.workflowId, row);
  for (const row of params.primary) byRun.set(row.workflowId, row);
  return [...byRun.values()].sort((a, b) =>
    a.workflowId < b.workflowId ? -1 : a.workflowId > b.workflowId ? 1 : 0,
  );
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
