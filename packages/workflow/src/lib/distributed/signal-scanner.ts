// ---------------------------------------------------------------------------
// SignalScanner — resumes suspended workflows whose `ctx.signal` wait has
// a delivered signal in storage.
//
// Mirror of SleepScanner, one notch over: SleepScanner wakes workflows
// whose `wakeAt` has passed; SignalScanner wakes workflows whose
// `waiting_for_signal` step has a matching signal row in
// `storage.loadSignals`. Both share the same shape — periodic poll,
// resolveWorkflow hook, onResume/onError, FakeWallClock-driven cadence in
// tests.
//
// Why a scanner instead of caller responsibility
// ----------------------------------------------
// `WorkflowStorage.deliverSignal` only appends a signal row; for
// journaled-suspend storages the pending journal entry must also be
// completed (`completeSignal`) and the workflow re-run before the body
// observes the delivery. Without this scanner, every code path that
// calls `deliverSignal` (server routes, custom resolvers, agent-loop
// approval path) has to do those extra two steps itself — easy to
// forget, easy to half-implement. The scanner closes the loop generically
// so a `deliverSignal` call from anywhere just works.
//
// Idempotency
// -----------
// `completeSignal` returns `false` when the pending journal entry was
// already completed (concurrent scanner, prior in-line resume). The
// scanner walks every signal matching the waiting step's name until one
// succeeds — so racing scanners / leftover already-consumed signal rows
// don't cause double resumes or false-negatives.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import {
  PollLoop,
  describeError,
  isNetworkError,
  type PollLoopErrorInfo,
} from "../shared/poll-loop.ts";
import { isActivityJournalStorage } from "../durable/activity-journal.ts";
import { isJournaledSuspendStorage } from "../durable/activity-journal.ts";
import type { Workflow } from "../durable/durable-pipeline.ts";
import { completeSignal } from "../durable/journaled-step.ts";
import type { WorkflowRunner } from "../durable/workflow-runner.ts";
import type { WorkflowState } from "../durable/workflow-state.ts";
import type { WorkflowStorage } from "../durable/workflow-storage.ts";

export interface SignalScannerConfig {
  /** Workflow storage to scan. */
  readonly storage: WorkflowStorage;
  /** Runner used to resume workflows whose signal has been delivered. */
  readonly runner: WorkflowRunner;
  /** How often to scan (ms). Default 5_000 (5 seconds). */
  readonly scanIntervalMs?: number;
  /**
   * Resolve a pure `Workflow` definition by name. Passed back to the runner
   * for resumption; the scanner doesn't bind storage itself.
   */
  readonly resolveWorkflow: (workflowName: string) => Workflow<unknown, unknown> | undefined;
  readonly onResume?: (workflowId: string) => void;
  readonly onError?: (workflowId: string, error: unknown) => void;
  /** Time source. Default `SystemWallClock`. Tests pass `FakeWallClock`. */
  readonly clock?: WallClock;
}

export interface SignalScanner {
  /**
   * Start scanning for delivered signals. A failed scan is reported through
   * `onError` and retried with backoff; it never ends the loop. Resolves
   * once the scanner has stopped.
   */
  start(): Promise<void>;
  /** Stop scanning. Resolves once the in-flight scan (if any) has finished. */
  stop(): Promise<void>;
}

export class DefaultSignalScanner implements SignalScanner {
  private readonly storage: WorkflowStorage;
  private readonly runner: WorkflowRunner;
  private readonly scanIntervalMs: number;
  private readonly resolveWorkflow: SignalScannerConfig["resolveWorkflow"];
  private readonly onResume?: SignalScannerConfig["onResume"];
  private readonly onError?: SignalScannerConfig["onError"];
  private readonly clock: WallClock;
  private readonly loop: PollLoop;

  constructor(config: SignalScannerConfig) {
    this.storage = config.storage;
    this.runner = config.runner;
    this.scanIntervalMs = config.scanIntervalMs ?? 5_000;
    this.resolveWorkflow = config.resolveWorkflow;
    this.onResume = config.onResume;
    this.onError = config.onError;
    this.clock = config.clock ?? SystemWallClock;
    this.loop = new PollLoop({
      name: "signal-scanner",
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
        console.warn(`[signal-scanner] storage unreachable, retrying — ${describeError(err)}`);
      }
    } else {
      console.error("[signal-scanner] scan failed:", err);
    }
    this.onError?.("(scan-loop)", err);
  }

  private async scan(): Promise<void> {
    // Resolve once per scan — `completeSignal` only applies to journaled
    // storages; non-journaled backends rely on `runner.run` re-driving
    // the workflow so the body sees the new signal directly.
    const journalStorage = isActivityJournalStorage(this.storage) ? this.storage : undefined;
    const journaledSuspend =
      journalStorage && isJournaledSuspendStorage(journalStorage) ? journalStorage : undefined;

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
        const waiting = findWaitingForSignal(wf);
        if (!waiting) continue;
        const signals = await this.storage.loadSignals(wf.workflowId);
        const matching = signals.filter((s) => s.signalName === waiting.signalName);
        if (matching.length === 0) continue;

        let consumed: boolean;
        if (journaledSuspend) {
          // Walk every matching delivered signal until one completes the
          // pending journal entry. completeSignal returns false when the
          // entry was already completed (concurrent scanner, leftover
          // already-consumed row), so this naturally finds the live one.
          consumed = false;
          for (const sig of matching) {
            const ok = await completeSignal({
              storage: journaledSuspend,
              workflowId: wf.workflowId,
              stepName: waiting.stepName,
              signalName: waiting.signalName,
              value: sig.payload,
            });
            if (ok) {
              consumed = true;
              break;
            }
          }
        } else {
          // Non-journaled: trust runner.run to re-drive the body and let
          // `ctx.signal` consume directly from `loadSignals`.
          consumed = true;
        }

        if (consumed) await this.resumeWorkflow(wf.workflowId, wf.workflowName, wf.input);
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
      // Expected when the workflow suspends again on a different signal /
      // sleep — let it stay suspended; next scan picks it up.
      if ((err as { _tag?: string })?._tag === "WorkflowSuspendedError") return;
      this.onError?.(workflowId, err);
    }
  }
}

export function createSignalScanner(config: SignalScannerConfig): SignalScanner {
  return new DefaultSignalScanner(config);
}

function findWaitingForSignal(wf: WorkflowState): { stepName: string; signalName: string } | null {
  for (const step of Object.values(wf.steps)) {
    if (step.status === "waiting_for_signal" && step.signalName !== undefined) {
      return { stepName: step.stepName, signalName: step.signalName };
    }
  }
  return null;
}
