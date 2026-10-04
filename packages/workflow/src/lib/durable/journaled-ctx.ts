// ---------------------------------------------------------------------------
// Journaled ctx assembly — builds the `JournaledContext` of one body run
// from the method modules (`journaled-ctx-*.ts`) over a shared journal
// cursor and compensation stack, plus the small non-journaled members:
// ctx.patched, ctx.metadata, ctx.setQueryHandler, ctx.continueAsNew.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import type { ActivityJournalStorage, JournalEntry } from "./activity-journal.ts";
import { CompensationStack } from "./compensation-stack.ts";
import { WorkflowContinueAsNewError } from "./durable-pipeline-error.ts";
import { JournalCursor } from "./journal-cursor.ts";
import { makeActivity } from "./journaled-ctx-activity.ts";
import { makeChild } from "./journaled-ctx-child.ts";
import { makeLoops, makeParallel, makeProxy } from "./journaled-ctx-compose.ts";
import { makeSignalMethods, makeSleep } from "./journaled-ctx-suspend.ts";
import type { JournaledContext, JournaledCtxEnv, RunChild } from "./journaled-context.ts";
import { registerQueryHandler } from "./query-registry.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";

/** Build the ctx of one body run, and the compensation stack it fills. */
export function makeCtx<Input, Prev>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  journal: JournalEntry[];
  storage: ActivityJournalStorage;
  /** See `JournaledCtxEnv.workflowStorage`. */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — exposed on ctx for user-space logic. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
  /**
   * Default codec inherited from the enclosing step; individual activities
   * may override via `ActivityOptions.codec`. Falls back to
   * LosslessJsonCodec so the fresh-run ↔ replay invariant holds even when
   * the step didn't pass one.
   */
  defaultCodec?: Codec<unknown>;
  /**
   * Workflow-level default for `ActivityOptions.payloadHash`. When `true`,
   * 3-arg `ctx.activity(name, input, fn)` calls hash by default; per-call
   * `payloadHash: false` overrides. The 2-arg form is unaffected.
   */
  defaultPayloadHash?: boolean;
  /** Executes a child workflow inline; `ctx.child()` throws without it. */
  runChild?: RunChild;
  /**
   * Snapshot of workflow metadata as of body start. `ctx.metadata.get()`
   * returns this layered with any in-body `set/merge` writes; storage is
   * updated as a side effect of those writes via `workflowStorage`.
   */
  initialMetadata?: Record<string, unknown>;
  /** Time source for sleep / signal deadlines and activity retry backoff. */
  clock?: WallClock;
  /** Fence guard of the run's lock, passed on every journal, suspend and metadata write. */
  guard?: FenceGuard;
}): { ctx: JournaledContext<Input, Prev>; compensations: CompensationStack } {
  const { input, prev, workflowId, stepName, workflowStorage, guard, initialMetadata } = params;
  const cursor = new JournalCursor({
    workflowId,
    stepName,
    journal: params.journal,
    storage: params.storage,
    guard,
  });
  const compensations = new CompensationStack(cursor);
  const env: JournaledCtxEnv = {
    workflowId,
    stepName,
    cursor,
    compensations,
    workflowStorage,
    guard,
    clock: params.clock ?? SystemWallClock,
    stepCodec: params.defaultCodec ?? LosslessJsonCodec,
    defaultPayloadHash: params.defaultPayloadHash,
    runChild: params.runChild,
  };
  const patchSet = new Set(params.patches ?? []);
  const activity = makeActivity(env);
  const { signal, validatedSignal, approval } = makeSignalMethods(env);
  const { dowhile, dountil } = makeLoops({ env, activity });

  // ctx.metadata — an in-process snapshot layered with storage writes.
  // Reads return a copy so callers can't mutate the canonical object. Writes
  // update the snapshot synchronously and dispatch an async storage merge —
  // fire-and-forget, errors logged. Replay re-fires the same writes (same
  // values), idempotent against the storage's merge semantics.
  const metadataState: Record<string, unknown> = { ...initialMetadata };
  const writeMetadataPatch = (patch: Record<string, unknown>): void => {
    if (!workflowStorage) return; // tests that drive runJournaledStep without WorkflowStorage skip persistence
    workflowStorage.setWorkflowMetadata(workflowId, patch, guard).catch((err) => {
      console.warn(
        `[ctx.metadata] failed to persist for workflow ${workflowId}:`,
        err instanceof Error ? err.message : String(err),
      );
    });
  };
  const metadata = {
    set(key: string, value: unknown): void {
      if (value === null) delete metadataState[key];
      else metadataState[key] = value;
      writeMetadataPatch({ [key]: value });
    },
    merge(patch: Record<string, unknown>): void {
      for (const [k, v] of Object.entries(patch)) {
        if (v === null) delete metadataState[k];
        else metadataState[k] = v;
      }
      writeMetadataPatch(patch);
    },
    get(): Record<string, unknown> {
      return { ...metadataState };
    },
  };

  const ctx: JournaledContext<Input, Prev> = {
    input,
    prev,
    workflowId,
    isReplay: cursor.isReplay,
    workflowVersion: params.workflowVersion,
    activity,
    sleep: makeSleep(env),
    signal,
    validatedSignal,
    approval,
    // Pure set membership. Returns false (not throws) for names not in the
    // currently-running definition's patches array — this is load-bearing
    // for the "same code file, different versions" pattern:
    //
    //   if (ctx.patched("new-pricing")) {
    //     // v2 code path (patches = ["new-pricing"])
    //   } else {
    //     // v1 code path (patches = [])
    //   }
    //
    // A throw-on-unknown design would break v1's false branch. Typo catching
    // is a linter concern, not a runtime one.
    patched: (name: string): boolean => patchSet.has(name),
    parallel: makeParallel(env),
    child: makeChild(env),
    dowhile,
    dountil,
    metadata,
    proxy: makeProxy(activity),
    setQueryHandler: <R>(name: string, handler: (args?: unknown) => R | Promise<R>): void => {
      // Process-local registry — query handlers close over the body's
      // in-memory state, so they're inherently per-process. The worker
      // control socket reads from the same registry to answer queries
      // routed in from the server.
      registerQueryHandler(workflowId, name, handler as (args?: unknown) => unknown);
    },
    continueAsNew: (nextInput: unknown): never => {
      throw new WorkflowContinueAsNewError({
        workflowId,
        nextInput,
        message: `Workflow "${workflowId}" requested continue-as-new`,
      });
    },
  };
  return { ctx, compensations };
}
