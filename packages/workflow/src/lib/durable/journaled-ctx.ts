// ---------------------------------------------------------------------------
// Journaled ctx assembly — builds the `JournaledContext` of one body run
// from the method modules (`journaled-ctx-*.ts`) over a shared journal
// cursor and compensation stack, plus the small non-journaled members:
// ctx.patched, ctx.metadata, ctx.setQueryHandler, ctx.continueAsNew.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import type { JournalStore, JournalEntry } from "./activity-journal.ts";
import { CompensationStack } from "./compensation-stack.ts";
import { WorkflowContinueAsNewError } from "./durable-pipeline-error.ts";
import { JournalCursor } from "./journal-cursor.ts";
import { makeActivity } from "./journaled-ctx-activity.ts";
import { makeChild } from "./journaled-ctx-child.ts";
import { makeLoops, makeParallel, makeProxy } from "./journaled-ctx-compose.ts";
import { makeSignalMethods, makeSleep } from "./journaled-ctx-suspend.ts";
import type { JournaledContext, JournaledCtxEnv, RunChild } from "./journaled-context.ts";
import { UpdateInbox, type UpdateEvent, type UpdateHandlerOptions } from "./journaled-updates.ts";
import { registerQueryHandler } from "./query-registry.ts";
import type { UpdateType } from "../updates/update-protocol.ts";
import type { WorkflowMetadataRef } from "./step-definition.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";

/** Build the ctx of one body run, and the compensation stack it fills. */
export function makeCtx<Input, Prev>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  journal: JournalEntry[];
  storage: JournalStore;
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
  /**
   * The runner's copy of the run's metadata: every `set/merge` replaces its
   * `current` with the merged metadata, so later steps of the run see it.
   */
  metadataRef?: WorkflowMetadataRef;
  /** Time source for sleep / signal deadlines and activity retry backoff. */
  clock?: WallClock;
  /** Fence guard of the run's lock, passed on every journal, suspend and metadata write. */
  guard?: FenceGuard;
  /**
   * Called when a `ctx.metadata` write fails with an error other than a lost
   * fence (which fails the step instead). Default: `console.warn`.
   */
  onMetadataWriteError?: (params: { error: unknown; keys: readonly string[] }) => void;
  /** Called with each update this body applies or rejects (the runner's tracer). */
  onUpdate?: (event: UpdateEvent) => void;
}): {
  ctx: JournaledContext<Input, Prev>;
  compensations: CompensationStack;
  /** The body's update handlers and update points (see `journaled-updates.ts`). */
  updates: UpdateInbox;
  /**
   * Wait for the `ctx.metadata` writes in flight; rejects with the
   * `FenceTokenMismatchError` of a write the lock holder lost the fence for.
   */
  settleMetadata: () => Promise<void>;
} {
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
  const updates = new UpdateInbox(env, params.onUpdate);
  const patchSet = new Set(params.patches ?? []);
  const activity = makeActivity(env);
  const { signal, validatedSignal, approval } = makeSignalMethods(env);
  const { dowhile, dountil } = makeLoops({ env, activity });

  // ctx.metadata — an in-process snapshot layered with storage writes.
  // Reads return a copy so callers can't mutate the canonical object. Writes
  // update the snapshot synchronously and dispatch an async storage merge,
  // so the call itself never throws. A failed write is never silent:
  //   - a `FenceTokenMismatchError` (the run's lock was lost) fails the step
  //     when the step settles its writes (`settleMetadata`), like a journal
  //     write under a lost lock: the stale holder must stop, and the new
  //     holder re-drives the step;
  //   - any other error is reported through `onMetadataWriteError` (the
  //     runner's `onMetadataWriteError` hook) and does not fail the step:
  //     metadata is a dashboard surface, not durable state the run depends
  //     on, and replay re-fires the same writes. The in-process snapshot
  //     (and the runner's copy) keep the value, so later steps of this
  //     execution still see it even though storage does not.
  const metadataState: Record<string, unknown> = { ...initialMetadata };
  const pendingWrites = new Set<Promise<void>>();
  let lostFence: unknown;
  const reportWriteError =
    params.onMetadataWriteError ??
    (({ error }: { error: unknown; keys: readonly string[] }) =>
      console.warn(`[ctx.metadata] failed to persist for workflow ${workflowId}:`, error));
  const writeMetadataPatch = (patch: Record<string, unknown>): void => {
    const ref = params.metadataRef;
    if (ref) {
      const merged: Record<string, unknown> = { ...ref.current };
      for (const [k, v] of Object.entries(patch)) {
        if (v === null) delete merged[k];
        else merged[k] = v;
      }
      ref.current = merged;
    }
    if (!workflowStorage) return; // tests that drive runJournaledStep without WorkflowStorage skip persistence
    const write: Promise<void> = workflowStorage
      .setWorkflowMetadata({ workflowId, patch, guard })
      .then(
        () => undefined,
        (error: unknown) => {
          if (
            (error as { _tag?: unknown } | null | undefined)?._tag === "FenceTokenMismatchError"
          ) {
            lostFence ??= error;
            return;
          }
          try {
            reportWriteError({ error, keys: Object.keys(patch) });
          } catch {
            // a failing reporter has nowhere left to report to
          }
        },
      )
      .finally(() => pendingWrites.delete(write));
    pendingWrites.add(write);
  };
  const settleMetadata = async (): Promise<void> => {
    while (pendingWrites.size > 0) await Promise.all([...pendingWrites]);
    if (lostFence !== undefined) throw lostFence;
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
    setUpdateHandler: (
      target: UpdateType<unknown, unknown> | string,
      handler: (args: never) => unknown,
      options?: UpdateHandlerOptions<never>,
    ): void => {
      updates.register({
        name: typeof target === "string" ? target : target.name,
        ...(typeof target !== "string" && { update: target as UpdateType<never, unknown> }),
        handler,
        ...(options !== undefined && { options }),
      });
    },
    continueAsNew: (nextInput: unknown): never => {
      throw new WorkflowContinueAsNewError({
        workflowId,
        nextInput,
        message: `Workflow "${workflowId}" requested continue-as-new`,
      });
    },
  };
  return { ctx, compensations, updates, settleMetadata };
}
