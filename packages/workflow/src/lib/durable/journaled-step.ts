// ---------------------------------------------------------------------------
// JournaledStep — generator-based step body with per-activity replay
//
// A `.journaled()` step body is a generator function. Each `yield* ctx.activity(name, fn)`
// is a checkpoint: on first run the activity fn executes and its result is
// appended to the activity journal; on retry/replay the generator re-runs from
// the top, and already-journaled activities return their recorded value
// without re-executing.
//
// Execution model (like `co` / `redux-saga`):
//   - Body is a sync generator (`function*`), NOT async generator.
//   - ctx.activity() builds the Promise for the side effect + journal write,
//     then yields the promise.
//   - The runner awaits each yielded promise and resumes the generator with
//     the resolved value via `.next(value)`. Errors flow via `.throw(err)`.
//
// Why sync generator: the body signature `Generator<ActivityYield, R, any>`
// makes raw `await` a compile-time type error — every side effect must go
// through `ctx.activity`, which is what makes the journal/replay safe.
//
// Failures and step attempts:
//   - An activity (or child) that fails after its own `retry` is journaled
//     as a Failure with the error's `_tag` / `name` and fields. While the
//     body that saw it is still in flight (suspended on sleep/signal, or
//     re-driven after a crash) replay rethrows an error of the same kind, so
//     a body that catches the failure takes the same branch again.
//   - When a failure escapes the body, the step attempt is over. After the
//     compensation unwind, the engine discards the attempt's recorded
//     failures (failed compensations included, so the next unwind retries
//     them), plus every activity whose compensation completed together with
//     that compensation's row: its effect was rolled back. The next attempt
//     (step-level `retry`, or a later resume of the failed workflow)
//     re-executes those activities. Successful activities that were not
//     rolled back still replay and never run twice.
//   - Control-flow and engine-integrity exits (suspend, continue-as-new,
//     tripwire, non-determinism, ambiguous outcome, lock loss) discard
//     nothing.
//
// Module map:
//   journaled-context.ts       the ctx types (`JournaledContext`, options)
//   journaled-ctx.ts           assembles one run's ctx
//   journaled-ctx-activity.ts  ctx.activity
//   journaled-ctx-suspend.ts   ctx.sleep / signal / validatedSignal / approval
//   journaled-ctx-child.ts     ctx.child
//   journaled-ctx-compose.ts   ctx.parallel / dowhile / dountil / proxy
//   journal-cursor.ts          slot allocation, replay checks, journal writes
//   compensation-stack.ts      unwind policy, compensations, failure discard
//   journal-wakeups.ts         completeSignal / completeDueSleeps
//   journal-errors.ts          JournalNonDeterminismError, JournalStorageMissingError
// This file is the runner entry, `runJournaledStep`, and re-exports the rest
// of the public surface.
// ---------------------------------------------------------------------------

import type { WallClock } from "../shared/wall-clock.ts";
import type { Codec } from "@spilne/perfect-core/connect";
import { isActivityJournalStorage, type ActivityJournalStorage } from "./activity-journal.ts";
import { discardFailedAttempt, runsCompensations } from "./compensation-stack.ts";
import { JournalStorageMissingError } from "./journal-errors.ts";
import { activityScope, journaledBodyScope } from "./journaled-body-scope.ts";
import type { ActivityYield, JournaledStepBody, RunChild } from "./journaled-context.ts";
import { makeCtx } from "./journaled-ctx.ts";
import type { WorkflowMetadataRef } from "./step-definition.ts";
import type { FenceGuard, WorkflowStorage } from "./workflow-storage.ts";

export type {
  ActivityOptions,
  ActivityYield,
  JournaledContext,
  JournaledStepBody,
} from "./journaled-context.ts";
export { JournalNonDeterminismError, JournalStorageMissingError } from "./journal-errors.ts";
export { completeDueSleeps, completeSignal } from "./journal-wakeups.ts";

// ---------------------------------------------------------------------------
// Runner — drive the generator, await each yielded promise, relay the value
// ---------------------------------------------------------------------------

/**
 * Drive a journaled step body to completion. Loads the existing journal,
 * runs the generator from the top, awaits each yielded activity promise
 * (which handles replay-or-execute internally), and resumes the generator
 * with the resolved value.
 *
 * Safe to call multiple times for the same (workflowId, stepName) — each
 * call re-runs deterministically against the current journal state.
 */
export async function runJournaledStep<Input, Prev, Output>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  storage: ActivityJournalStorage;
  /**
   * Full WorkflowStorage — when provided, ctx.sleep / ctx.signal / ctx.child
   * call suspendWorkflow() so the scanners resume them, and ctx.metadata
   * persists. Omit only when driving runJournaledStep directly from tests.
   */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — surfaced on ctx.workflowVersion. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
  /**
   * Default codec for activities inside this step. Each activity can still
   * override via its own options. Defaults to LosslessJsonCodec so fresh-run
   * values round-trip to the same shape replay would produce.
   */
  codec?: Codec<unknown>;
  /**
   * Workflow-level default for `ActivityOptions.payloadHash`. When `true`,
   * every 3-arg `ctx.activity(name, input, fn)` in this step's body
   * fingerprints its input by default; per-activity `payloadHash: false`
   * still opts out. The 2-arg form is unaffected (no reified input to hash).
   */
  payloadHash?: boolean;
  /**
   * Executes a child workflow inline. Wired automatically when called through
   * `WorkflowRunner` / the `.journaled()` builder; pass a stub in unit tests
   * that call `runJournaledStep` directly and want to exercise `ctx.child`.
   */
  runChild?: RunChild;
  /**
   * Time source for `ctx.sleep` / `ctx.signal` deadlines and activity retry
   * backoff. Wired from the runner's clock; default `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Fence guard of the runner's lock on this run, passed on the
   * journal, suspend and metadata writes the step body makes.
   */
  guard?: FenceGuard;
  /**
   * The run's metadata as the runner holds it. Seeds `ctx.metadata`
   * without re-reading the run, and is kept current with the body's
   * `ctx.metadata` writes. Without it, the run is loaded for its metadata.
   */
  workflowMetadata?: WorkflowMetadataRef;
  body: JournaledStepBody<Input, Prev, Output>;
}): Promise<Output> {
  const {
    input,
    prev,
    workflowId,
    stepName,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
    codec,
    payloadHash,
    runChild,
    clock,
    guard,
    workflowMetadata,
    body,
  } = params;

  if (!isActivityJournalStorage(storage)) throw new JournalStorageMissingError(stepName);
  const journal = await storage.loadJournal(workflowId, stepName);
  // Workflow metadata snapshot for `ctx.metadata.get()` — reads are
  // synchronous from the body, so we materialize the snapshot up front:
  // the runner's copy when it passed one, else a load of the run. Writes
  // go through `setWorkflowMetadata` independently.
  const initialMetadata =
    workflowMetadata !== undefined
      ? workflowMetadata.current
      : workflowStorage
        ? (await workflowStorage.loadWorkflow(workflowId))?.metadata
        : undefined;
  const { ctx, compensations } = makeCtx({
    input,
    prev,
    workflowId,
    stepName,
    journal,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
    defaultCodec: codec,
    defaultPayloadHash: payloadHash,
    runChild,
    ...(clock !== undefined && { clock }),
    ...(guard !== undefined && { guard }),
    ...(initialMetadata !== undefined && { initialMetadata }),
    ...(workflowMetadata !== undefined && { metadataRef: workflowMetadata }),
  });
  const gen = body(ctx, prev);

  // Code between `yield*` expressions inside the generator runs when we call
  // gen.next / gen.throw. Running each tick inside `journaledBodyScope` lets
  // dev tooling (e.g. instrumentNonDeterminism) detect direct Date.now /
  // Math.random calls from the body. Activity fn bodies run outside this
  // scope — they're supposed to touch the outside world.
  const bodyCtx = { stepName };
  const tick = <R>(fn: () => R): R => journaledBodyScope.run(bodyCtx, fn);

  /** Drive the generator until it returns, propagating or catching errors. */
  const driveBody = async (): Promise<Output> => {
    let step: IteratorResult<ActivityYield, Output>;
    step = tick(() => gen.next());
    while (!step.done) {
      const yielded = step.value;
      try {
        const resolved = await yielded.promise;
        step = tick(() => gen.next(resolved as never));
      } catch (err) {
        // Let the body's try/catch handle it if it wants; otherwise re-throw.
        step = tick(() => gen.throw(err));
      }
    }
    return step.value;
  };

  try {
    // Drive outside any enclosing parallel-branch scope: a child workflow
    // started from a branch (ctx.child → runChild → runJournaledStep) must
    // allocate its slots from its own top-level counter, not the parent's
    // branch.
    return await activityScope.exit(() => driveBody());
  } catch (bodyError) {
    if (runsCompensations(bodyError)) {
      const rolledBack = await compensations.unwind();
      await discardFailedAttempt({
        storage,
        workflowId,
        stepName,
        rolledBack,
        ...(guard !== undefined && { guard }),
      });
    }
    throw bodyError;
  }
}
