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
// ---------------------------------------------------------------------------

import type { RetryPolicy } from "@promin/core";
import type { JournalEntry, ActivityJournalStorage } from "./activity-journal.ts";

// ---------------------------------------------------------------------------
// Ctx types
// ---------------------------------------------------------------------------

/**
 * The yield shape from a journaled step body. The runner reads `.promise`
 * to do the async work; `.name` is preserved for debugging/observability.
 */
export interface ActivityYield {
  readonly _tag: "Activity";
  readonly name: string;
  readonly promise: Promise<unknown>;
}

/** Per-activity configuration — retry for now; Phase 3 adds codec, idempotent, etc. */
export interface ActivityOptions {
  readonly retry?: RetryPolicy<unknown>;
}

/**
 * Context passed to a journaled step body. The ONLY supported way to produce
 * side effects is `ctx.activity()`. Direct I/O in the body will re-fire on
 * replay and cause non-determinism bugs.
 */
export interface JournaledContext<Input, Prev> {
  readonly input: Input;
  readonly prev: Prev;
  readonly workflowId: string;

  /**
   * Record an activity as a journal checkpoint. First run executes `fn`,
   * persists the result, resolves to it. Replay resolves to the persisted
   * value without calling `fn`.
   *
   * Must be consumed with `yield*` — the sub-generator delegates its single
   * yielded promise to the runner and returns the resolved value.
   */
  activity<T>(
    name: string,
    fn: () => T | Promise<T>,
    options?: ActivityOptions,
  ): Generator<ActivityYield, T, T>;
}

/** The body function passed to `.journaled()`. */
export type JournaledStepBody<Input, Prev, Output> = (
  ctx: JournaledContext<Input, Prev>,
  prev: Prev,
) => Generator<ActivityYield, Output, unknown>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Thrown when the engine detects that the step body's structure has diverged
 * from the journaled execution (e.g. an activity was renamed between the
 * journaled run and the replay). Phase 1 catches name mismatches only; Phase 3
 * adds step-type and payload-hash checks.
 */
export class JournalNonDeterminismError extends Error {
  readonly _tag = "JournalNonDeterminismError";
  constructor(
    readonly stepName: string,
    readonly activityIndex: number,
    readonly expected: string,
    readonly actual: string,
  ) {
    super(
      `journaled step "${stepName}" diverged at activity ${activityIndex}: ` +
        `expected "${expected}", got "${actual}"`,
    );
  }
}

/**
 * Thrown when a `.journaled()` step is built on a storage that doesn't
 * implement `ActivityJournalStorage`. Fail-loud at build time rather than
 * silently losing journal entries at runtime.
 */
export class JournalStorageMissingError extends Error {
  readonly _tag = "JournalStorageMissingError";
  constructor(stepName: string) {
    super(
      `journaled step "${stepName}" requires a WorkflowStorage that implements ` +
        `ActivityJournalStorage. Use InMemoryWorkflowStorage or PostgresWorkflowStorage, ` +
        `or extend your custom storage with loadJournal/appendEntry.`,
    );
  }
}

// ---------------------------------------------------------------------------
// Ctx factory — builds the Promise inside activity() and yields it
// ---------------------------------------------------------------------------

function makeCtx<Input, Prev>(params: {
  input: Input;
  prev: Prev;
  workflowId: string;
  stepName: string;
  journal: JournalEntry[];
  storage: ActivityJournalStorage;
}): JournaledContext<Input, Prev> {
  const { input, prev, workflowId, stepName, journal, storage } = params;
  const indexRef = { next: 0 };
  const journalByIndex = new Map(journal.map((e) => [e.activityIndex, e]));

  function* activity<T>(
    name: string,
    fn: () => T | Promise<T>,
    options?: ActivityOptions,
  ): Generator<ActivityYield, T, T> {
    const activityIndex = indexRef.next++;

    // Build the async work for this activity. Replay-or-run is decided here
    // so the runner sees a single awaitable Promise regardless of path.
    const promise = (async (): Promise<T> => {
      const recorded = journalByIndex.get(activityIndex);
      if (recorded) {
        // Replay path — validate determinism, rehydrate value or rethrow error.
        if (recorded.activityName !== name) {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            recorded.activityName,
            name,
          );
        }
        if (recorded.exit.tag === "Failure") {
          throw new Error(recorded.exit.error);
        }
        return recorded.exit.value as T;
      }

      // Fresh-run path — execute with retry, persist (success or failure).
      const runOnce = async (): Promise<T> => (await Promise.resolve(fn())) as T;
      let value: T;
      try {
        value = options?.retry ? await runWithRetry(runOnce, options.retry) : await runOnce();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        await storage.appendEntry({
          workflowId,
          stepName,
          activityIndex,
          activityName: name,
          exit: { tag: "Failure", error: message },
        });
        throw err;
      }
      await storage.appendEntry({
        workflowId,
        stepName,
        activityIndex,
        activityName: name,
        exit: { tag: "Success", value },
      });
      return value;
    })();

    // The runner will await the promise and resume via .next(resolvedValue);
    // that resumed value becomes the result of this `yield` expression, which
    // `yield*` hoists as the sub-generator's return value.
    return yield { _tag: "Activity", name, promise };
  }

  return { input, prev, workflowId, activity };
}

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
  body: JournaledStepBody<Input, Prev, Output>;
}): Promise<Output> {
  const { input, prev, workflowId, stepName, storage, body } = params;

  const journal = await storage.loadJournal(workflowId, stepName);
  const ctx = makeCtx({ input, prev, workflowId, stepName, journal, storage });
  const gen = body(ctx, prev);

  let step: IteratorResult<ActivityYield, Output>;
  try {
    step = gen.next();
  } catch (err) {
    // Body threw synchronously before yielding anything.
    throw err;
  }

  while (!step.done) {
    const yielded = step.value;
    try {
      const resolved = await yielded.promise;
      step = gen.next(resolved as never);
    } catch (err) {
      // Let the body's try/catch handle it if it wants; otherwise re-throw.
      step = gen.throw(err);
    }
  }
  return step.value;
}

// ---------------------------------------------------------------------------
// Local retry runner — intentionally small; mirrors @promin/core pattern.
// ---------------------------------------------------------------------------

async function runWithRetry<T>(fn: () => Promise<T>, policy: RetryPolicy<unknown>): Promise<T> {
  const maxRetries = policy.maxRetries ?? 3;
  const baseDelay = policy.baseDelayMs ?? 100;
  const maxDelay = policy.maxDelayMs ?? Infinity;
  const jitter = policy.jitter ?? false;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= maxRetries) throw err;
      if (policy.when && !policy.when(err)) throw err;
      let delay = Math.min(baseDelay * 2 ** attempt, maxDelay);
      if (jitter) delay *= 0.75 + Math.random() * 0.5;
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw new Error("unreachable");
}
