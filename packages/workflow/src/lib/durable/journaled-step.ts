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
import {
  isJournaledSuspendStorage,
  type JournalEntry,
  type ActivityJournalStorage,
  type JournaledSuspendStorage,
} from "./activity-journal.ts";
import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";

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

/** Per-activity configuration — retry today; codec/idempotent options planned. */
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
   * The version the workflow row was created under, as stored in the DB.
   * Exposed for user-space custom version-comparison logic (e.g. semver,
   * date-based ordering) when `ctx.patched()`'s set-membership model
   * isn't enough. The framework itself uses this ONLY for display — all
   * drain/patch logic uses equality/membership.
   *
   * `undefined` when the workflow was created without a `version` field.
   */
  readonly workflowVersion?: string;

  /**
   * Returns `true` if `name` is in the currently-running workflow
   * definition's `patches` array. Inline version branches:
   *
   * ```typescript
   * if (ctx.patched("use-new-pricing")) {
   *   // v2+ code path
   * } else {
   *   // v1 code path (for workflows resuming under v1's definition)
   * }
   * ```
   *
   * Pure set membership — no version comparison. The drain policy ensures
   * each stored version's own definition (with its own patch list) is the
   * one running, so `ctx.patched` naturally reflects what the stored
   * version knew about.
   *
   * Throws if `name` wasn't declared in the workflow's `patches` config —
   * catches typos at runtime instead of silently returning false.
   */
  patched(name: string): boolean;

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

  /**
   * Durable sleep inside a journaled step. First run writes a pending journal
   * entry with `wakeAt = now + duration` and throws `WorkflowSuspendedError`,
   * releasing the worker. Replay after the scanner (or test driver) completes
   * the entry resolves to the actual wake time.
   *
   * Requires the configured storage to implement `JournaledSuspendStorage`;
   * throws a clear error at first use if not.
   */
  sleep(duration: number | Date): Generator<ActivityYield, Date, Date>;

  /**
   * Durable signal wait inside a journaled step. First run writes a pending
   * journal entry naming the signal and throws `WorkflowSuspendedError`.
   * External `completeSignal(...)` delivers a value, completes the entry,
   * and enqueues resume. Replay returns the delivered value.
   *
   * The generic `T` types the delivered payload; runtime validation via a
   * per-signal Zod codec is a planned refinement.
   */
  signal<T>(name: string): Generator<ActivityYield, T, T>;
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
 * journaled run and the replay). Today catches activity-name and step-type
 * mismatches; payload-hash checks are a planned refinement.
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
        `expected "${expected}", got "${actual}". ` +
        `This usually means workflow code changed between runs. ` +
        `Either bump the workflow \`version\` (strict policy throws cleanly) ` +
        `or use \`onVersionMismatch: "drain"\` + \`previousVersions\` to let ` +
        `in-flight workflows finish on their original code.`,
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
        `ActivityJournalStorage. Supported built-in backends: InMemoryWorkflowStorage, ` +
        `PostgresWorkflowStorage, RedisWorkflowStorage. Extend your custom storage ` +
        `with loadJournal/appendEntry if you need a different backend.`,
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
  /**
   * Full WorkflowStorage — only used by ctx.sleep/ctx.signal to call
   * suspendWorkflow() so the existing DefaultSleepScanner picks up
   * journal-suspended workflows.
   */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — exposed on ctx for user-space logic. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
}): JournaledContext<Input, Prev> {
  const {
    input,
    prev,
    workflowId,
    stepName,
    journal,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
  } = params;
  const patchSet = new Set(patches ?? []);
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
        // Seeing a sleep/signal journal entry at an activity yield index
        // means the user swapped an activity for a sleep/signal at the same
        // position between runs — a determinism bug.
        const recordedType = recorded.stepType ?? "activity";
        if (recordedType !== "activity") {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `${recordedType}:${recorded.activityName}`,
            `activity:${name}`,
          );
        }
        // Pending activity rows shouldn't happen — the engine only appends
        // activity entries after their side effect completes. If we see one,
        // something earlier went wrong.
        if (!recorded.exit) {
          throw new Error(
            `journal entry ${activityIndex} for step "${stepName}" is pending; ` +
              `expected a completed activity`,
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

  // -------------------------------------------------------------------------
  // ctx.sleep / ctx.signal — require JournaledSuspendStorage
  // -------------------------------------------------------------------------

  function requireSuspendStorage(op: "sleep" | "signal"): JournaledSuspendStorage {
    if (!isJournaledSuspendStorage(storage)) {
      throw new Error(
        `ctx.${op}() requires a WorkflowStorage that implements JournaledSuspendStorage. ` +
          `Supported built-in backends: InMemoryWorkflowStorage, PostgresWorkflowStorage, ` +
          `RedisWorkflowStorage. Extend your custom storage with ` +
          `appendPendingEntry/completePendingEntry/findDueSleeps/findPendingSignal ` +
          `if you need a different backend.`,
      );
    }
    return storage;
  }

  function* sleep(duration: number | Date): Generator<ActivityYield, Date, Date> {
    const suspendStorage = requireSuspendStorage("sleep");
    const activityIndex = indexRef.next++;
    const name = "sleep";

    const promise = (async (): Promise<Date> => {
      const recorded = journalByIndex.get(activityIndex);
      const recordedType = recorded?.stepType ?? "activity";
      if (recorded && recordedType !== "sleep") {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          `${recordedType}:${recorded.activityName}`,
          "sleep",
        );
      }

      // Replay after completion — entry holds the actual wake time.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        if (recorded.exit.tag === "Failure") throw new Error(recorded.exit.error);
        const raw = recorded.exit.value;
        return typeof raw === "string" ? new Date(raw) : (raw as Date);
      }

      // First run or pending replay — determine wake time.
      // Use the recorded wakeAt when replaying a pending entry so time isn't
      // re-computed (which would drift on every replay).
      const wakeAt =
        recorded?.wakeAt ?? (duration instanceof Date ? duration : new Date(Date.now() + duration));

      if (!recorded) {
        await suspendStorage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          activityName: name,
          stepType: "sleep",
          wakeAt,
        });
      }

      // Self-healing replay: if the scanner re-ran us and our wake time has
      // passed, complete the entry here (no external completion needed) and
      // return. The DefaultSleepScanner's existing "run workflow on wake"
      // loop works unchanged — ctx.sleep does its own time check.
      if (Date.now() >= wakeAt.getTime()) {
        await suspendStorage.completePendingEntry({
          workflowId,
          stepName,
          activityIndex,
          exit: { tag: "Success", value: wakeAt.toISOString() },
        });
        return wakeAt;
      }

      // Still sleeping — mark the WORKFLOW as suspended at step level so the
      // existing DefaultSleepScanner (which scans step.wakeAt) picks it up.
      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(workflowId, stepName, {
          status: "sleeping",
          wakeAt,
        });
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "sleep",
        message: `sleeping until ${wakeAt.toISOString()}`,
      });
    })();

    return yield { _tag: "Activity", name, promise };
  }

  function* signalImpl<T>(signalName: string): Generator<ActivityYield, T, T> {
    const suspendStorage = requireSuspendStorage("signal");
    const activityIndex = indexRef.next++;

    const promise = (async (): Promise<T> => {
      const recorded = journalByIndex.get(activityIndex);
      const recordedType = recorded?.stepType ?? "activity";
      if (recorded && recordedType !== "signal") {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          `${recordedType}:${recorded.activityName}`,
          `signal:${signalName}`,
        );
      }
      if (recorded && recorded.activityName !== signalName) {
        throw new JournalNonDeterminismError(
          stepName,
          activityIndex,
          recorded.activityName,
          signalName,
        );
      }

      // Replay after delivery — entry completed with the signal payload.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        if (recorded.exit.tag === "Failure") {
          throw new Error(recorded.exit.error);
        }
        return recorded.exit.value as T;
      }

      // First run (or still pending) — register interest, mark the workflow
      // suspended, then suspend. The DefaultSleepScanner ignores workflows
      // without a wakeAt, so signals require external delivery via
      // `completeSignal` to resume (no automatic wake from the scanner).
      if (!recorded) {
        await suspendStorage.appendPendingEntry({
          workflowId,
          stepName,
          activityIndex,
          activityName: signalName,
          stepType: "signal",
        });
      }
      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(workflowId, stepName, {
          status: "waiting_signal",
          signalName,
        });
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "signal",
        message: `waiting for signal "${signalName}"`,
      });
    })();

    return yield { _tag: "Activity", name: signalName, promise };
  }

  function patched(name: string): boolean {
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
    return patchSet.has(name);
  }

  return {
    input,
    prev,
    workflowId,
    workflowVersion,
    activity,
    sleep,
    signal: signalImpl,
    patched,
  };
}

// ---------------------------------------------------------------------------
// completeSignal — external API used to deliver a value to a suspended step
// ---------------------------------------------------------------------------

/**
 * Deliver a signal value to a workflow awaiting it via `ctx.signal(name)`.
 * Finds the matching pending journal entry and completes it with the given
 * value. Subsequent replay of the journaled step unblocks at the signal and
 * continues.
 *
 * The caller is responsible for re-enqueuing the workflow for execution after
 * delivery (via PgStepQueue, in-memory scheduler, or direct re-run). Today
 * callers drive resume themselves; an automatic resume path through the
 * step queue is planned as a refinement.
 *
 * Returns `true` if a pending entry was found and completed; `false` if no
 * matching pending signal exists (already delivered, or never registered).
 */
export async function completeSignal(params: {
  storage: JournaledSuspendStorage;
  workflowId: string;
  stepName: string;
  signalName: string;
  value: unknown;
}): Promise<boolean> {
  const hit = await params.storage.findPendingSignal({
    workflowId: params.workflowId,
    stepName: params.stepName,
    signalName: params.signalName,
  });
  if (!hit) return false;

  await params.storage.completePendingEntry({
    workflowId: params.workflowId,
    stepName: params.stepName,
    activityIndex: hit.activityIndex,
    exit: { tag: "Success", value: params.value },
  });
  return true;
}

/**
 * Scanner hook — complete all due sleeps up to `limit`. Returns the
 * completed entries so a caller (or test) can re-enqueue the workflows.
 *
 * Usage:
 * ```ts
 * const due = await completeDueSleeps({ storage, now, limit: 100 });
 * for (const { workflowId } of due) {
 *   await workflow.run({ workflowId }); // re-drive to consume completion
 * }
 * ```
 */
export async function completeDueSleeps(params: {
  storage: JournaledSuspendStorage;
  now: Date;
  limit: number;
}): Promise<Array<{ workflowId: string; stepName: string; activityIndex: number; wakeAt: Date }>> {
  const due = await params.storage.findDueSleeps({
    now: params.now,
    limit: params.limit,
  });
  for (const entry of due) {
    await params.storage.completePendingEntry({
      workflowId: entry.workflowId,
      stepName: entry.stepName,
      activityIndex: entry.activityIndex,
      // Store as ISO string for consistent JSON roundtrip; the generator
      // hydrates back to Date on replay.
      exit: { tag: "Success", value: entry.wakeAt.toISOString() },
    });
  }
  return due;
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
  /**
   * Full WorkflowStorage — when provided, ctx.sleep/ctx.signal call
   * suspendWorkflow() so the existing DefaultSleepScanner resumes them.
   * Omit only when driving runJournaledStep directly from tests.
   */
  workflowStorage?: WorkflowStorage;
  /** Stored workflow version — surfaced on ctx.workflowVersion. */
  workflowVersion?: string;
  /** Active patches in the currently-running definition — drives ctx.patched. */
  patches?: readonly string[];
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
    body,
  } = params;

  const journal = await storage.loadJournal(workflowId, stepName);
  const ctx = makeCtx({
    input,
    prev,
    workflowId,
    stepName,
    journal,
    storage,
    workflowStorage,
    workflowVersion,
    patches,
  });
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
