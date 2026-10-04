// ---------------------------------------------------------------------------
// ctx.activity — the journaled side effect. Replays a recorded outcome, or
// runs the activity under its retry policy and records the outcome
// (two-phase), and registers its compensation once it has succeeded.
// ---------------------------------------------------------------------------

import { retryAsync, type RetryPolicy } from "../shared/retry-policy.ts";
import type { WallClock } from "../shared/wall-clock.ts";
import { payloadHash as hashPayload } from "@spilne/perfect-core/connect";
import type { JournalExit } from "./activity-journal.ts";
import {
  AmbiguousActivityOutcome,
  RetryableError,
  TerminalError,
} from "./durable-pipeline-error.ts";
import { rehydrateFailure } from "./journal-exit.ts";
import { JournalNonDeterminismError } from "./journal-errors.ts";
import { activityScope, journaledBodyScope } from "./journaled-body-scope.ts";
import type {
  ActivityOptions,
  ActivityYield,
  JournaledContext,
  JournaledCtxEnv,
} from "./journaled-context.ts";

/** The `ctx.activity` overload pair. */
export type ActivityFn = JournaledContext<unknown, unknown>["activity"];

/** Build `ctx.activity` for one body run. */
export function makeActivity(env: JournaledCtxEnv): ActivityFn {
  const { workflowId, stepName, cursor, compensations, clock, stepCodec, defaultPayloadHash } = env;

  function* activity<I, T>(
    name: string,
    argA: I | (() => T | Promise<T>),
    argB?: ActivityOptions<T> | ((input: I) => T | Promise<T>),
    argC?: ActivityOptions<T>,
  ): Generator<ActivityYield, T, T> {
    // Disambiguate the 2-arg vs 3-arg overload.
    //   2-arg: ctx.activity(name, fn, options?)     — argA is fn
    //   3-arg: ctx.activity(name, input, fn, opts?) — argB is fn
    // We key off whether argB is a function: if so, argA is the reified
    // input and argB is the unary activity fn. Otherwise argA must be the
    // zero-arg fn.
    let fn: () => T | Promise<T>;
    let hasInput: boolean;
    let boundInput: I | undefined;
    let options: ActivityOptions<T> | undefined;
    if (typeof argB === "function") {
      hasInput = true;
      boundInput = argA as I;
      const unary = argB as (input: I) => T | Promise<T>;
      fn = () => unary(argA as I);
      options = argC;
    } else {
      if (typeof argA !== "function") {
        throw new TypeError(
          `ctx.activity("${name}"): expected 2-arg (name, fn) or 3-arg (name, input, fn) form; ` +
            `got a non-function as the second argument with no activity function in the third.`,
        );
      }
      hasInput = false;
      boundInput = undefined;
      fn = argA as () => T | Promise<T>;
      options = argB as ActivityOptions<T> | undefined;
    }
    const scope = activityScope.getStore();
    const slot = cursor.allocateSlot();
    const { activityIndex } = slot;
    const codec = options?.codec ?? stepCodec;
    const idempotent = options?.idempotent === true;
    const compensate = options?.compensate;
    // Payload fingerprint — per-activity opt-in wins, else fall back to the
    // pipeline default.
    //   * Explicit `options.payloadHash: true` on the 2-arg form throws —
    //     the caller asked for something they can't have, surface it.
    //   * Workflow-level default on a 2-arg call silently skips instead.
    //     The pipeline flag means "hash where you can"; forcing every
    //     existing 2-arg activity to migrate would make the flag
    //     impractical to enable in a real codebase.
    const explicitHashOpt = options?.payloadHash;
    const wantsHash = explicitHashOpt ?? defaultPayloadHash ?? false;
    if (wantsHash && !hasInput && explicitHashOpt === true) {
      throw new Error(
        `ctx.activity("${name}"): \`payloadHash\` requires the 3-arg form ctx.activity(name, input, fn). ` +
          `The 2-arg form captures inputs inside a closure, so there's nothing separate to hash.`,
      );
    }
    const payloadHashValue = wantsHash && hasInput ? hashPayload(boundInput) : undefined;
    if (compensate && scope) {
      // Compensation indices come from the top-level counter; reserving one
      // while concurrent branches are also bumping the counter is racy and
      // would make replay non-deterministic, so the combination is rejected.
      throw new Error(
        `ctx.activity("${name}"): \`compensate\` is not supported inside a ctx.parallel branch. ` +
          `Hoist the compensation to an activity outside the parallel, or use step-level ` +
          `StepOptions.compensate for rollback.`,
      );
    }

    /**
     * Value of a completed exit: a failure rethrows, a success decodes and
     * registers the compensation (on replay too — a LATER activity in this
     * run might still fail, and the unwind needs the replayed value).
     */
    const settle = (exit: JournalExit): T => {
      if (exit.tag === "Failure") throw rehydrateFailure(exit);
      const value = codec.decode(exit.value) as T;
      if (compensate) {
        compensations.register({
          sourceActivityIndex: activityIndex,
          activityName: name,
          compensate: () => compensate(value),
        });
      }
      return value;
    };

    // Replay-or-run is decided inside the promise so the runner sees a
    // single awaitable regardless of path.
    const promise = (async (): Promise<T> => {
      const recorded = cursor.expectRecorded({ slot, kind: "activity", name });
      if (recorded) {
        // Payload-hash drift check. Fires only when BOTH sides opted in:
        //   - recorded.payloadHash: the original run stored a fingerprint
        //   - payloadHashValue:     this replay also computed one
        // Asymmetric cases (hashing toggled on or off between runs) don't
        // throw — that's a migration, not a bug. Forcing strictness there
        // would trap in-flight workflows whenever the operator flipped the
        // pipeline-level flag. If both hashes exist and they disagree,
        // something fed the same-named activity a different input on replay
        // — the exact silent drift this option is meant to surface.
        if (recorded.payloadHash && payloadHashValue && recorded.payloadHash !== payloadHashValue) {
          throw new JournalNonDeterminismError(
            stepName,
            activityIndex,
            `payloadHash=${recorded.payloadHash}`,
            `payloadHash=${payloadHashValue}`,
          );
        }
        const phase = recorded.phase ?? "completed";
        if (phase === "completed") {
          if (!recorded.exit) {
            throw new Error(
              `journal entry ${activityIndex} for step "${stepName}" is completed but has no exit`,
            );
          }
          return settle(recorded.exit);
        }
        // Pending row on replay — the worker that started this activity
        // crashed between the pending write and the completion write. The
        // side effect may or may not have run.
        if (!idempotent) {
          throw new AmbiguousActivityOutcome({
            workflowId,
            stepName,
            activityIndex,
            activityName: name,
            message:
              `activity "${name}" (step "${stepName}", index ${activityIndex}) was ` +
              `interrupted after starting but before completing, and is not marked ` +
              `idempotent. Inspect the external system and either mark idempotent, ` +
              `compensate, or fail the workflow.`,
          });
        }
        // Idempotent: re-run. Re-writing the pending row is a no-op and the
        // completion updates it with the new exit.
      }

      // Exit the journaledBodyScope before running `fn()` — the scope is
      // meant to flag non-deterministic globals called between `yield*`
      // expressions, NOT the intentional side effects that happen inside an
      // activity. Node's AsyncLocalStorage would otherwise propagate the
      // body scope through every async continuation descending from the
      // generator tick.
      const runOnce = async (): Promise<T> =>
        journaledBodyScope.exit(async () => (await Promise.resolve(fn())) as T);
      const outcome = await cursor.recordOutcome({
        slot,
        kind: "activity",
        name,
        payloadHash: payloadHashValue,
        run: () =>
          options?.retry ? runWithRetry({ fn: runOnce, policy: options.retry, clock }) : runOnce(),
        // Encode for storage; `settle` decodes the stored exit, so fresh-run
        // consumers see the same shape replay would. Without the round trip,
        // a body that yields `new Date()` would see a real Date on fresh run
        // and a stringified one after restart.
        encode: (value) => codec.encode(value),
      });
      if (outcome.kind === "failed") throw outcome.error;
      return settle(outcome.exit);
    })();

    // The runner will await the promise and resume via .next(resolvedValue);
    // that resumed value becomes the result of this `yield` expression, which
    // `yield*` hoists as the sub-generator's return value.
    return yield { _tag: "Activity", name, promise };
  }

  return activity;
}

/**
 * Run an activity body under its retry policy on the shared retry loop.
 * Class-based classification wins over the policy's `when`: a
 * `TerminalError` is never retried, a `RetryableError` always is (while
 * retries remain), so a lax predicate can't re-run the first and a strict
 * one can't skip the second.
 */
function runWithRetry<T>(params: {
  fn: () => Promise<T>;
  policy: RetryPolicy<unknown>;
  clock: WallClock;
}): Promise<T> {
  const { policy } = params;
  const userWhen = policy.when;
  return retryAsync({
    policy: {
      ...policy,
      when: (err) => {
        if (err instanceof TerminalError) return false;
        if (err instanceof RetryableError) return true;
        return userWhen === undefined || userWhen(err);
      },
    },
    clock: params.clock,
    run: () => params.fn(),
  });
}
