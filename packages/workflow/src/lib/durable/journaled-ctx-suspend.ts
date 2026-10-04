// ---------------------------------------------------------------------------
// ctx.sleep / ctx.signal / ctx.validatedSignal / ctx.approval — durable
// waits. Each writes a pending journal entry, marks the workflow suspended
// so the scanners resume it, and throws `WorkflowSuspendedError`; the replay
// after the wake time or delivery returns the recorded outcome.
// ---------------------------------------------------------------------------

import type { JournalExit } from "./activity-journal.ts";
import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import {
  decodeSignalExitValue,
  rehydrateFailure,
  timedOutSignalExitValue,
} from "./journal-exit.ts";
import { JournalNonDeterminismError } from "./journal-errors.ts";
import type {
  ActivityYield,
  JournaledContext,
  JournaledCtxEnv,
  TimedSignalOutcome,
} from "./journaled-context.ts";
import {
  approvalSignal,
  type ApprovalDecision,
  type SignalType,
} from "../signals/define-signal.ts";

/** Build `ctx.sleep` for one body run. */
export function makeSleep(env: JournaledCtxEnv): JournaledContext<unknown, unknown>["sleep"] {
  const { workflowId, stepName, cursor, workflowStorage, guard, clock } = env;

  return function* sleep(duration: number | Date): Generator<ActivityYield, Date, Date> {
    const slot = cursor.allocateSlot({ suspendOrChild: true });
    const name = "sleep";

    const promise = (async (): Promise<Date> => {
      const recorded = cursor.expectRecorded({ slot, kind: "sleep", name });

      const wokeAt = (exit: JournalExit): Date => {
        if (exit.tag === "Failure") throw rehydrateFailure(exit);
        const raw = exit.value;
        return typeof raw === "string" ? new Date(raw) : (raw as Date);
      };

      // Replay after completion — entry holds the actual wake time.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        return wokeAt(recorded.exit);
      }

      // First run or pending replay — determine wake time.
      // Use the recorded wakeAt when replaying a pending entry so time isn't
      // re-computed (which would drift on every replay).
      const wakeAt =
        recorded?.wakeAt ??
        (duration instanceof Date ? duration : new Date(clock.currentTimeMs() + duration));

      if (!recorded) {
        await cursor.writePending({ slot, kind: "sleep", name, wakeAt });
      }

      // Self-healing replay: if the scanner re-ran us and our wake time has
      // passed, complete the entry here (no external completion needed) and
      // return. The DefaultSleepScanner's existing "run workflow on wake"
      // loop works unchanged — ctx.sleep does its own time check.
      // `completeDueSleeps` may complete the entry at the same moment; both
      // write the same wake time, but the stored exit is what replay sees.
      if (clock.currentTimeMs() >= wakeAt.getTime()) {
        const stored = await cursor.complete({
          slot,
          exit: { tag: "Success", value: wakeAt.toISOString() },
          readBack: false,
        });
        return wokeAt(stored.exit);
      }

      // Still sleeping — mark the WORKFLOW as suspended at step level so the
      // existing DefaultSleepScanner (which scans step.wakeAt) picks it up.
      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(
          workflowId,
          stepName,
          { status: "sleeping", wakeAt },
          guard,
        );
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "sleep",
        message: `sleeping until ${wakeAt.toISOString()}`,
      });
    })();

    return yield { _tag: "Activity", name, promise };
  };
}

/** `ctx.signal`, `ctx.validatedSignal` and `ctx.approval` for one body run. */
export interface SignalMethods {
  readonly signal: JournaledContext<unknown, unknown>["signal"];
  readonly validatedSignal: JournaledContext<unknown, unknown>["validatedSignal"];
  readonly approval: JournaledContext<unknown, unknown>["approval"];
}

/** Build the signal waits for one body run. */
export function makeSignalMethods(env: JournaledCtxEnv): SignalMethods {
  const { workflowId, stepName, cursor, workflowStorage, guard, clock } = env;

  function* signalImpl<T>(
    signalName: string,
    options?: {
      readonly timeout?: number | Date;
      /**
       * Internal — JSON Schema snapshot for the suspended signal. Persisted
       * onto `step.signalJsonSchema` so server-side delivery can validate
       * payloads against the shape this suspend point waited on, even if
       * the SignalType definition later evolves. Not exposed on the public
       * `ctx.signal` overload — `ctx.validatedSignal` / `ctx.approval` pass
       * it in.
       */
      readonly jsonSchema?: unknown;
    },
  ): Generator<ActivityYield, T | TimedSignalOutcome<T>, unknown> {
    const slot = cursor.allocateSlot({ suspendOrChild: true });
    const { activityIndex } = slot;
    const hasTimeout = options?.timeout !== undefined;

    const promise = (async (): Promise<T | TimedSignalOutcome<T>> => {
      const recorded = cursor.expectRecorded({ slot, kind: "signal", name: signalName });

      // Result of a completed entry. The stored value is tagged delivered /
      // timeout (see `journal-exit.ts`); untagged legacy rows decode by
      // their old shape. With a timeout configured a delivery is wrapped in
      // the `{ ok: true, value }` envelope so `result.ok` works uniformly.
      const outcomeOf = (exit: JournalExit): T | TimedSignalOutcome<T> => {
        if (exit.tag === "Failure") throw rehydrateFailure(exit);
        const outcome = decodeSignalExitValue({ stored: exit.value, hasTimeout });
        if (outcome.kind === "timeout") {
          if (!hasTimeout) {
            // The run that recorded this waited with a timeout; this code
            // waits without one.
            throw new JournalNonDeterminismError(
              stepName,
              activityIndex,
              `signal:${signalName} (timed out)`,
              `signal:${signalName} (no timeout)`,
            );
          }
          return { ok: false, error: "timeout" };
        }
        const value = outcome.value as T;
        return hasTimeout ? { ok: true, value } : value;
      };

      // Replay after delivery or timeout.
      if (recorded && recorded.phase === "completed" && recorded.exit) {
        return outcomeOf(recorded.exit);
      }

      // First run (or still pending) — register interest, mark the workflow
      // suspended, then suspend. The DefaultSleepScanner skips workflows
      // without a wakeAt, so an unbounded signal still requires external
      // delivery via `completeSignal`; with a timeout configured, the
      // wakeAt is set so the scanner can complete the entry on expiry.
      // Use the recorded wakeAt on replay so time isn't re-computed (which
      // would drift on every replay).
      const wakeAt = recorded?.wakeAt
        ? recorded.wakeAt
        : options?.timeout !== undefined
          ? options.timeout instanceof Date
            ? options.timeout
            : new Date(clock.currentTimeMs() + options.timeout)
          : undefined;

      if (!recorded) {
        await cursor.writePending({
          slot,
          kind: "signal",
          name: signalName,
          ...(wakeAt && { wakeAt }),
        });
      }

      // Self-healing replay: if the scanner re-ran us and our timeout has
      // passed without a delivery, complete the entry with the timeout
      // outcome and return. Mirrors `ctx.sleep` — the scanner wakes us, the
      // body decides what to do. A delivery that completed the entry after
      // the journal was loaded wins the race: take its value, the same one
      // every replay will see.
      if (wakeAt && clock.currentTimeMs() >= wakeAt.getTime()) {
        const stored = await cursor.complete({
          slot,
          exit: { tag: "Success", value: timedOutSignalExitValue() },
          readBack: true,
        });
        return outcomeOf(stored.exit);
      }

      if (workflowStorage) {
        await workflowStorage.suspendWorkflow(
          workflowId,
          stepName,
          {
            status: "waiting_for_signal",
            signalName,
            ...(wakeAt && { signalTimeoutAt: wakeAt }),
            // Schema snapshot — a dedicated field on `StepState`. The server's
            // delivery path (POST /api/runs/:id/signal + the public token
            // complete route) reads `step.signalJsonSchema` and validates
            // inbound payloads against it before calling deliverSignal. Lives
            // on the suspend record (not the journal entry) so it survives a
            // SignalType definition change between suspend and delivery.
            ...(options?.jsonSchema !== undefined && {
              signalJsonSchema: options.jsonSchema,
            }),
          },
          guard,
        );
      }
      throw new WorkflowSuspendedError({
        workflowId,
        stepName,
        reason: "signal",
        message: wakeAt
          ? `waiting for signal "${signalName}" (timeout at ${wakeAt.toISOString()})`
          : `waiting for signal "${signalName}"`,
      });
    })();

    return (yield { _tag: "Activity", name: signalName, promise }) as T | TimedSignalOutcome<T>;
  }

  // Typed wrapper around signalImpl, keyed by a SignalType artifact. The
  // schema's `jsonSchema` rides along so the server can validate any future
  // delivery against the shape this suspend point waited on. Declared with
  // overloads so it matches the interface's two-overload shape.
  function validatedSignalImpl<T>(sig: SignalType<T>): Generator<ActivityYield, T, T>;
  function validatedSignalImpl<T>(
    sig: SignalType<T>,
    options: { readonly timeout: number | Date },
  ): Generator<ActivityYield, TimedSignalOutcome<T>, TimedSignalOutcome<T>>;
  function validatedSignalImpl<T>(
    sig: SignalType<T>,
    options?: { readonly timeout: number | Date },
  ): Generator<ActivityYield, T | TimedSignalOutcome<T>, unknown> {
    return signalImpl<T>(sig.name, {
      ...(options?.timeout !== undefined && { timeout: options.timeout }),
      jsonSchema: sig.schema.jsonSchema,
    });
  }

  // Approval preset over validatedSignal. The wire-format signal name is
  // `approve:<id>`, the convention agentLoop, SignalScanner and the
  // dashboard /signals Approve/Reject shortcut already use.
  function approvalImpl(id: string): Generator<ActivityYield, ApprovalDecision, ApprovalDecision>;
  function approvalImpl(
    id: string,
    options: { readonly timeout: number | Date },
  ): Generator<
    ActivityYield,
    TimedSignalOutcome<ApprovalDecision>,
    TimedSignalOutcome<ApprovalDecision>
  >;
  function approvalImpl(
    id: string,
    options?: { readonly timeout: number | Date },
  ): Generator<ActivityYield, ApprovalDecision | TimedSignalOutcome<ApprovalDecision>, unknown> {
    return options !== undefined
      ? validatedSignalImpl(approvalSignal(id), options)
      : validatedSignalImpl(approvalSignal(id));
  }

  return { signal: signalImpl, validatedSignal: validatedSignalImpl, approval: approvalImpl };
}
