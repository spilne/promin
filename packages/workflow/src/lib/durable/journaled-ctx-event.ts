// ---------------------------------------------------------------------------
// ctx.waitForEvent — a durable wait for a buffered, addressed event. See
// `wait-for-event.ts` for the delivery side and the buffering rules.
//
// Each round of a wait journals two slots: a `select` (an activity entry
// recording which buffered event the wait took, if any, and the wait's
// deadline and wake nonce) and, when nothing was buffered, a signal entry the
// wait parks on. Replay re-walks the recorded rounds, so the outcome never
// depends on what the buffer holds later.
// ---------------------------------------------------------------------------

import type { JournalExit } from "./activity-journal.ts";
import { WorkflowSuspendedError } from "./durable-pipeline-error.ts";
import {
  decodeSignalExitValue,
  rehydrateFailure,
  timedOutSignalExitValue,
} from "./journal-exit.ts";
import type {
  ActivityYield,
  JournaledContext,
  JournaledCtxEnv,
  TimedSignalOutcome,
} from "./journaled-context.ts";
import type { SignalType } from "../signals/define-signal.ts";
import {
  bufferedEventsOf,
  eventWakeSignalName,
  type EventAddress,
  type BufferedEvent,
} from "./wait-for-event.ts";

/** What a round's `select` entry records. */
interface Selection {
  readonly event?: BufferedEvent;
  /** ISO time the wait times out at; set in the first round, carried by later ones. */
  readonly deadline?: string;
  /** Names this round's wake signal. */
  readonly nonce: string;
}

type WaitOptions<T> = {
  readonly key?: string;
  readonly timeout?: number | Date;
  /** Take only events whose payload satisfies this (rejected events stay buffered for other waits). */
  readonly where?: (payload: T) => boolean;
};

/** Build `ctx.waitForEvent` for one body run. */
export function makeWaitForEvent(
  env: JournaledCtxEnv,
): JournaledContext<unknown, unknown>["waitForEvent"] {
  const { workflowId, stepName, cursor, workflowStorage, guard, clock } = env;
  /** Event ids this body run's waits took. */
  const seen = new Set<string>();

  function* waitForEvent<T>(
    event: string | SignalType<T>,
    options?: WaitOptions<T>,
  ): Generator<ActivityYield, T | TimedSignalOutcome<T>, unknown> {
    const name = typeof event === "string" ? event : event.name;
    const address: EventAddress = {
      name,
      ...(options?.key !== undefined && { key: options.key }),
    };
    const hasTimeout = options?.timeout !== undefined;
    /** Events this wait's `where` turned down: skipped by it alone. */
    const rejected = new Set<string>();
    const first = cursor.allocateSlot();

    const promise = (async (): Promise<T | TimedSignalOutcome<T>> => {
      if (!workflowStorage) throw new TypeError("ctx.waitForEvent requires a WorkflowStorage");
      const storage = workflowStorage;
      const outcome = (value: T): T | TimedSignalOutcome<T> =>
        hasTimeout ? { ok: true, value } : value;
      const selectName = `waitForEvent:${name}`;
      let deadline: string | undefined;

      for (let round = 0; ; round++) {
        // -- select: which buffered event does this round take? -----------
        const slot = round === 0 ? first : cursor.allocateSlot();
        const recorded = cursor.expectRecorded({ slot, kind: "activity", name: selectName });
        let selection: Selection;
        if (recorded && recorded.phase === "completed" && recorded.exit) {
          selection = selectionOf(recorded.exit);
        } else {
          const buffered = bufferedEventsOf({
            signals: await storage.loadSignals(workflowId),
            address,
          });
          const next = buffered.find((e) => !seen.has(e.eventId) && !rejected.has(e.eventId));
          const fresh: Selection = {
            ...(next !== undefined && { event: next }),
            ...(deadline === undefined && options?.timeout !== undefined
              ? {
                  deadline: (options.timeout instanceof Date
                    ? options.timeout
                    : new Date(clock.currentTimeMs() + options.timeout)
                  ).toISOString(),
                }
              : deadline !== undefined && { deadline }),
            nonce: crypto.randomUUID(),
          };
          if (!recorded) await cursor.writePending({ slot, kind: "activity", name: selectName });
          const stored = await cursor.complete({
            slot,
            exit: { tag: "Success", value: fresh },
          });
          selection = stored.won ? fresh : selectionOf(stored.exit);
        }
        deadline = selection.deadline ?? deadline;

        if (selection.event !== undefined) {
          const taken = selection.event;
          const payload = taken.payload as T;
          if (!options?.where || options.where(payload)) {
            seen.add(taken.eventId);
            return outcome(payload);
          }
          rejected.add(taken.eventId); // stays buffered for other waits; look again
          continue;
        }

        // -- nothing buffered: park on the wake signal -----------------------
        const wakeName = eventWakeSignalName({ address, nonce: selection.nonce });
        const wakeAt = deadline === undefined ? undefined : new Date(deadline);
        const waitSlot = cursor.allocateSlot();
        const waiting = cursor.expectRecorded({ slot: waitSlot, kind: "signal", name: wakeName });
        if (waiting && waiting.phase === "completed" && waiting.exit) {
          if (decodeWake(waiting.exit) === "timeout") return { ok: false, error: "timeout" };
          continue; // woken: look at the buffer again
        }
        if (!waiting) {
          await cursor.writePending({
            slot: waitSlot,
            kind: "signal",
            name: wakeName,
            ...(wakeAt && { wakeAt }),
          });
        }
        if (wakeAt && clock.currentTimeMs() >= wakeAt.getTime()) {
          const stored = await cursor.complete({
            slot: waitSlot,
            exit: { tag: "Success", value: timedOutSignalExitValue() },
          });
          if (decodeWake(stored.exit) === "timeout") return { ok: false, error: "timeout" };
          continue;
        }

        await storage.suspendWorkflow({
          workflowId,
          stepName,
          stepUpdate: {
            status: "waiting_for_signal",
            signalName: wakeName,
            ...(wakeAt && { signalTimeoutAt: wakeAt }),
            ...(typeof event !== "string" && { signalJsonSchema: event.schema.jsonSchema }),
          },
          guard,
        });
        // A delivery between the select above and the park saw no parked
        // wait to wake: wake ourselves if the buffer now holds something.
        const late = bufferedEventsOf({
          signals: await storage.loadSignals(workflowId),
          address,
        }).some((e) => !seen.has(e.eventId) && !rejected.has(e.eventId));
        if (late) {
          await storage.deliverSignal({ workflowId, signalName: wakeName, payload: null });
        }
        throw new WorkflowSuspendedError({
          workflowId,
          stepName,
          reason: "event",
          eventName: name,
          ...(options?.key !== undefined && { eventKey: options.key }),
          message: `waiting for event "${name}"${
            options?.key !== undefined ? ` (key "${options.key}")` : ""
          }${wakeAt ? ` (timeout at ${wakeAt.toISOString()})` : ""}`,
        });
      }
    })();

    return (yield { _tag: "Activity", name: `waitForEvent:${name}`, promise }) as
      | T
      | TimedSignalOutcome<T>;
  }

  return waitForEvent as JournaledContext<unknown, unknown>["waitForEvent"];
}

function selectionOf(exit: JournalExit): Selection {
  if (exit.tag === "Failure") throw rehydrateFailure(exit);
  return exit.value as Selection;
}

function decodeWake(exit: JournalExit): "woken" | "timeout" {
  if (exit.tag === "Failure") throw rehydrateFailure(exit);
  return decodeSignalExitValue(exit.value).kind === "timeout" ? "timeout" : "woken";
}
