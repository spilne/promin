// ---------------------------------------------------------------------------
// Events — `ctx.waitForEvent` and its sender, `deliverEvent`.
//
// An event is a buffered, per-run message addressed by `(name, key?)`. It is
// built on the signal store, so no backend needs new storage:
//
//   - Delivery writes the event under its own signal name
//     (`event:<name>#<key>@<eventId>`), so events never overwrite each other
//     (signals under one name are last-wins) and a redelivery with the same
//     `eventId` finds it already there: idempotent. The buffer holds events
//     delivered before any wait started; `startFreshRun` drops them with the
//     other signals.
//   - A parked wait sits on a one-shot wake signal
//     (`event-wake:<name>#<key>~<nonce>`). The sender, after buffering, looks
//     for runs parked on that prefix and delivers their wake signal, which
//     the ordinary signal scanner turns into a resume. The wait re-reads the
//     buffer after it parks, so a delivery that landed between its look and
//     its park is not missed.
//   - Which event a wait takes is recorded in the journal, so replay is
//     deterministic: a step's Nth wait on `(name, key)` takes the oldest
//     buffered event of that address that no earlier wait of the same step
//     body took or rejected (delivery order, ties by event id).
// ---------------------------------------------------------------------------

import type { SignalState } from "./workflow-state.ts";
import type { WorkflowStorage } from "./workflow-storage.ts";
import type { SignalType } from "../signals/define-signal.ts";

/** Prefix of the signal names buffered events are stored under. */
export const EVENT_BUFFER_PREFIX = "event:";
/** Prefix of the one-shot signal names parked waits sit on. */
export const EVENT_WAKE_PREFIX = "event-wake:";

/** What a buffered event holds. */
export interface BufferedEvent {
  readonly eventId: string;
  readonly payload: unknown;
}

/** The address of an event: its name and optional match key. */
export interface EventAddress {
  readonly name: string;
  readonly key?: string | undefined;
}

const encode = (s: string): string => encodeURIComponent(s).replace(/~/g, "%7E");

function addressOf(address: EventAddress): string {
  return `${encode(address.name)}#${address.key === undefined ? "" : encode(address.key)}`;
}

/** The signal name event `eventId` of `address` is buffered under. */
export function eventBufferSignalName(params: {
  readonly address: EventAddress;
  readonly eventId: string;
}): string {
  return `${EVENT_BUFFER_PREFIX}${addressOf(params.address)}@${encode(params.eventId)}`;
}

/** The prefix every buffered event of `address` shares. */
export function eventBufferPrefix(address: EventAddress): string {
  return `${EVENT_BUFFER_PREFIX}${addressOf(address)}@`;
}

/** The one-shot signal name a wait on `address` parks on. */
export function eventWakeSignalName(params: {
  readonly address: EventAddress;
  readonly nonce: string;
}): string {
  return `${EVENT_WAKE_PREFIX}${addressOf(params.address)}~${params.nonce}`;
}

/** The prefix every wake signal of waits on `address` shares. */
export function eventWakePrefix(address: EventAddress): string {
  return `${EVENT_WAKE_PREFIX}${addressOf(address)}~`;
}

/**
 * The address a wake signal name belongs to; undefined for any other signal
 * (how `deriveWaitingOn` and the tracer recognise an event wait).
 */
export function eventOfWakeSignal(signalName: string): EventAddress | undefined {
  if (!signalName.startsWith(EVENT_WAKE_PREFIX)) return undefined;
  const rest = signalName.slice(EVENT_WAKE_PREFIX.length);
  const hash = rest.indexOf("#");
  const tilde = rest.lastIndexOf("~");
  if (hash < 0 || tilde < hash) return undefined;
  try {
    const name = decodeURIComponent(rest.slice(0, hash));
    const key = rest.slice(hash + 1, tilde);
    return key === "" ? { name } : { name, key: decodeURIComponent(key) };
  } catch {
    return undefined;
  }
}

/**
 * The buffered events of `address` among `signals`, oldest first (delivery
 * time, then event id).
 */
export function bufferedEventsOf(params: {
  readonly signals: readonly SignalState[];
  readonly address: EventAddress;
}): BufferedEvent[] {
  const prefix = eventBufferPrefix(params.address);
  return params.signals
    .filter((s) => s.signalName.startsWith(prefix))
    .sort(
      (a, b) =>
        a.deliveredAt.getTime() - b.deliveredAt.getTime() ||
        (a.signalName < b.signalName ? -1 : a.signalName > b.signalName ? 1 : 0),
    )
    .map((s) => {
      const value = s.payload as Partial<BufferedEvent> | null;
      return {
        eventId: value?.eventId ?? decodeURIComponent(s.signalName.slice(prefix.length)),
        payload: value?.payload,
      };
    });
}

/** Params of `deliverEvent`. */
export interface DeliverEventParams<T = unknown> {
  readonly storage: WorkflowStorage;
  readonly workflowId: string;
  /** The event's name, or the `SignalType` a typed wait uses. */
  readonly event: string | SignalType<T>;
  /** Match key; a wait with the same key (or none) takes it. */
  readonly key?: string;
  readonly payload: T;
  /**
   * Identity of this event. Delivering the same id to the same address again
   * is a no-op, so a sender may retry safely. Default: a fresh id (every call
   * is a new event).
   */
  readonly eventId?: string;
}

/** What `deliverEvent` did. */
export interface DeliverEventResult {
  readonly eventId: string;
  /** The event was already buffered: nothing new was stored. */
  readonly duplicate: boolean;
  /** Parked waits whose wake signal this call delivered. */
  readonly woken: number;
}

/**
 * Deliver an event to a run: buffer it, then wake the waits parked on its
 * address. A wait that starts later finds it in the buffer. The signal
 * scanner (or the caller re-running the workflow) does the actual resume.
 *
 * A retry of a delivery whose first attempt stopped half way (buffered, not
 * woken) completes the wake without buffering twice.
 */
export async function deliverEvent<T>(params: DeliverEventParams<T>): Promise<DeliverEventResult> {
  const { storage, workflowId } = params;
  const address: EventAddress = {
    name: typeof params.event === "string" ? params.event : params.event.name,
    ...(params.key !== undefined && { key: params.key }),
  };
  const eventId = params.eventId ?? crypto.randomUUID();
  const signalName = eventBufferSignalName({ address, eventId });
  const existing = (await storage.loadSignals(workflowId)).some((s) => s.signalName === signalName);
  if (!existing) {
    const stored: BufferedEvent = { eventId, payload: params.payload };
    await storage.deliverSignal({ workflowId, signalName, payload: stored });
  }
  const wakePrefix = eventWakePrefix(address);
  const state = await storage.loadWorkflow(workflowId);
  let woken = 0;
  for (const step of Object.values(state?.steps ?? {})) {
    if (step.status !== "waiting_for_signal" || step.signalName === undefined) continue;
    if (!step.signalName.startsWith(wakePrefix)) continue;
    await storage.deliverSignal({ workflowId, signalName: step.signalName, payload: null });
    woken++;
  }
  return { eventId, duplicate: existing, woken };
}
