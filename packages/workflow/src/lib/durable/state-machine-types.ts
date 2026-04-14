// ---------------------------------------------------------------------------
// State machine type utilities — core type-safety layer
// ---------------------------------------------------------------------------
import type { SchemaParser } from "@promin/core";
//
// Users declare a state definition type to define their machine:
//
//   type OrderStates = {
//     draft: { context: { items: Item[] }; transitions: { submit: "submitted"; cancel: "cancelled" } };
//     submitted: { context: { items: Item[]; submittedAt: Date }; transitions: { review: "approved" | "rejected" } };
//     ...
//   };
// ---------------------------------------------------------------------------

/** Extract context type for a given state */
export type ContextOf<S, State extends keyof S> = S[State] extends { context: infer C } ? C : never;

/** Extract transitions map for a given state */
export type TransitionsOf<S, State extends keyof S> = S[State] extends { transitions: infer T }
  ? T
  : never;

/** All valid event names across all states */
export type EventsOf<S> = { [K in keyof S]: keyof TransitionsOf<S, K> }[keyof S];

/** Target state(s) for a given event from a given state */
export type TargetOf<
  S,
  State extends keyof S,
  Event extends keyof TransitionsOf<S, State>,
> = TransitionsOf<S, State>[Event];

/** States with no transitions (terminal) */
export type TerminalStates<S> = {
  [K in keyof S]: keyof TransitionsOf<S, K> extends never ? K : never;
}[keyof S];

/** Discriminated union of all possible machine snapshots */
export type MachineSnapshot<S> = {
  [State in keyof S]: { current: State; context: ContextOf<S, State> };
}[keyof S];

/** A transition result — used by conditional transitions */
export interface TransitionTo<S, State extends keyof S> {
  readonly _tag: "TransitionTo";
  readonly target: State;
  readonly context: ContextOf<S, State>;
}

/** Helper to create a TransitionTo value */
export function transitionTo<S, State extends keyof S>(
  target: State,
  context: ContextOf<S, State>,
): TransitionTo<S, State> {
  return { _tag: "TransitionTo", target, context };
}

/** Machine instance state stored in storage */
export interface MachineState {
  readonly id: string;
  readonly name: string;
  readonly type?: string;
  readonly namespace?: string;
  readonly current: string;
  readonly context: unknown;
  readonly version?: string;
  readonly metadata?: Record<string, unknown>;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

/** Transition event record for audit trail */
export interface TransitionEvent {
  readonly id: string;
  readonly event: string;
  readonly from: string;
  readonly to: string;
  readonly context: unknown;
  readonly eventData?: unknown;
  readonly metadata?: unknown;
  readonly createdAt: Date;
}

// ---------------------------------------------------------------------------
// Typed event payloads (Events generic)
// ---------------------------------------------------------------------------
//
// Three usage tiers:
//   1. No types, no validation: `stateMachine<S>(...)` — Events defaults to void.
//   2. Typed events, compile-time only: `stateMachine<S, { approve: { amount: number } }>(...)`.
//   3. Typed + runtime validation: same as 2, plus `.strict({ approve: zSchema })`.
//
// Event values can be plain TS types or SchemaParser instances. EventData<E,K>
// extracts the parsed payload type either way.
// ---------------------------------------------------------------------------

/** Event-name → payload-type map. Values may be plain types or SchemaParser. */
export type EventsMap = Record<string, unknown>;

/** Extract payload type for one event. Unwraps SchemaParser<T> → T. */
export type EventData<E, K extends keyof E> = E extends void
  ? unknown
  : E[K] extends SchemaParser<infer T>
    ? T
    : E[K];

/** Valid event names — `keyof Events` when typed, else any string. */
export type EventName<E> = [E] extends [void] ? string : keyof E & string;

/** Discriminated `send()` parameter shape. Hides `data` for void-payload events. */
export type SendParams<E> = [E] extends [void]
  ? { id: string; event: string; data?: unknown; metadata?: unknown }
  : {
      [K in keyof E & string]: [EventData<E, K>] extends [void]
        ? { id: string; event: K; metadata?: unknown }
        : { id: string; event: K; data: EventData<E, K>; metadata?: unknown };
    }[keyof E & string];

/** Per-event SchemaParser map for `.strict()` overrides. */
export type StrictSchemas<E> = {
  [K in keyof E]?: SchemaParser<EventData<E, K>>;
};
