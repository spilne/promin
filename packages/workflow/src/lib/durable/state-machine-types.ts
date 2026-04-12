// ---------------------------------------------------------------------------
// State machine type utilities — core type-safety layer
// ---------------------------------------------------------------------------
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
  readonly metadata?: unknown;
  readonly createdAt: Date;
}
