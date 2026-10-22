// ---------------------------------------------------------------------------
// Builder state — the one immutable value a `WorkflowBuilder` wraps: the
// workflow-level config, the steps added so far, and the current head.
//
// Steps live in a `StepSeq`: a prefix of an append-only array shared by
// every builder that extends the same chain. A builder value can still be
// branched (`const b = …; b.step("x"); b.step("y")`), but the common case,
// a straight chain, appends in amortised O(1) and checks a name in O(1):
//
//   - The seq that ends where the shared array ends (the tip) pushes onto
//     it in place. Seqs with a shorter prefix read only their own
//     `length` entries, so they never see the tip's later appends.
//   - Appending to a seq that is not the tip copies its prefix into a new
//     array first (O(n), once per branch).
//   - A name → index map on the shared array answers "is this name in my
//     prefix?" as `index < length`.
// ---------------------------------------------------------------------------

import type { StepDefinition } from "./step-definition.ts";
import type { WorkflowConfig } from "./workflow-types.ts";

/** The array a family of `StepSeq`s shares. */
interface StepStore {
  readonly defs: StepDefinition[];
  /** Step name → its index in `defs`. */
  readonly index: Map<string, number>;
}

/** An immutable view of the first `length` steps of a shared `StepStore`. */
export interface StepSeq {
  /** @internal */
  readonly store: StepStore;
  readonly length: number;
}

export interface BuilderState<Input> {
  readonly config: WorkflowConfig<Input>;
  readonly steps: StepSeq;
  /** The step the next linear step depends on (`null` before the first step). */
  readonly lastStepName: string | null;
}

/** An empty step list with its own backing store. */
export function emptySteps(): StepSeq {
  return { store: { defs: [], index: new Map() }, length: 0 };
}

/** Whether a step named `name` is among the steps of `seq`. */
export function hasStep(params: { readonly seq: StepSeq; readonly name: string }): boolean {
  const i = params.seq.store.index.get(params.name);
  return i !== undefined && i < params.seq.length;
}

/** The last step of `seq`, if any. */
export function lastStep(seq: StepSeq): StepDefinition | undefined {
  return seq.length === 0 ? undefined : seq.store.defs[seq.length - 1];
}

/**
 * `seq` followed by `defs`. Throws `onDuplicate(name)` (before changing
 * anything) when a name in `defs` is already in `seq` or repeats in `defs`.
 */
export function appendSteps(params: {
  readonly seq: StepSeq;
  readonly defs: readonly StepDefinition[];
  readonly onDuplicate: (name: string) => Error;
}): StepSeq {
  const { seq, defs } = params;
  const batch = new Set<string>();
  for (const def of defs) {
    if (batch.has(def.name) || hasStep({ seq, name: def.name })) {
      throw params.onDuplicate(def.name);
    }
    batch.add(def.name);
  }
  const store = seq.length === seq.store.defs.length ? seq.store : copyPrefix(seq);
  for (const def of defs) {
    store.index.set(def.name, store.defs.length);
    store.defs.push(def);
  }
  return { store, length: store.defs.length };
}

/** `seq` with its last step replaced by `def` (same name). Copies the prefix. */
export function replaceLastStep(params: {
  readonly seq: StepSeq;
  readonly def: StepDefinition;
}): StepSeq {
  const { seq, def } = params;
  const store = copyPrefix({ store: seq.store, length: seq.length - 1 });
  store.index.set(def.name, store.defs.length);
  store.defs.push(def);
  return { store, length: store.defs.length };
}

/** A snapshot array of the steps of `seq` (later appends do not touch it). */
export function stepsToArray(seq: StepSeq): StepDefinition[] {
  return seq.store.defs.slice(0, seq.length);
}

function copyPrefix(seq: StepSeq): StepStore {
  const { store, length } = seq;
  const defs = store.defs.slice(0, length);
  let index: Map<string, number>;
  if (store.defs.length - length <= length) {
    // Usually a short tail past the fork point: clone the map, drop the tail.
    index = new Map(store.index);
    for (let i = length; i < store.defs.length; i++) index.delete(store.defs[i]!.name);
  } else {
    index = new Map();
    for (let i = 0; i < length; i++) index.set(defs[i]!.name, i);
  }
  return { defs, index };
}
