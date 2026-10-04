import { describe, expect, it } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { LosslessJsonCodec } from "@spilne/perfect-core/connect";
import {
  appendSteps,
  emptySteps,
  hasStep,
  lastStep,
  stepsToArray,
  type BuilderState,
  type StepSeq,
} from "../builder-state.ts";
import type { StepDefinition } from "../step-definition.ts";
import { workflow, type WorkflowBuilder } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

const def = (name: string): StepDefinition => ({
  name,
  dependsOn: [],
  kind: "normal",
  codec: LosslessJsonCodec as never,
  execute: () => succeed(name),
});

const duplicate = (name: string) => new Error(`dup ${name}`);
const append = (seq: StepSeq, ...names: string[]): StepSeq =>
  appendSteps({ seq, defs: names.map(def), onDuplicate: duplicate });
const names = (seq: StepSeq): string[] => stepsToArray(seq).map((d) => d.name);

/** The state a builder wraps (a private field; read here to guard its cost). */
const stateOf = (b: WorkflowBuilder<any, any, any, any>): BuilderState<unknown> =>
  (b as unknown as { s: BuilderState<unknown> }).s;

describe("StepSeq", () => {
  it("a straight chain of appends shares one backing array", () => {
    const first = append(emptySteps(), "s0");
    let seq = first;
    for (let i = 1; i < 10_000; i++) seq = append(seq, `s${i}`);
    expect(seq.length).toBe(10_000);
    // No copy on any append: the first and last seq share the store, and
    // the store holds each step exactly once.
    expect(seq.store).toBe(first.store);
    expect(seq.store.defs.length).toBe(10_000);
    expect(seq.store.index.size).toBe(10_000);
    expect(hasStep({ seq, name: "s9999" })).toBe(true);
    expect(hasStep({ seq: first, name: "s1" })).toBe(false);
  });

  it("an earlier seq never sees later appends", () => {
    const a = append(emptySteps(), "a");
    const ab = append(a, "b");
    expect(names(a)).toEqual(["a"]);
    expect(names(ab)).toEqual(["a", "b"]);
    expect(lastStep(a)?.name).toBe("a");
    expect(hasStep({ seq: a, name: "b" })).toBe(false);
  });

  it("branching copies the prefix once, and branches stay independent", () => {
    const base = append(emptySteps(), "a", "b");
    const left = append(base, "x");
    // `base` is no longer the tip: this append copies its prefix.
    const right = append(base, "x", "y");
    expect(right.store).not.toBe(left.store);
    expect(names(left)).toEqual(["a", "b", "x"]);
    expect(names(right)).toEqual(["a", "b", "x", "y"]);
    // Further appends on either branch stay in place.
    const right2 = append(right, "z");
    expect(right2.store).toBe(right.store);
    expect(names(append(left, "y"))).toEqual(["a", "b", "x", "y"]);
    expect(names(base)).toEqual(["a", "b"]);
  });

  it("rejects a duplicate name without changing anything", () => {
    const seq = append(emptySteps(), "a", "b");
    expect(() => append(seq, "c", "a")).toThrow("dup a");
    expect(() => append(seq, "c", "c")).toThrow("dup c");
    expect(seq.store.defs.length).toBe(2);
    // The failed append left `seq` the tip, so this appends in place.
    expect(append(seq, "c").store).toBe(seq.store);
  });

  it("a snapshot array is not affected by later appends", () => {
    const seq = append(emptySteps(), "a");
    const snapshot = stepsToArray(seq);
    append(seq, "b");
    expect(snapshot.map((d) => d.name)).toEqual(["a"]);
  });
});

describe("WorkflowBuilder state", () => {
  it("a 5000-step chain appends in place: one backing array for every builder", () => {
    let b: WorkflowBuilder<number, any, any, any> = workflow<number>({ name: "long" });
    const builders: WorkflowBuilder<number, any, any, any>[] = [];
    for (let i = 0; i < 5_000; i++) {
      b = b.step(`s${i}`, ({ prev }: { prev: number }) => succeed(prev + 1));
      builders.push(b);
    }
    const store = stateOf(b).steps.store;
    expect(builders.every((x) => stateOf(x).steps.store === store)).toBe(true);
    expect(store.defs.length).toBe(5_000);
    expect(b.build()._definition.steps.length).toBe(5_000);
  });

  it("a builder value can be branched: each branch sees only its own steps", async () => {
    const base = workflow<number>({ name: "fork" }).step("a", ({ input }) => succeed(input + 1));
    const left = base.step("tail", ({ prev }) => succeed(`left:${prev}`));
    // Same step name on a sibling branch is allowed.
    const right = base
      .step("tail", ({ prev }) => succeed(`right:${prev}`))
      .step("extra", ({ prev }) => succeed(`${prev}!`));

    expect(base.build().dag.steps.map((s) => s.name)).toEqual(["a"]);
    expect(left.build().dag.steps.map((s) => s.name)).toEqual(["a", "tail"]);
    expect(right.build().dag.steps.map((s) => s.name)).toEqual(["a", "tail", "extra"]);
    expect(() => left.step("a", () => succeed(0))).toThrow(/Duplicate step name: "a"/);
    expect(() => left.step("extra", () => succeed(0))).not.toThrow();

    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    expect(await runner.run({ workflow: left.build(), workflowId: "l", input: 1 })).toBe("left:2");
    expect(await runner.run({ workflow: right.build(), workflowId: "r", input: 1 })).toBe(
      "right:2!",
    );
  });

  it("a built workflow is a snapshot: later appends do not change it", () => {
    const b = workflow<number>({ name: "snap" }).step("a", () => succeed(1));
    const built = b.build();
    b.step("b", () => succeed(2)).step("c", () => succeed(3));
    expect(built._definition.steps.map((s) => s.name)).toEqual(["a"]);
    expect(built.dag.steps.map((s) => s.name)).toEqual(["a"]);
  });

  it("parallelSteps branch names collide with existing steps", () => {
    const b = workflow<number>({ name: "p" }).step("fan.a", () => succeed(1));
    expect(() => b.parallelSteps("fan", { a: () => succeed(2) })).toThrow(
      /Duplicate step name: "fan.a"/,
    );
  });

  it("version() and build({ idempotency }) keep steps and config", () => {
    const b = workflow<number>({ name: "cfg", timeoutMs: 5 }).step("a", () => succeed(1));
    const wf = b.version("7").build({ idempotency: { ttl: 10 } });
    expect(wf.version).toBe("7");
    expect(wf.idempotency).toEqual({ ttl: 10 });
    expect(wf._definition.timeoutMs).toBe(5);
    expect(wf._definition.onVersionMismatch).toBe("strict");
    expect(b.build().version).toBeUndefined();
    expect(b.build().idempotency).toBeUndefined();
  });
});
