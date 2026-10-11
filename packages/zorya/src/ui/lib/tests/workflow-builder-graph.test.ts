import { describe, expect, it } from "bun:test";
import type { ActivityDescriptor, WorkflowSchema } from "@promin/workflow";
import {
  INPUT_NODE_ID,
  INPUT_W,
  NODE_W,
  addStep,
  autoLayout,
  connect,
  cycleSteps,
  disconnect,
  graphFromSchema,
  moveStep,
  newStep,
  positionsFor,
  removeStep,
  renameStep,
  starterSchema,
  uniqueStepName,
  wouldCreateCycle,
} from "../workflow-builder-graph.ts";

const ACTIVITIES: ActivityDescriptor[] = [
  { id: "is.long", title: "Is long", outputSchema: { type: "boolean" } },
  { id: "text.upper", title: "Upper", defaultConfig: { locale: "en" } },
  { id: "text.split", title: "Split", outputSchema: { type: "array" } },
];

const chain: WorkflowSchema = {
  version: 1,
  name: "w",
  steps: [
    { type: "step", name: "a", dependsOn: [], activityRef: "text.upper" },
    { type: "step", name: "b", dependsOn: ["a"], activityRef: "text.upper" },
    { type: "map", name: "each", arrayFrom: "b", activityRef: "text.upper" },
  ],
};

describe("graphFromSchema", () => {
  it("draws dependency, array and input edges", () => {
    const graph = graphFromSchema(chain);
    expect(graph.edges).toEqual([
      { from: INPUT_NODE_ID, to: "a", kind: "input" },
      { from: "a", to: "b", kind: "dependency" },
      { from: "b", to: "each", kind: "array" },
    ]);
    expect(graph.nodes.map((n) => n.id)).toEqual(["a", "b", "each"]);
  });

  it("places unpositioned nodes by rank after the Input column, and the Input node left of the roots", () => {
    const graph = graphFromSchema(chain);
    const [a, b, each] = graph.nodes;
    expect(a!.x).toBeLessThan(b!.x);
    expect(b!.x).toBeLessThan(each!.x);
    expect(graph.input.x + INPUT_W).toBeLessThan(a!.x);
  });

  it("keeps stored positions and skips edges to missing steps", () => {
    const schema: WorkflowSchema = {
      ...chain,
      steps: [...chain.steps, { type: "step", name: "c", dependsOn: ["ghost"], activityRef: "x" }],
      ui: { a: { x: 500, y: 300 } },
    };
    expect(positionsFor(schema).a).toEqual({ x: 500, y: 300 });
    expect(graphFromSchema(schema).edges.some((e) => e.from === "ghost")).toBe(false);
  });
});

describe("edits", () => {
  it("adds steps with unique names, activity defaults and a free position", () => {
    const first = addStep({
      schema: chain,
      kind: "step",
      activity: ACTIVITIES[1],
      activities: ACTIVITIES,
    });
    expect(first.name).toBe("upper");
    expect(first.schema.steps.at(-1)).toEqual({
      type: "step",
      name: "upper",
      dependsOn: [],
      activityRef: "text.upper",
      config: { locale: "en" },
    });
    const second = addStep({
      schema: first.schema,
      kind: "step",
      activity: ACTIVITIES[1],
      activities: ACTIVITIES,
      position: { x: 10.4, y: 20.6 },
    });
    expect(second.name).toBe("upper2");
    expect(second.schema.ui?.upper2).toEqual({ x: 10, y: 21 });
    // Every node now has a stored position and the new one is right of the rest.
    const xs = chain.steps.map((s) => first.schema.ui![s.name]!.x);
    expect(first.schema.ui!.upper!.x).toBeGreaterThan(Math.max(...xs) + NODE_W - 1);
  });

  it("builds every node kind with sensible defaults", () => {
    const kinds = ["step", "map", "sleep", "signal", "approval", "branch", "parallel"] as const;
    const steps = kinds.map((kind) => newStep({ kind, name: kind, activities: ACTIVITIES }));
    expect(steps.map((s) => s.type)).toEqual([...kinds]);
    expect(steps.find((s) => s.type === "branch")).toMatchObject({
      conditionRef: "is.long",
      ifTrue: { activityRef: "text.upper" },
    });
    expect(steps.find((s) => s.type === "signal")).toMatchObject({ signalName: "signal-signal" });
    expect(steps.find((s) => s.type === "map")).toMatchObject({ arrayFrom: "" });
  });

  it("connects, refuses cycles and self-loops, and makes roots from the Input node", () => {
    const linked = connect({ schema: chain, from: "a", to: "b" });
    expect(linked).toEqual({ ok: true, schema: chain });

    const cyclic = connect({ schema: chain, from: "b", to: "a" });
    expect(cyclic).toMatchObject({ ok: false });
    expect(wouldCreateCycle({ schema: chain, from: "b", to: "a" })).toBe(true);
    expect(wouldCreateCycle({ schema: chain, from: "a", to: "a" })).toBe(true);
    expect(wouldCreateCycle({ schema: chain, from: INPUT_NODE_ID, to: "b" })).toBe(false);

    const root = connect({ schema: chain, from: INPUT_NODE_ID, to: "b" });
    expect(root.ok && root.schema.steps[1]).toMatchObject({ name: "b", dependsOn: [] });
    expect(connect({ schema: chain, from: INPUT_NODE_ID, to: "each" }).ok).toBe(false);

    const remapped = connect({ schema: chain, from: "a", to: "each" });
    expect(remapped.ok && remapped.schema.steps[2]).toMatchObject({ arrayFrom: "a" });
  });

  it("disconnects dependencies but not a map's only source", () => {
    const cut = disconnect({ schema: chain, from: "a", to: "b" });
    expect(cut.ok && cut.schema.steps[1]).toMatchObject({ dependsOn: [] });
    expect(disconnect({ schema: chain, from: "b", to: "each" }).ok).toBe(false);
  });

  it("renames a step everywhere it is referenced", () => {
    const withUi = { ...chain, ui: { b: { x: 1, y: 2 } } };
    const result = renameStep({ schema: withUi, from: "b", to: "middle" });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.schema.steps.map((s) => s.name)).toEqual(["a", "middle", "each"]);
    expect(result.schema.steps[2]).toMatchObject({ arrayFrom: "middle" });
    expect(result.schema.ui).toEqual({ middle: { x: 1, y: 2 } });
    expect(renameStep({ schema: chain, from: "a", to: "b" })).toMatchObject({ ok: false });
    expect(renameStep({ schema: chain, from: "a", to: " " })).toMatchObject({ ok: false });
  });

  it("removes a step and the dependencies on it", () => {
    const next = removeStep({ schema: { ...chain, ui: { a: { x: 0, y: 0 } } }, name: "a" });
    expect(next.steps.map((s) => s.name)).toEqual(["b", "each"]);
    expect(next.steps[0]).toMatchObject({ dependsOn: [] });
    expect(next.ui).toEqual({});
  });

  it("moves nodes to rounded positions and re-lays out on demand", () => {
    const moved = moveStep({ schema: chain, name: "a", x: 12.7, y: -3.2 });
    expect(moved.ui?.a).toEqual({ x: 13, y: -3 });
    const laid = autoLayout({ ...moved, ui: { a: { x: 999, y: 999, label: "A" } } });
    expect(laid.ui?.a?.label).toBe("A");
    expect(laid.ui?.a?.x).not.toBe(999);
  });
});

describe("helpers", () => {
  it("finds cycles including the steps downstream of them", () => {
    const schema: WorkflowSchema = {
      version: 1,
      name: "c",
      steps: [
        { type: "step", name: "a", dependsOn: ["b"], activityRef: "x" },
        { type: "step", name: "b", dependsOn: ["a"], activityRef: "x" },
        { type: "step", name: "c", dependsOn: ["b"], activityRef: "x" },
        { type: "step", name: "d", dependsOn: [], activityRef: "x" },
      ],
    };
    expect(cycleSteps(schema)).toEqual(["a", "b", "c"]);
    expect(cycleSteps(chain)).toEqual([]);
  });

  it("names steps uniquely", () => {
    expect(uniqueStepName({ schema: chain, base: "a" })).toBe("a2");
    expect(uniqueStepName({ schema: chain, base: "fresh" })).toBe("fresh");
  });

  it("starts a new workflow on the first non-predicate activity", () => {
    const schema = starterSchema({ name: "new-flow", activities: ACTIVITIES });
    expect(schema.steps).toEqual([
      {
        type: "step",
        name: "upper",
        dependsOn: [],
        activityRef: "text.upper",
        config: { locale: "en" },
      },
    ]);
    expect(schema.ui?.upper).toBeDefined();
  });
});
