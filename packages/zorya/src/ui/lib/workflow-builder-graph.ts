// ---------------------------------------------------------------------------
// workflow-builder-graph — the builder canvas's model of a `WorkflowSchema`.
//
// The schema is the single source of truth: every edit (add, connect,
// rename, move, remove) is a pure function from one schema to the next, and
// the canvas renders `graphFromSchema(schema)`. Node positions live in the
// schema's `ui` map; nodes without one are placed by the shared DAG layout.
//
// Edges are dependencies: `dependsOn` for most kinds, `arrayFrom` for map
// steps. Steps with no dependencies read the workflow input, drawn as edges
// from a synthetic Input node. No DOM, no framework.
// ---------------------------------------------------------------------------

import type { ActivityDescriptor, NodeUiMeta, StepSchema, WorkflowSchema } from "@promin/workflow";
import { isRootStep } from "../../shared/workflow-input.ts";
import { layoutGraph } from "./graph-layout.ts";

export { isRootStep };

export type StepKind = StepSchema["type"];

/** Id of the synthetic Input node (never a valid step name: it has spaces). */
export const INPUT_NODE_ID = "workflow input";

export const NODE_W = 184;
export const NODE_H = 76;
export const INPUT_W = 168;
export const INPUT_H = 64;
const COL_GAP = 96;
const ROW_GAP = 28;

/** Palette entries for the node kinds built into the schema. */
export const CONTROL_KINDS: ReadonlyArray<{
  readonly kind: Exclude<StepKind, "step">;
  readonly title: string;
  readonly description: string;
}> = [
  {
    kind: "branch",
    title: "Branch",
    description: "Run one of two activities, chosen by a boolean condition activity.",
  },
  {
    kind: "parallel",
    title: "Parallel",
    description: "Run several activities on the same input at once; joins their results.",
  },
  {
    kind: "map",
    title: "Map",
    description: "Run an activity once per element of another step's array output.",
  },
  { kind: "sleep", title: "Sleep", description: "Wait a fixed time, then pass the value on." },
  {
    kind: "signal",
    title: "Wait for signal",
    description: "Suspend until a named signal arrives; continues with its payload.",
  },
  {
    kind: "approval",
    title: "Approval",
    description: "Suspend until someone approves (signal `<name>-approved`).",
  },
];

export interface GraphNode {
  readonly id: string;
  readonly step: StepSchema;
  readonly x: number;
  readonly y: number;
}

export interface GraphEdge {
  readonly from: string;
  readonly to: string;
  /** `dependency` (dependsOn), `array` (a map's arrayFrom) or `input` (from the Input node). */
  readonly kind: "dependency" | "array" | "input";
}

export interface WorkflowGraph {
  readonly nodes: ReadonlyArray<GraphNode>;
  readonly input: { readonly x: number; readonly y: number };
  readonly edges: ReadonlyArray<GraphEdge>;
}

/** The steps a step waits for. */
export function stepDependencies(step: StepSchema): readonly string[] {
  return step.type === "map" ? (step.arrayFrom ? [step.arrayFrom] : []) : step.dependsOn;
}

/** Every activity a step references, with where (for messages and pickers). */
export function stepActivityRefs(
  step: StepSchema,
): Array<{ readonly role: string; readonly ref: string }> {
  switch (step.type) {
    case "step":
    case "map":
      return [{ role: "activity", ref: step.activityRef }];
    case "branch":
      return [
        { role: "condition", ref: step.conditionRef },
        { role: "if true", ref: step.ifTrue.activityRef },
        { role: "if false", ref: step.ifFalse.activityRef },
      ];
    case "parallel":
      return Object.entries(step.branches).map(([branch, arm]) => ({
        role: `branch ${branch}`,
        ref: arm.activityRef,
      }));
    default:
      return [];
  }
}

/**
 * Nodes at their stored positions (laid out when missing), the Input node
 * left of the roots, and every edge.
 */
export function graphFromSchema(schema: WorkflowSchema): WorkflowGraph {
  const positions = positionsFor(schema);
  const nodes = schema.steps.map((step) => ({ id: step.name, step, ...positions[step.name]! }));
  const names = new Set(schema.steps.map((s) => s.name));
  const edges: GraphEdge[] = [];
  for (const step of schema.steps) {
    if (isRootStep(step)) edges.push({ from: INPUT_NODE_ID, to: step.name, kind: "input" });
    for (const dep of stepDependencies(step)) {
      if (!names.has(dep)) continue;
      edges.push({ from: dep, to: step.name, kind: step.type === "map" ? "array" : "dependency" });
    }
  }
  const roots = nodes.filter((n) => isRootStep(n.step));
  const anchor = roots.length > 0 ? roots : nodes;
  const minX = anchor.length > 0 ? Math.min(...anchor.map((n) => n.x)) : INPUT_W + COL_GAP;
  const ys = anchor.map((n) => n.y);
  const y = ys.length > 0 ? (Math.min(...ys) + Math.max(...ys)) / 2 + (NODE_H - INPUT_H) / 2 : 40;
  return { nodes, input: { x: minX - INPUT_W - COL_GAP, y: Math.round(y) }, edges };
}

/**
 * Positions of every step: its `ui` entry when it has one, else where the
 * DAG layout puts it (after the Input column).
 */
export function positionsFor(schema: WorkflowSchema): Record<string, { x: number; y: number }> {
  const out: Record<string, { x: number; y: number }> = {};
  const missing = schema.steps.filter((s) => !schema.ui?.[s.name]);
  if (missing.length > 0) {
    const laid = layoutGraph(
      schema.steps.map((s) => ({ id: s.name, dependsOn: stepDependencies(s) })),
      { orientation: "horizontal", nodeW: NODE_W, nodeH: NODE_H, colGap: COL_GAP, rowGap: ROW_GAP },
    );
    for (const placed of laid.nodes) {
      out[placed.node.id] = { x: placed.x + INPUT_W + COL_GAP, y: placed.y };
    }
  }
  for (const step of schema.steps) {
    const ui = schema.ui?.[step.name];
    if (ui) out[step.name] = { x: ui.x, y: ui.y };
  }
  return out;
}

/** Store the layout's position for every step that has none. */
export function withLayout(schema: WorkflowSchema): WorkflowSchema {
  if (schema.steps.every((s) => schema.ui?.[s.name])) return schema;
  const positions = positionsFor(schema);
  const ui: Record<string, NodeUiMeta> = { ...schema.ui };
  for (const step of schema.steps) ui[step.name] ??= positions[step.name]!;
  return { ...schema, ui };
}

/** Re-run the layout for every step, dropping stored positions (labels kept). */
export function autoLayout(schema: WorkflowSchema): WorkflowSchema {
  const ui: Record<string, NodeUiMeta> = {};
  const stripped = { ...schema, ui: {} };
  const positions = positionsFor(stripped);
  for (const step of schema.steps) {
    ui[step.name] = { ...schema.ui?.[step.name], ...positions[step.name]! };
  }
  return { ...schema, ui };
}

// ---------------------------------------------------------------------------
// Edits
// ---------------------------------------------------------------------------

/** What an edit produced: the next schema, or why it was refused. */
export type EditResult =
  | { readonly ok: true; readonly schema: WorkflowSchema }
  | { readonly ok: false; readonly error: string };

/** A fresh step of `kind`, wired to nothing, using the catalog's activities. */
export function newStep(params: {
  readonly kind: StepKind;
  readonly name: string;
  readonly activity?: ActivityDescriptor;
  readonly activities: ReadonlyArray<ActivityDescriptor>;
}): StepSchema {
  const { kind, name, activities } = params;
  const plain = activities.filter((a) => !isPredicate(a));
  const firstPlain = plain[0]?.id ?? activities[0]?.id ?? "";
  const activity = params.activity;
  const activityRef = activity?.id ?? firstPlain;
  const config = activity?.defaultConfig ? { config: { ...activity.defaultConfig } } : {};
  switch (kind) {
    case "step":
      return { type: "step", name, dependsOn: [], activityRef, ...config };
    case "map":
      return { type: "map", name, arrayFrom: "", activityRef, ...config };
    case "sleep":
      return { type: "sleep", name, dependsOn: [], ms: 1_000 };
    case "signal":
      return { type: "signal", name, dependsOn: [], signalName: `${name}-signal` };
    case "approval":
      return { type: "approval", name, dependsOn: [] };
    case "branch":
      return {
        type: "branch",
        name,
        dependsOn: [],
        conditionRef: activities.find(isPredicate)?.id ?? "",
        ifTrue: { activityRef: firstPlain },
        ifFalse: { activityRef: firstPlain },
      };
    case "parallel":
      return {
        type: "parallel",
        name,
        dependsOn: [],
        branches: { a: { activityRef: firstPlain }, b: { activityRef: firstPlain } },
      };
  }
}

/** True for activities whose output is a boolean: branch conditions. */
export function isPredicate(activity: ActivityDescriptor): boolean {
  return activity.outputSchema?.type === "boolean";
}

/** Add a step, named after its activity or kind, at `position` (laid out when absent). */
export function addStep(params: {
  readonly schema: WorkflowSchema;
  readonly kind: StepKind;
  readonly activity?: ActivityDescriptor;
  readonly activities: ReadonlyArray<ActivityDescriptor>;
  readonly position?: { x: number; y: number };
}): { schema: WorkflowSchema; name: string } {
  const { schema } = params;
  const base = params.activity ? baseStepName(params.activity.id) : params.kind;
  const name = uniqueStepName({ schema, base });
  const step = newStep({
    kind: params.kind,
    name,
    activities: params.activities,
    ...(params.activity && { activity: params.activity }),
  });
  const placed = withLayout(schema);
  const position = params.position ?? nextFreePosition(placed);
  return {
    schema: {
      ...placed,
      steps: [...schema.steps, step],
      ui: { ...placed.ui, [name]: { x: Math.round(position.x), y: Math.round(position.y) } },
    },
    name,
  };
}

/** Right of the rightmost node, level with it; the first column when empty. */
function nextFreePosition(schema: WorkflowSchema): { x: number; y: number } {
  const positions = Object.values(schema.ui ?? {});
  if (positions.length === 0) return { x: INPUT_W + COL_GAP + 24, y: 24 };
  const rightmost = positions.reduce((a, b) => (b.x > a.x ? b : a));
  return { x: rightmost.x + NODE_W + COL_GAP, y: rightmost.y };
}

/** Remove a step and every dependency on it. A map reading it keeps a dangling `arrayFrom`. */
export function removeStep(params: { schema: WorkflowSchema; name: string }): WorkflowSchema {
  const { schema, name } = params;
  const steps = schema.steps
    .filter((s) => s.name !== name)
    .map((s) =>
      s.type !== "map" && s.dependsOn.includes(name)
        ? { ...s, dependsOn: s.dependsOn.filter((d) => d !== name) }
        : s,
    );
  const { [name]: _removed, ...ui } = schema.ui ?? {};
  return { ...schema, steps, ui };
}

/** Rename a step, following every reference to it. */
export function renameStep(params: {
  schema: WorkflowSchema;
  from: string;
  to: string;
}): EditResult {
  const { schema, from } = params;
  const to = params.to.trim();
  if (to === from) return { ok: true, schema };
  if (!to) return { ok: false, error: "A step needs a name." };
  if (to === INPUT_NODE_ID) return { ok: false, error: `"${to}" is reserved.` };
  if (schema.steps.some((s) => s.name === to)) {
    return { ok: false, error: `Another step is already named "${to}".` };
  }
  const steps = schema.steps.map((s): StepSchema => {
    const renamed = s.name === from ? { ...s, name: to } : s;
    if (renamed.type === "map") {
      return renamed.arrayFrom === from ? { ...renamed, arrayFrom: to } : renamed;
    }
    return renamed.dependsOn.includes(from)
      ? { ...renamed, dependsOn: renamed.dependsOn.map((d) => (d === from ? to : d)) }
      : renamed;
  });
  const ui = { ...schema.ui };
  if (ui[from]) {
    ui[to] = ui[from]!;
    delete ui[from];
  }
  return { ok: true, schema: { ...schema, steps, ui } };
}

/** Replace a step with `next` (same name; use `renameStep` to rename). */
export function replaceStep(params: { schema: WorkflowSchema; step: StepSchema }): WorkflowSchema {
  return {
    ...params.schema,
    steps: params.schema.steps.map((s) => (s.name === params.step.name ? params.step : s)),
  };
}

/** Move a step's node. */
export function moveStep(params: {
  schema: WorkflowSchema;
  name: string;
  x: number;
  y: number;
}): WorkflowSchema {
  const { schema, name } = params;
  return {
    ...schema,
    ui: {
      ...schema.ui,
      [name]: { ...schema.ui?.[name], x: Math.round(params.x), y: Math.round(params.y) },
    },
  };
}

/**
 * Connect `from` → `to`: `to` then waits for `from` (a map iterates over
 * it instead). From the Input node it makes `to` a root step. Refuses
 * self-loops, cycles and a map fed by the workflow input.
 */
export function connect(params: { schema: WorkflowSchema; from: string; to: string }): EditResult {
  const { schema, from, to } = params;
  const target = schema.steps.find((s) => s.name === to);
  if (!target) return { ok: false, error: `No step "${to}".` };
  if (from === to) return { ok: false, error: "A step cannot depend on itself." };
  if (from === INPUT_NODE_ID) {
    if (target.type === "map") {
      return { ok: false, error: `Map step "${to}" iterates over another step's array output.` };
    }
    return { ok: true, schema: replaceStep({ schema, step: { ...target, dependsOn: [] } }) };
  }
  if (!schema.steps.some((s) => s.name === from)) return { ok: false, error: `No step "${from}".` };
  if (stepDependencies(target).includes(from)) return { ok: true, schema };
  const next: StepSchema =
    target.type === "map"
      ? { ...target, arrayFrom: from }
      : { ...target, dependsOn: [...target.dependsOn, from] };
  const candidate = replaceStep({ schema, step: next });
  if (cycleSteps(candidate).length > 0) {
    return { ok: false, error: `Connecting ${from} → ${to} would create a cycle.` };
  }
  return { ok: true, schema: candidate };
}

/** Remove the edge `from` → `to`. A map's source can only be replaced, not removed. */
export function disconnect(params: {
  schema: WorkflowSchema;
  from: string;
  to: string;
}): EditResult {
  const { schema, from, to } = params;
  const target = schema.steps.find((s) => s.name === to);
  if (!target || from === INPUT_NODE_ID) return { ok: true, schema };
  if (target.type === "map") {
    return {
      ok: false,
      error: `Map step "${to}" needs an array source; connect another step to replace it.`,
    };
  }
  return {
    ok: true,
    schema: replaceStep({
      schema,
      step: { ...target, dependsOn: target.dependsOn.filter((d) => d !== from) },
    }),
  };
}

/** True when connecting `from` → `to` would close a cycle (for disabling targets). */
export function wouldCreateCycle(params: {
  schema: WorkflowSchema;
  from: string;
  to: string;
}): boolean {
  if (params.from === INPUT_NODE_ID) return false;
  if (params.from === params.to) return true;
  const result = connect(params);
  return !result.ok && result.error.includes("cycle");
}

/** Steps on or downstream of a dependency cycle (empty when acyclic). */
export function cycleSteps(schema: WorkflowSchema): string[] {
  const names = new Set(schema.steps.map((s) => s.name));
  const remaining = new Map(
    schema.steps.map((s) => [s.name, stepDependencies(s).filter((d) => names.has(d)).length]),
  );
  const dependents = new Map<string, string[]>();
  for (const step of schema.steps) {
    for (const dep of stepDependencies(step)) {
      if (names.has(dep)) dependents.set(dep, [...(dependents.get(dep) ?? []), step.name]);
    }
  }
  const ready = [...remaining].filter(([, n]) => n === 0).map(([name]) => name);
  while (ready.length > 0) {
    const name = ready.pop()!;
    remaining.delete(name);
    for (const next of dependents.get(name) ?? []) {
      const n = remaining.get(next)! - 1;
      remaining.set(next, n);
      if (n === 0) ready.push(next);
    }
  }
  return [...remaining.keys()].sort();
}

/** `base`, or `base2`, `base3`, … — the first name no step has. */
export function uniqueStepName(params: { schema: WorkflowSchema; base: string }): string {
  const names = new Set(params.schema.steps.map((s) => s.name));
  const base = params.base || "step";
  if (!names.has(base)) return base;
  for (let i = 2; ; i++) {
    if (!names.has(`${base}${i}`)) return `${base}${i}`;
  }
}

/** A step name from an activity id: its last segment, e.g. `text.upper` → `upper`. */
export function baseStepName(activityId: string): string {
  const last = activityId.split(".").at(-1) ?? activityId;
  return last.replace(/[^A-Za-z0-9_-]/g, "_") || "step";
}

/** A new workflow's starting schema: one step on the first plain activity. */
export function starterSchema(params: {
  readonly name: string;
  readonly activities: ReadonlyArray<ActivityDescriptor>;
}): WorkflowSchema {
  const activity = params.activities.find((a) => !isPredicate(a)) ?? params.activities[0];
  const step = newStep({
    kind: "step",
    name: activity ? baseStepName(activity.id) : "step",
    activities: params.activities,
    ...(activity && { activity }),
  });
  return withLayout({ version: 1, name: params.name, steps: [step] });
}
