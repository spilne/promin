// ---------------------------------------------------------------------------
// WorkflowDAG — the serializable step graph of a workflow definition, and
// its Mermaid / DOT (Graphviz) renderings.
// ---------------------------------------------------------------------------

import type { StepDefinition } from "./step-definition.ts";

export interface WorkflowDAG {
  readonly name: string;
  readonly steps: readonly {
    readonly name: string;
    readonly dependsOn: readonly string[];
    /**
     * The step's `StepKind` when built by this package. Typed `string`
     * because a DAG is persisted and read back (stub workflows, the
     * dashboard), possibly from another version that has other kinds.
     */
    readonly kind: string;
    /**
     * Capabilities this step requires from a worker. Empty / omitted = any
     * worker can run it. The coordinator copies this onto the dispatched
     * task at enqueue time; workers claim only tasks whose needs are a
     * subset of their own declared `capabilities`.
     */
    readonly needs?: readonly string[];
    /** Dispatch priority — higher runs first (default 5 at the queue level). */
    readonly priority?: number;
    /**
     * For decision nodes (currently `match`): the named case alternatives.
     * Visualizers render these as labeled outgoing edges so the static
     * diagram shows every possible path the workflow can take, not just
     * the one that fired in some run.
     */
    readonly cases?: readonly string[];
    /** True if a `default` fallback exists for the cases. */
    readonly hasDefault?: boolean;
  }[];
}

/** Project step definitions onto the serializable `WorkflowDAG`. */
export function toWorkflowDag(params: {
  readonly name: string;
  readonly steps: readonly StepDefinition[];
}): WorkflowDAG {
  return {
    name: params.name,
    steps: params.steps.map((s) => ({
      name: s.name,
      dependsOn: s.dependsOn,
      kind: s.kind,
      ...(s.needs && s.needs.length > 0 ? { needs: s.needs } : {}),
      ...(s.priority !== undefined ? { priority: s.priority } : {}),
      ...(s.viz?.cases ? { cases: s.viz.cases } : {}),
      ...(s.viz?.hasDefault ? { hasDefault: true } : {}),
    })),
  };
}

type DagStep = WorkflowDAG["steps"][number];

/** A `match` step with case alternatives renders as a decision node. */
const isDecision = (step: DagStep): boolean =>
  step.kind === "match" && step.cases !== undefined && step.cases.length > 0;

/** Every case label of a decision step, `"default"` last when present. */
const caseLabels = (step: DagStep): readonly string[] => [
  ...(step.cases ?? []),
  ...(step.hasDefault ? ["default"] : []),
];

/**
 * Hands out node ids that are unique within one diagram. `preferred` is
 * used as-is when free; a taken id gets the first free `~2`, `~3`, ...
 * suffix (after `transform`, which may map distinct inputs to one id).
 */
function createIdAllocator(transform: (s: string) => string) {
  const taken = new Set<string>();
  return (preferred: string): string => {
    const base = transform(preferred);
    let id = base;
    for (let n = 2; taken.has(id); n++) id = transform(`${preferred}~${n}`);
    taken.add(id);
    return id;
  };
}

/** Mermaid ids: alphanumerics and `_` only. */
const mermaidId = (s: string): string => s.replace(/[^a-zA-Z0-9]/g, "_");

/** Escape a Mermaid quoted label (`"` would end the string). */
const mermaidLabel = (s: string): string => s.replaceAll('"', "#quot;");

/** Escape a DOT quoted string. */
const dotString = (s: string): string => `"${s.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;

/**
 * Convert a WorkflowDAG to Mermaid graph syntax. Node ids are the step
 * names reduced to `[a-zA-Z0-9_]`; names that reduce to the same id (`a-b`
 * and `a_b`) get distinct ids, so they stay separate nodes.
 */
export function dagToMermaid(dag: WorkflowDAG): string {
  const allocate = createIdAllocator(mermaidId);
  // Step ids first, so a case node never takes a step's id.
  const ids = new Map<string, string>();
  for (const step of dag.steps) ids.set(step.name, allocate(step.name));
  const idOf = (name: string): string => {
    let id = ids.get(name);
    if (id === undefined) ids.set(name, (id = allocate(name)));
    return id;
  };

  const lines: string[] = ["graph LR"];
  for (const step of dag.steps) {
    const id = idOf(step.name);
    const label = mermaidLabel(step.name);
    // Decision nodes get the diamond shape `{...}`; others stay rectangular.
    const decision = isDecision(step);
    lines.push(decision ? `    ${id}{"${label}"}` : `    ${id}["${label}"]`);

    for (const dep of step.dependsOn) {
      lines.push(`    ${idOf(dep)} --> ${id}`);
    }

    // For match nodes, render each case as a labeled phantom node so the
    // alternatives are visible even though only one fires per run.
    if (decision) {
      for (const caseLabel of caseLabels(step)) {
        const caseId = allocate(`${id}_${caseLabel}`);
        const escaped = mermaidLabel(caseLabel);
        lines.push(`    ${caseId}(["${escaped}"])`);
        lines.push(`    ${id} -->|"${escaped}"| ${caseId}`);
      }
    }
  }
  return lines.join("\n");
}

/**
 * Convert a WorkflowDAG to DOT (Graphviz) syntax. Step nodes are named by
 * their step names; a case node is `<step>.<case>` unless that collides
 * with a step (e.g. a `parallelSteps` branch), in which case it gets a
 * `~N` suffix.
 */
export function dagToDot(dag: WorkflowDAG): string {
  const allocate = createIdAllocator((s) => s);
  for (const step of dag.steps) allocate(step.name);

  const lines: string[] = [`digraph ${dotString(dag.name)} {`];
  for (const step of dag.steps) {
    const node = dotString(step.name);
    const decision = isDecision(step);
    lines.push(decision ? `    ${node} [shape=diamond];` : `    ${node};`);
    for (const dep of step.dependsOn) {
      lines.push(`    ${dotString(dep)} -> ${node};`);
    }
    if (decision) {
      for (const caseLabel of caseLabels(step)) {
        const caseNode = dotString(allocate(`${step.name}.${caseLabel}`));
        lines.push(`    ${caseNode} [shape=ellipse];`);
        lines.push(`    ${node} -> ${caseNode} [label=${dotString(caseLabel)}];`);
      }
    }
  }
  lines.push("}");
  return lines.join("\n");
}
