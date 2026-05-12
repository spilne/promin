// ---------------------------------------------------------------------------
// Agentic DAG — a planned multi-agent execution graph.
//
// Each node references an agent recipe (by id). Edges describe
// data flow + optional routing conditions. The DAG is a first-class,
// version-controlled artifact (lives in the recipe registry alongside
// agent recipes), so revisions become diffable + replay-anchored.
//
// Wraps the existing `findAgent` / `callAgent` network primitives — the
// DAG is essentially a recorded, replay-safe call graph between agents.
// ---------------------------------------------------------------------------

/**
 * Source for a node's input field. The executor evaluates these against
 * the running state when invoking each node:
 *
 *   - `initial`: pull from the executor's initialInput object
 *   - `node`: pull from another node's output (must be upstream in the DAG)
 *   - `literal`: a constant value baked into the recipe
 *
 * Pure data-flow today — no expressions / templates. If you need
 * computed inputs (e.g. "concat node A's output with node B's"), add a
 * dedicated `transform` node kind rather than embedding logic in edges.
 */
export type NodeInputSource =
  | { readonly kind: "initial"; readonly path: string }
  | { readonly kind: "node"; readonly nodeId: string; readonly path: string }
  | { readonly kind: "literal"; readonly value: unknown };

export interface DagNode {
  readonly id: string;
  /**
   * Recipe id in the AgentRegistry. Required for `kind: "agent"`.
   * The executor resolves this to an Agent at run time.
   */
  readonly agentId: string;
  /**
   * How to build this node's input. Keys are field names on the agent's
   * input shape; values describe where to fetch them from. The executor
   * assembles `{ [field]: <resolved value> }` and passes it as input.
   */
  readonly inputs: Readonly<Record<string, NodeInputSource>>;
  /**
   * Optional: when the agent's output isn't a string, project a field
   * out of the structured output. Defaults to `text` (matches AgentRunOutput).
   */
  readonly outputPath?: string;
  /**
   * Failure policy. Default `"abort"` halts the whole run.
   *   - `"skip"` — record the failure, mark this node's output null,
   *      continue (downstream nodes that depend on this fail too unless
   *      they also `skip`).
   *   - `"abort"` — propagate the error, halt the DAG.
   */
  readonly onError?: "abort" | "skip";
}

export interface DagEdge {
  readonly from: string;
  readonly to: string;
  /**
   * Optional condition. When unset, the edge always fires. When set,
   * the executor evaluates the condition against the source node's
   * output before activating the target. v0 supports a string-equals
   * shape only; richer conditions land later as a dedicated node kind.
   */
  readonly condition?: { readonly kind: "equals"; readonly path: string; readonly value: string };
}

export interface AgenticDagRecipe {
  readonly id: string;
  readonly version: string;
  readonly nodes: ReadonlyArray<DagNode>;
  readonly edges: ReadonlyArray<DagEdge>;
  /** Node ids that have no in-edges. The executor starts here in parallel. */
  readonly entry: ReadonlyArray<string>;
  /**
   * Node ids whose outputs collectively form the DAG's final output.
   * The executor returns `{ [terminalId]: nodeOutput }`.
   */
  readonly terminals: ReadonlyArray<string>;
  /** Operator-facing metadata. */
  readonly metadata?: {
    readonly description?: string;
    readonly tags?: ReadonlyArray<string>;
  };
}

/** Per-run state tracked by the executor. */
export interface DagRunState {
  /** Outputs from completed nodes, keyed by node id. */
  readonly outputs: Readonly<Record<string, unknown>>;
  /** Errors from failed nodes, keyed by node id. */
  readonly errors: Readonly<Record<string, string>>;
  /** Set of node ids that have started (kept off `outputs` until completion). */
  readonly started: ReadonlyArray<string>;
}

/** Surfaced when the DAG can't be statically validated. */
export class DagValidationError extends Error {
  readonly issues: ReadonlyArray<string>;
  constructor(issues: ReadonlyArray<string>) {
    super(`DAG validation failed:\n  - ${issues.join("\n  - ")}`);
    this.name = "DagValidationError";
    this.issues = issues;
  }
}
