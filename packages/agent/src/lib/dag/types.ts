import type { Agent, AgentScope } from "../agent/types.ts";

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

/**
 * The tenant scope a DAG run acts in. Every node runs bound to it: in
 * `namespaceId`, at `resourceId` — or, with `ownerId`, at that owner's
 * instance of the node's agent (see `composeAgentInstanceId`). Set it
 * from the authorized caller that started the run.
 */
export interface DagRunScope {
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly ownerId?: string;
}

/**
 * Strategy for resolving a node's `agentId` to an Agent. The host supplies
 * this so the executor doesn't bake in one resolution path (a registry, a
 * local cache, a remote lookup, etc). `scope` is the scope the node will
 * run in — the executor binds the returned agent to it with `withScope`.
 * Throw `AgentRefusedError` to refuse a node (unknown or disabled agent);
 * the node then fails under its `onError` policy. Any other error is not a
 * refusal: it fails the DAG step, which retries like any step failure.
 * `createRegistryAgentResolver` is the registry-backed resolver that
 * refuses unknown and disabled recipes.
 */
export type AgentResolver = (params: {
  readonly agentId: string;
  readonly scope: AgentScope;
}) => Promise<Agent>;

/** An agent resolver refused a node: the agent is unknown or disabled. */
export class AgentRefusedError extends Error {
  readonly _tag = "AgentRefusedError";
  constructor(
    readonly code: "agent_not_found" | "agent_disabled",
    message: string,
  ) {
    super(`${code}: ${message}`);
  }
}

/** Whether `err` is an `AgentRefusedError`, also across module copies. */
export function isAgentRefusal(err: unknown): err is AgentRefusedError {
  return (err as { _tag?: unknown } | null)?._tag === "AgentRefusedError";
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
