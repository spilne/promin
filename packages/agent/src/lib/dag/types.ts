import type { Agent, AgentScope } from "../agent/types.ts";
import type { BudgetUsage } from "../budget/budget.ts";
import type {
  RegisterVersionInput,
  Versioned,
  VersionedRegistry,
} from "../versioned-registry/types.ts";

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

export type ConditionScalar = string | number | boolean | null;

export type Condition =
  | { readonly kind: "equals"; readonly path: string; readonly value: ConditionScalar }
  | {
      readonly kind: "in";
      readonly path: string;
      readonly values: ReadonlyArray<ConditionScalar>;
    }
  | { readonly kind: "exists"; readonly path: string }
  | {
      readonly kind: "compare";
      readonly path: string;
      readonly op: "gt" | "gte" | "lt" | "lte";
      readonly value: number;
    }
  | { readonly kind: "and"; readonly all: ReadonlyArray<Condition> }
  | { readonly kind: "or"; readonly any: ReadonlyArray<Condition> }
  | { readonly kind: "not"; readonly of: Condition };

export interface NodeBase {
  readonly id: string;
  readonly join?: "all" | "any";
  /**
   * How to build this node's input. Keys are field names on the agent's
   * input shape; values describe where to fetch them from. The executor
   * assembles `{ [field]: <resolved value> }` and passes it as input.
   */
  readonly inputs: Readonly<Record<string, NodeInputSource>>;
  /**
   * Failure policy. Default `"abort"` halts the whole run.
   *   - `"skip"` — record the failure, mark this node's output null,
   *      continue (downstream nodes that depend on this fail too unless
   *      they also `skip`).
   *   - `"abort"` — propagate the error, halt the DAG.
   */
  readonly onError?: "abort" | "skip";
}

export interface AgentNode extends NodeBase {
  readonly kind: "agent";
  /** Recipe id in the AgentRegistry. The executor resolves it at run time. */
  readonly agentId: string;
  /** A JSON Schema whose validated object becomes this node's output. */
  readonly output?: { readonly schema: Record<string, unknown> };
}

export type TransformOp =
  | { readonly op: "collect"; readonly fields: ReadonlyArray<string> }
  | { readonly op: "pick"; readonly path: string }
  | { readonly op: "flatten" }
  | { readonly op: "compact" }
  | { readonly op: "count" }
  | { readonly op: "dedupe"; readonly by?: string }
  | { readonly op: "filter"; readonly where: Condition }
  | { readonly op: "sort"; readonly by: string; readonly order?: "asc" | "desc" }
  | { readonly op: "take"; readonly count: number };

export interface TransformNode extends NodeBase {
  readonly kind: "transform";
  readonly from?: string;
  readonly ops: ReadonlyArray<TransformOp>;
}

export type DagNode = AgentNode | TransformNode;

/** Durable accounting and timing for one settled DAG node. */
export interface DagNodeStats {
  readonly spend: BudgetUsage;
  readonly durationMs: number;
  readonly items?: number;
  readonly failedItems?: number;
  readonly iterations?: number;
  readonly attempts?: number;
}

export interface DagEdge {
  readonly from: string;
  readonly to: string;
  /** Optional condition evaluated against the source node's output. */
  readonly condition?: Condition;
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

/** The graph part of a DAG — what `validateDag` checks. */
export type DagGraph = Pick<AgenticDagRecipe, "nodes" | "edges" | "entry" | "terminals">;

/** What a DAG version stores: the recipe minus its identity. */
export type DagRecipe = Omit<AgenticDagRecipe, "id" | "version">;

/** A stored DAG version. */
export type RegisteredDag = Versioned<DagRecipe>;

/** `DagRegistry.register` input. */
export type RegisterDagInput = RegisterVersionInput<DagRecipe>;

/** `DagRegistry.list` filter. */
export interface DagFilter {
  /** Only DAGs whose `metadata.tags` include this. */
  readonly tag?: string;
}

/** Versioned store of DAG recipes. `register` rejects invalid graphs with `DagValidationError`. */
export type DagRegistry = VersionedRegistry<DagRecipe, DagFilter>;

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
