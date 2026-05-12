// ---------------------------------------------------------------------------
// DagExecutor — walks an AgenticDagRecipe, calling agents per node and
// threading outputs along edges.
//
// Resolution model:
//   1. Validate DAG statically (validateDag).
//   2. Compute topological order.
//   3. For each node in order:
//      - resolve inputs from {initial, literal, upstream-node-output}
//      - check edge conditions for each incoming edge — if any source
//        edge has a `condition` that didn't match, skip the node (its
//        downstream chain is also skipped unless reachable via another
//        path)
//      - resolve agent recipe via AgentResolver, invoke, capture output
//      - on error: respect node.onError (abort | skip)
//   4. Return outputs for terminal nodes.
//
// Durability is OUT OF SCOPE here — this is the core in-process walker.
// Wrap calls in `runJournaledStep` from the workflow layer to make a
// long DAG resumable on worker restart.
// ---------------------------------------------------------------------------

import type { AgentRegistry } from "../registry/types.ts";
import type { Agent } from "../agent/types.ts";
import {
  type AgenticDagRecipe,
  type DagNode,
  type DagEdge,
  type DagRunState,
  type NodeInputSource,
} from "./types.ts";
import { topologicalOrder, validateDag } from "./validate.ts";

/**
 * Strategy for resolving a node's `agentId` to an Agent instance. The
 * caller supplies this so we don't bake any one resolution path (host
 * may pull from a registry, a local cache, a federated lookup, etc).
 */
export type AgentResolver = (agentId: string, version?: string) => Promise<Agent>;

export interface DagExecuteParams {
  readonly dag: AgenticDagRecipe;
  readonly initialInput: Readonly<Record<string, unknown>>;
  readonly resolver: AgentResolver;
  /**
   * Optional hook fired for each node's lifecycle. Useful for streaming
   * progress to a UI without coupling the executor to a transport.
   */
  readonly onEvent?: (event: DagExecutionEvent) => void;
}

export type DagExecutionEvent =
  | { readonly kind: "node-start"; readonly nodeId: string; readonly input: unknown }
  | {
      readonly kind: "node-complete";
      readonly nodeId: string;
      readonly output: unknown;
    }
  | { readonly kind: "node-skipped"; readonly nodeId: string; readonly reason: string }
  | {
      readonly kind: "node-failed";
      readonly nodeId: string;
      readonly error: string;
      readonly aborted: boolean;
    };

export interface DagExecutionResult {
  readonly state: DagRunState;
  /** Final outputs by terminal-node id. */
  readonly outputs: Readonly<Record<string, unknown>>;
  /** True when no node tripped an `abort`. Skipped failures still allow ok=true. */
  readonly ok: boolean;
}

export async function executeDag(params: DagExecuteParams): Promise<DagExecutionResult> {
  validateDag(params.dag);
  const order = topologicalOrder(params.dag);
  const nodeById = new Map<string, DagNode>(params.dag.nodes.map((n) => [n.id, n]));
  const incoming = new Map<string, DagEdge[]>();
  for (const id of nodeById.keys()) incoming.set(id, []);
  for (const e of params.dag.edges) incoming.get(e.to)!.push(e);

  const outputs: Record<string, unknown> = {};
  const errors: Record<string, string> = {};
  const skipped = new Set<string>();
  const started: string[] = [];
  let aborted = false;

  for (const nodeId of order) {
    if (aborted) break;
    const node = nodeById.get(nodeId)!;

    // Skip if any incoming edge's source was skipped/errored, OR if any
    // condition didn't match. This is a conservative "AND" join — for
    // explicit OR semantics, the planner emits a "merge" node; v0 keeps
    // the executor simple.
    const incomingEdges = incoming.get(nodeId) ?? [];
    const blockingReason = checkBlockers({
      incomingEdges,
      outputs,
      errors,
      skipped,
      nodeById,
    });
    if (blockingReason) {
      skipped.add(nodeId);
      params.onEvent?.({ kind: "node-skipped", nodeId, reason: blockingReason });
      continue;
    }

    // Resolve inputs.
    let input: Record<string, unknown>;
    try {
      input = resolveInputs(node, params.initialInput, outputs);
    } catch (err) {
      errors[nodeId] = err instanceof Error ? err.message : String(err);
      const willAbort = (node.onError ?? "abort") === "abort";
      params.onEvent?.({
        kind: "node-failed",
        nodeId,
        error: errors[nodeId]!,
        aborted: willAbort,
      });
      if (willAbort) {
        aborted = true;
        break;
      }
      skipped.add(nodeId);
      continue;
    }

    started.push(nodeId);
    params.onEvent?.({ kind: "node-start", nodeId, input });

    try {
      const agent = await params.resolver(node.agentId);
      const run = await agent.invoke({ task: stringifyTask(input) });
      const text = await run.text;
      const output =
        node.outputPath !== undefined ? pickPath({ text, ...input }, node.outputPath) : text;
      outputs[nodeId] = output;
      params.onEvent?.({ kind: "node-complete", nodeId, output });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors[nodeId] = msg;
      const willAbort = (node.onError ?? "abort") === "abort";
      params.onEvent?.({ kind: "node-failed", nodeId, error: msg, aborted: willAbort });
      if (willAbort) {
        aborted = true;
        break;
      }
      skipped.add(nodeId);
    }
  }

  const finalOutputs: Record<string, unknown> = {};
  for (const id of params.dag.terminals) {
    if (id in outputs) finalOutputs[id] = outputs[id];
  }

  const state: DagRunState = {
    outputs,
    errors,
    started,
  };
  return { state, outputs: finalOutputs, ok: !aborted };
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function checkBlockers(args: {
  incomingEdges: DagEdge[];
  outputs: Record<string, unknown>;
  errors: Record<string, string>;
  skipped: Set<string>;
  nodeById: Map<string, DagNode>;
}): string | null {
  for (const e of args.incomingEdges) {
    if (args.skipped.has(e.from)) {
      return `upstream node ${e.from} was skipped`;
    }
    if (e.from in args.errors && !(e.from in args.outputs)) {
      return `upstream node ${e.from} failed`;
    }
    if (e.condition) {
      const sourceOut = args.outputs[e.from];
      const observed = pickPath(sourceOut, e.condition.path);
      if (observed !== e.condition.value) {
        return `edge ${e.from}→${args.nodeById.get(e.from)?.id ?? "?"} condition not met`;
      }
    }
  }
  return null;
}

function resolveInputs(
  node: DagNode,
  initial: Readonly<Record<string, unknown>>,
  outputs: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [field, src] of Object.entries(node.inputs)) {
    out[field] = resolveSource(src, initial, outputs);
  }
  return out;
}

function resolveSource(
  src: NodeInputSource,
  initial: Readonly<Record<string, unknown>>,
  outputs: Readonly<Record<string, unknown>>,
): unknown {
  switch (src.kind) {
    case "initial":
      return pickPath(initial, src.path);
    case "node": {
      if (!(src.nodeId in outputs)) {
        throw new Error(`Upstream node "${src.nodeId}" has no output yet`);
      }
      return pickPath(outputs[src.nodeId], src.path);
    }
    case "literal":
      return src.value;
  }
}

/**
 * Dotted-path getter on a plain object/string. `""` returns the root.
 * Strings: only `""` valid; any other path returns undefined. Arrays
 * indexed by numeric segment.
 */
export function pickPath(value: unknown, path: string): unknown {
  if (path === "" || path === "$") return value;
  const parts = path.split(".");
  let cur: unknown = value;
  for (const part of parts) {
    if (cur === null || cur === undefined) return undefined;
    if (Array.isArray(cur)) {
      const idx = Number.parseInt(part, 10);
      cur = Number.isFinite(idx) ? cur[idx] : undefined;
    } else if (typeof cur === "object") {
      cur = (cur as Record<string, unknown>)[part];
    } else {
      return undefined;
    }
  }
  return cur;
}

/**
 * Encode an arbitrary input record as a single string `task` for the
 * existing AgentInput shape. v0 stringifies straight; later we'll grow
 * a richer typed input that doesn't round-trip through JSON.
 */
function stringifyTask(input: Record<string, unknown>): string {
  // If there's exactly one field and it's already a string, use it raw —
  // matches the natural "task" mental model for simple chains.
  const entries = Object.entries(input);
  if (entries.length === 1 && typeof entries[0]![1] === "string") {
    return entries[0]![1] as string;
  }
  return JSON.stringify(input, null, 2);
}

/**
 * Resolver that pulls recipes from an AgentRegistry. The caller still
 * supplies the local-recipe → Agent mapping (since that depends on the
 * runtime's resolveLocalAgent + memory + LLM wiring).
 */
export function createRegistryResolver(args: {
  registry: AgentRegistry;
  resolve: (recipeId: string, version?: string) => Promise<Agent>;
}): AgentResolver {
  return async (agentId, version) => {
    const recipe = await args.registry.get(agentId, version);
    if (!recipe) throw new Error(`Recipe not found: ${agentId}@${version ?? "latest"}`);
    return args.resolve(agentId, version);
  };
}
