// ---------------------------------------------------------------------------
// Static validation for AgenticDagRecipe. Runs before execution to
// catch the worst classes of LLM-generated graph bugs:
//   - cycles
//   - dangling edge endpoints
//   - undefined input refs (node id, initial-key, upstream-only)
//   - entry / terminal not in nodes
//   - unreachable nodes from entry
//
// Throws DagValidationError with a list of issues; does NOT validate
// that referenced agentIds exist in a registry — that's a runtime concern.
// ---------------------------------------------------------------------------

import { type AgenticDagRecipe, DagValidationError } from "./types.ts";

export function validateDag(dag: AgenticDagRecipe): void {
  const issues: string[] = [];
  const nodeIds = new Set(dag.nodes.map((n) => n.id));

  // Duplicate node ids.
  if (nodeIds.size !== dag.nodes.length) {
    const seen = new Set<string>();
    for (const n of dag.nodes) {
      if (seen.has(n.id)) issues.push(`duplicate node id: ${n.id}`);
      seen.add(n.id);
    }
  }

  // Entry / terminal node refs exist.
  for (const id of dag.entry) {
    if (!nodeIds.has(id)) issues.push(`entry node not found: ${id}`);
  }
  for (const id of dag.terminals) {
    if (!nodeIds.has(id)) issues.push(`terminal node not found: ${id}`);
  }

  // Edge endpoints exist.
  for (const e of dag.edges) {
    if (!nodeIds.has(e.from)) issues.push(`edge.from not in nodes: ${e.from} → ${e.to}`);
    if (!nodeIds.has(e.to)) issues.push(`edge.to not in nodes: ${e.from} → ${e.to}`);
  }

  // Build adjacency for cycle + reachability checks.
  const outgoing = new Map<string, string[]>();
  for (const id of nodeIds) outgoing.set(id, []);
  for (const e of dag.edges) {
    if (nodeIds.has(e.from) && nodeIds.has(e.to)) {
      outgoing.get(e.from)!.push(e.to);
    }
  }

  // Cycle detection (DFS with grey/black coloring).
  const WHITE = 0;
  const GREY = 1;
  const BLACK = 2;
  const color = new Map<string, number>();
  for (const id of nodeIds) color.set(id, WHITE);

  const dfs = (id: string, stack: string[]): void => {
    color.set(id, GREY);
    stack.push(id);
    for (const next of outgoing.get(id) ?? []) {
      const c = color.get(next);
      if (c === GREY) {
        // Back edge → cycle. Walk stack from `next` to current.
        const i = stack.indexOf(next);
        const cycle = stack.slice(i).concat([next]).join(" → ");
        issues.push(`cycle detected: ${cycle}`);
      } else if (c === WHITE) {
        dfs(next, stack);
      }
    }
    stack.pop();
    color.set(id, BLACK);
  };
  for (const id of nodeIds) {
    if (color.get(id) === WHITE) dfs(id, []);
  }

  // Reachability from entry.
  const reachable = new Set<string>();
  const queue = [...dag.entry];
  while (queue.length > 0) {
    const id = queue.shift()!;
    if (reachable.has(id)) continue;
    reachable.add(id);
    for (const next of outgoing.get(id) ?? []) queue.push(next);
  }
  for (const id of nodeIds) {
    if (!reachable.has(id)) issues.push(`node not reachable from entry: ${id}`);
  }

  // Input source refs: `node` kind must point to an upstream node.
  // Build a "depends on" set per node = transitive predecessors.
  const incoming = new Map<string, Set<string>>();
  for (const id of nodeIds) incoming.set(id, new Set());
  for (const e of dag.edges) {
    if (nodeIds.has(e.from) && nodeIds.has(e.to)) {
      incoming.get(e.to)!.add(e.from);
    }
  }
  // Transitive closure (Floyd-Warshall-ish; small graphs).
  let changed = true;
  while (changed) {
    changed = false;
    for (const id of nodeIds) {
      const set = incoming.get(id)!;
      const before = set.size;
      for (const direct of [...set]) {
        for (const trans of incoming.get(direct) ?? []) set.add(trans);
      }
      if (set.size !== before) changed = true;
    }
  }
  for (const node of dag.nodes) {
    for (const [field, src] of Object.entries(node.inputs)) {
      if (src.kind === "node") {
        if (!nodeIds.has(src.nodeId)) {
          issues.push(`node ${node.id} input ${field} → unknown node ${src.nodeId}`);
        } else if (!incoming.get(node.id)!.has(src.nodeId)) {
          issues.push(
            `node ${node.id} input ${field} reads ${src.nodeId} but ${src.nodeId} isn't reachable upstream`,
          );
        }
      }
    }
  }

  // Entry nodes can only depend on `initial` / `literal` inputs.
  for (const entryId of dag.entry) {
    const node = dag.nodes.find((n) => n.id === entryId);
    if (!node) continue;
    for (const [field, src] of Object.entries(node.inputs)) {
      if (src.kind === "node") {
        issues.push(`entry node ${entryId} input ${field} pulls from another node — not allowed`);
      }
    }
  }

  if (issues.length > 0) throw new DagValidationError(issues);
}

/** Topological sort. Throws DagValidationError on cycle. */
export function topologicalOrder(dag: AgenticDagRecipe): string[] {
  const inDegree = new Map<string, number>();
  for (const n of dag.nodes) inDegree.set(n.id, 0);
  const outgoing = new Map<string, string[]>();
  for (const n of dag.nodes) outgoing.set(n.id, []);
  for (const e of dag.edges) {
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1);
    outgoing.get(e.from)?.push(e.to);
  }
  const ready: string[] = [];
  for (const [id, d] of inDegree) {
    if (d === 0) ready.push(id);
  }
  const order: string[] = [];
  while (ready.length > 0) {
    const id = ready.shift()!;
    order.push(id);
    for (const next of outgoing.get(id) ?? []) {
      const nd = (inDegree.get(next) ?? 0) - 1;
      inDegree.set(next, nd);
      if (nd === 0) ready.push(next);
    }
  }
  if (order.length !== dag.nodes.length) {
    throw new DagValidationError(["cycle detected during topological sort"]);
  }
  return order;
}
