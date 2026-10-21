// ---------------------------------------------------------------------------
// DAG resolution: topological sort, cycle detection, ready-set computation
// ---------------------------------------------------------------------------

import { WorkflowError } from "./durable-pipeline-error.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DagNode {
  readonly name: string;
  readonly dependsOn: string[];
}

// ---------------------------------------------------------------------------
// Topological sort with cycle detection (Kahn's algorithm)
// ---------------------------------------------------------------------------

export function topologicalSort(params: { nodes: DagNode[]; workflowId: string }): string[] {
  const { nodes, workflowId } = params;
  const nameSet = new Set(nodes.map((n) => n.name));

  // Validate all dependencies reference existing steps
  for (const node of nodes) {
    for (const dep of node.dependsOn) {
      if (!nameSet.has(dep)) {
        throw new WorkflowError({
          workflowId,
          message: `Step "${node.name}" depends on unknown step "${dep}"`,
        });
      }
    }
  }

  // Build in-degree map and adjacency list
  const inDegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();

  for (const node of nodes) {
    inDegree.set(node.name, node.dependsOn.length);
    if (!dependents.has(node.name)) {
      dependents.set(node.name, []);
    }
    for (const dep of node.dependsOn) {
      if (!dependents.has(dep)) {
        dependents.set(dep, []);
      }
      dependents.get(dep)!.push(node.name);
    }
  }

  // Start with nodes that have no dependencies
  const queue: string[] = [];
  for (const [name, degree] of inDegree) {
    if (degree === 0) {
      queue.push(name);
    }
  }

  const sorted: string[] = [];

  while (queue.length > 0) {
    const current = queue.shift()!;
    sorted.push(current);

    for (const dependent of dependents.get(current) ?? []) {
      const newDegree = inDegree.get(dependent)! - 1;
      inDegree.set(dependent, newDegree);
      if (newDegree === 0) {
        queue.push(dependent);
      }
    }
  }

  if (sorted.length !== nodes.length) {
    const remaining = nodes.filter((n) => !sorted.includes(n.name)).map((n) => n.name);
    throw new WorkflowError({
      workflowId,
      message: `Cycle detected in workflow DAG involving steps: ${remaining.join(", ")}`,
    });
  }

  return sorted;
}

// ---------------------------------------------------------------------------
// Ready-set computation
// ---------------------------------------------------------------------------

/** Returns step names whose dependencies are all in the completed set. */
export function computeReadySet(params: {
  nodes: DagNode[];
  completed: Set<string>;
  running: Set<string>;
}): string[] {
  const { nodes, completed, running } = params;
  const ready: string[] = [];

  for (const node of nodes) {
    if (completed.has(node.name) || running.has(node.name)) {
      continue;
    }
    const allDepsCompleted = node.dependsOn.every((dep) => completed.has(dep));
    if (allDepsCompleted) {
      ready.push(node.name);
    }
  }

  return ready;
}

// ---------------------------------------------------------------------------
// Incremental ready tracking
// ---------------------------------------------------------------------------

/**
 * Tracks which nodes of a DAG are ready as nodes complete, without
 * rescanning the graph: each node keeps a count of its unfinished
 * dependencies, and completing a node only visits its dependents. Driving
 * a whole DAG through it costs O(V + E), plus sorting each ready batch.
 */
export interface ReadyTracker {
  /**
   * Nodes whose dependencies have all completed and that have not
   * completed themselves, in definition order (the order of `nodes`): the
   * names and order `computeReadySet` returns with nothing running. A node
   * stays ready until it is marked completed.
   */
  ready(): string[];
  /** Mark `name` completed. Unknown or already-completed names are ignored. */
  markCompleted(name: string): void;
  /** Number of nodes marked completed, including the initial ones. */
  readonly completedCount: number;
}

/**
 * A `ReadyTracker` over `nodes`, with the names in `completed` (nodes
 * already done, e.g. replayed from storage) marked completed up front. A
 * dependency on a name that is not a node never completes.
 */
export function createReadyTracker(params: {
  nodes: readonly DagNode[];
  completed?: Iterable<string>;
}): ReadyTracker {
  const { nodes } = params;
  const indexOf = new Map<string, number>();
  nodes.forEach((node, i) => indexOf.set(node.name, i));

  // Unfinished dependencies per node, and each node's dependents.
  const pendingDeps = new Int32Array(nodes.length);
  const dependents: number[][] = nodes.map(() => []);
  nodes.forEach((node, i) => {
    const deps = node.dependsOn;
    pendingDeps[i] = deps.length;
    for (const dep of deps) {
      const d = indexOf.get(dep);
      if (d !== undefined) dependents[d]!.push(i);
    }
  });

  const done = new Uint8Array(nodes.length);
  const readyNow = new Set<number>();
  nodes.forEach((_, i) => {
    if (pendingDeps[i] === 0) readyNow.add(i);
  });
  let completedCount = 0;

  const markCompleted = (name: string): void => {
    const i = indexOf.get(name);
    if (i === undefined || done[i] === 1) return;
    done[i] = 1;
    completedCount++;
    readyNow.delete(i);
    for (const dependent of dependents[i]!) {
      pendingDeps[dependent]!--;
      if (pendingDeps[dependent] === 0 && done[dependent] === 0) readyNow.add(dependent);
    }
  };

  for (const name of params.completed ?? []) markCompleted(name);

  return {
    ready: () => [...readyNow].sort((a, b) => a - b).map((i) => nodes[i]!.name),
    markCompleted,
    get completedCount() {
      return completedCount;
    },
  };
}
