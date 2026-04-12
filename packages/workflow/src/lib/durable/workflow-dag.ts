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
