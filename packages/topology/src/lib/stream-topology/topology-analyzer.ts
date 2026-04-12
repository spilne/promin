// ---------------------------------------------------------------------------
// TopologyAnalyzer — static analysis of topology DAGs
//
// Detects potential issues like missing shuffles before stateful operators.
// ---------------------------------------------------------------------------

import type { TopologyNode, CompiledTopology } from "./types.ts";

export interface TopologyWarning {
  type: "missing-shuffle";
  message: string;
  /** The node that may need a shuffle before it. */
  node: TopologyNode;
}

/** Stateful node types that require same-key-same-partition for correctness. */
const STATEFUL_TYPES = new Set(["aggregate", "process", "dedupe", "join"]);

/**
 * Analyze a topology for potential issues.
 *
 * Currently detects:
 * - **missing-shuffle**: A stateful operator (aggregate, process, dedupe)
 *   follows a keyBy without a shuffle. In distributed mode, this means
 *   events for the same key could arrive at different instances.
 */
export function analyze(compiled: CompiledTopology): TopologyWarning[] {
  const warnings: TopologyWarning[] = [];
  const visited = new Set<TopologyNode>();

  function walk(node: TopologyNode) {
    if (visited.has(node)) return;
    visited.add(node);

    // Check: stateful op after keyBy without shuffle in between
    if (STATEFUL_TYPES.has(node.type)) {
      if (hasKeyByWithoutShuffle(node)) {
        warnings.push({
          type: "missing-shuffle",
          message: `"${node.type}" follows keyBy without .shuffle(). In distributed mode, events for the same key may arrive at different instances. Add .shuffle() after keyBy for correct multi-instance behavior.`,
          node,
        });
      }
    }

    // Walk parents
    if ("parent" in node && node.parent) walk(node.parent as TopologyNode);
    if (node.type === "join") {
      walk(node.left);
      walk(node.right);
    }
  }

  for (const sink of compiled.sinks) walk(sink);
  for (const node of compiled.nodes) walk(node);
  return warnings;
}

/**
 * Check if a stateful node has a keyBy ancestor but no shuffle between them.
 * Returns true if there's a keyBy with no shuffle in between.
 */
function hasKeyByWithoutShuffle(node: TopologyNode): boolean {
  let current: TopologyNode | undefined = "parent" in node ? (node as any).parent : undefined;
  while (current) {
    if (current.type === "shuffle") return false; // shuffle found — no warning
    if (current.type === "keyBy") return true; // keyBy found without shuffle — warn
    current = "parent" in current ? (current as any).parent : undefined;
  }
  return false; // no keyBy found — no warning (different issue)
}
