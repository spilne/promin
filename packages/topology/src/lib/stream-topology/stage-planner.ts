// ---------------------------------------------------------------------------
// Stage Planner — splits a topology at shuffle boundaries into stages
//
// A topology with N shuffle nodes becomes N+1 stages:
//   source → map → keyBy → shuffle → window → agg → sink
//                           ↓
//   [Stage 0: source→map→keyBy→publish]  [Stage 1: consume→window→agg→sink]
//
// Each stage is a sub-topology that TopologyRunner can execute independently.
// ---------------------------------------------------------------------------

import type { TopologyNode, CompiledTopology, ShuffleNode, SinkNode } from "./types.ts";

// ---------------------------------------------------------------------------
// Stage plan types
// ---------------------------------------------------------------------------

export interface StagePlan {
  /** Ordered list of stages (stage 0 reads from original source). */
  stages: TopologyStage[];
  /** Names of repartition topics created between stages. */
  repartitionTopics: string[];
}

export interface TopologyStage {
  /** Unique stage identifier. */
  id: string;
  /** Where this stage reads from. */
  source: "original" | { repartitionTopic: string };
  /** Where this stage writes to. */
  sink: "terminal" | { repartitionTopic: string };
  /** The topology nodes in this stage. */
  nodes: TopologyNode[];
  /** Sink nodes (only present in the final stage). */
  sinkNodes: SinkNode<unknown>[];
  /** The keyFn from the keyBy node preceding the shuffle (for publishing with key). */
  keyFn?: (value: unknown) => string;
}

// ---------------------------------------------------------------------------
// Planning algorithm
// ---------------------------------------------------------------------------

/**
 * Split a compiled topology into stages at shuffle boundaries.
 *
 * Walks each sink's parent chain to find shuffle nodes, then constructs
 * stages with spliced source/sink connections through repartition topics.
 *
 * @returns StagePlan with stages and repartition topic names.
 *          If no shuffles, returns a single stage with the original topology.
 */
export function planStages(params: { compiled: CompiledTopology; group: string }): StagePlan {
  const { compiled, group } = params;

  // Collect all nodes in the DAG by walking from sinks
  const allNodes = collectAllNodes(compiled);

  // Find shuffle nodes in the DAG
  const shuffleNodes = allNodes.filter((n): n is ShuffleNode<unknown> => n.type === "shuffle");

  // No shuffles → single stage, no splitting needed
  if (shuffleNodes.length === 0) {
    return {
      stages: [
        {
          id: `${group}-stage-0`,
          source: "original",
          sink: "terminal",
          nodes: allNodes,
          sinkNodes: compiled.sinks,
        },
      ],
      repartitionTopics: [],
    };
  }

  // Build repartition topic names for each shuffle
  const shuffleTopics = new Map<ShuffleNode<unknown>, string>();
  shuffleNodes.forEach((node, i) => {
    const name = node.topicName ?? `${group}-repartition-${i}`;
    shuffleTopics.set(node, name);
  });

  // For each sink, walk back to find the chain of shuffles and create stages
  // For simplicity, handle the common case: linear topology with shuffles
  const stages: TopologyStage[] = [];
  const repartitionTopics: string[] = [...shuffleTopics.values()];

  // Walk from source to sinks, splitting at shuffles
  // Find the ordered shuffle boundaries by walking the first sink's parent chain
  const parentChain = buildParentChain(compiled.sinks[0]!);
  const orderedShuffles = parentChain.filter(
    (n): n is ShuffleNode<unknown> => n.type === "shuffle",
  );

  // Stage 0: source → ... → keyBy → publish to repartition[0]
  if (orderedShuffles.length > 0) {
    const firstShuffle = orderedShuffles[0]!;
    const keyFn = findKeyFnBefore(firstShuffle);
    stages.push({
      id: `${group}-stage-0`,
      source: "original",
      sink: { repartitionTopic: shuffleTopics.get(firstShuffle)! },
      nodes: collectNodesBefore(firstShuffle),
      sinkNodes: [],
      keyFn,
    });
  }

  // Middle stages: repartition[i] → ... → keyBy → publish to repartition[i+1]
  for (let i = 0; i < orderedShuffles.length - 1; i++) {
    const currentShuffle = orderedShuffles[i]!;
    const nextShuffle = orderedShuffles[i + 1]!;
    const keyFn = findKeyFnBefore(nextShuffle);
    stages.push({
      id: `${group}-stage-${i + 1}`,
      source: { repartitionTopic: shuffleTopics.get(currentShuffle)! },
      sink: { repartitionTopic: shuffleTopics.get(nextShuffle)! },
      nodes: collectNodesBetween(currentShuffle, nextShuffle),
      sinkNodes: [],
      keyFn,
    });
  }

  // Final stage: repartition[last] → ... → sink
  const lastShuffle = orderedShuffles[orderedShuffles.length - 1]!;
  stages.push({
    id: `${group}-stage-${orderedShuffles.length}`,
    source: { repartitionTopic: shuffleTopics.get(lastShuffle)! },
    sink: "terminal",
    nodes: collectNodesAfter(lastShuffle, parentChain),
    sinkNodes: compiled.sinks,
  });

  return { stages, repartitionTopics };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect all unique nodes by walking from sinks/nodes. */
function collectAllNodes(compiled: CompiledTopology): TopologyNode[] {
  const visited = new Set<TopologyNode>();
  function walk(node: TopologyNode) {
    if (visited.has(node)) return;
    visited.add(node);
    if ("parent" in node && node.parent) walk(node.parent as TopologyNode);
    if (node.type === "join") {
      walk(node.left);
      walk(node.right);
    }
  }
  for (const sink of compiled.sinks) walk(sink);
  for (const node of compiled.nodes) walk(node);
  return [...visited];
}

/** Build ordered parent chain from a node back to the source. */
function buildParentChain(node: TopologyNode): TopologyNode[] {
  const chain: TopologyNode[] = [];
  let current: TopologyNode | undefined = node;
  while (current) {
    chain.unshift(current); // prepend — source first
    current = "parent" in current ? (current as any).parent : undefined;
  }
  return chain;
}

/** Find the keyBy keyFn immediately before a shuffle node. */
function findKeyFnBefore(shuffle: ShuffleNode<unknown>): ((value: unknown) => string) | undefined {
  let current: TopologyNode | undefined = shuffle.parent;
  while (current) {
    if (current.type === "keyBy") return current.keyFn as (value: unknown) => string;
    current = "parent" in current ? (current as any).parent : undefined;
  }
  return undefined;
}

/** Collect nodes from source up to (not including) a shuffle node. */
function collectNodesBefore(shuffle: ShuffleNode<unknown>): TopologyNode[] {
  const nodes: TopologyNode[] = [];
  let current: TopologyNode | undefined = shuffle.parent;
  while (current) {
    nodes.unshift(current);
    current = "parent" in current ? (current as any).parent : undefined;
  }
  return nodes;
}

/** Collect nodes between two shuffle nodes (exclusive on both ends). */
function collectNodesBetween(from: ShuffleNode<unknown>, to: ShuffleNode<unknown>): TopologyNode[] {
  // Walk from `to` back until we hit `from`
  const nodes: TopologyNode[] = [];
  let current: TopologyNode | undefined = to.parent;
  while (current && current !== from) {
    nodes.unshift(current);
    current = "parent" in current ? (current as any).parent : undefined;
  }
  return nodes;
}

/** Collect nodes after the last shuffle (from shuffle's children to sinks). */
function collectNodesAfter(
  shuffle: ShuffleNode<unknown>,
  fullChain: TopologyNode[],
): TopologyNode[] {
  const idx = fullChain.indexOf(shuffle);
  if (idx === -1) return [];
  return fullChain.slice(idx + 1);
}
