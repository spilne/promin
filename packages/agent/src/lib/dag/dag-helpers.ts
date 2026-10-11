// ---------------------------------------------------------------------------
// Pure helpers for walking an AgenticDagRecipe: edge blockers, node input
// resolution and dotted-path lookup. Side-effect free, so the durable
// executor can re-run them on every replay.
// ---------------------------------------------------------------------------

import type { DagEdge, DagNode, NodeInputSource } from "./types.ts";

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
 * Reason a node can't run given the state of its incoming edges, or
 * `null` when every upstream node completed and every edge condition holds.
 */
export function checkBlockers(args: {
  incomingEdges: ReadonlyArray<DagEdge>;
  outputs: Readonly<Record<string, unknown>>;
  errors: Readonly<Record<string, string>>;
  skipped: ReadonlySet<string>;
}): string | null {
  for (const e of args.incomingEdges) {
    if (args.skipped.has(e.from)) {
      return `upstream node ${e.from} was skipped`;
    }
    if (e.from in args.errors && !(e.from in args.outputs)) {
      return `upstream node ${e.from} failed`;
    }
    if (e.condition) {
      const observed = pickPath(args.outputs[e.from], e.condition.path);
      if (observed !== e.condition.value) {
        return `edge condition not met from ${e.from}`;
      }
    }
  }
  return null;
}

/** Resolve every input field of `node` against the initial input and upstream outputs. */
export function resolveNodeInputs(args: {
  node: DagNode;
  initial: Readonly<Record<string, unknown>>;
  outputs: Readonly<Record<string, unknown>>;
}): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [field, src] of Object.entries(args.node.inputs)) {
    out[field] = resolveSource({ src, initial: args.initial, outputs: args.outputs });
  }
  return out;
}

function resolveSource(args: {
  src: NodeInputSource;
  initial: Readonly<Record<string, unknown>>;
  outputs: Readonly<Record<string, unknown>>;
}): unknown {
  const { src } = args;
  switch (src.kind) {
    case "initial":
      return pickPath(args.initial, src.path);
    case "node": {
      if (!(src.nodeId in args.outputs)) {
        throw new Error(`Upstream node "${src.nodeId}" has no output yet`);
      }
      return pickPath(args.outputs[src.nodeId], src.path);
    }
    case "literal":
      return src.value;
  }
}

/**
 * Encode a node's input record as the single `task` string an Agent takes.
 * A lone string field passes through raw, matching the natural "task"
 * mental model for simple chains; anything else is pretty-printed JSON.
 */
export function stringifyTask(input: Record<string, unknown>): string {
  const entries = Object.entries(input);
  if (entries.length === 1 && typeof entries[0]![1] === "string") {
    return entries[0]![1] as string;
  }
  return JSON.stringify(input, null, 2);
}
