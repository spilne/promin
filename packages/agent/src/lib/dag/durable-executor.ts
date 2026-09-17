// ---------------------------------------------------------------------------
// Durable DAG executor — wraps the in-process executeDag walker in a
// journaled workflow so each node call becomes a checkpoint.
//
// Replay semantics:
//   - The generator body runs from the top on every attempt.
//   - validateDag + topologicalOrder are pure; safe to re-run each time.
//   - Each node's agent.invoke is wrapped in `ctx.activity(`node-${id}`, ...)`.
//     Completed activities return their journaled output without re-firing
//     the agent call, so a worker crash mid-DAG resumes from the last
//     completed node.
//
// The host wires its AgentResolver into createDagWorkflow at boot; the
// resolver is closure-captured so it doesn't need to round-trip through
// JSON (which it can't — Agent instances aren't serializable).
// ---------------------------------------------------------------------------

import { workflow, type Workflow } from "@promin/workflow";
import type { AgentResolver } from "./executor.ts";
import { pickPath } from "./executor.ts";
import {
  type AgenticDagRecipe,
  type DagEdge,
  type DagNode,
  type NodeInputSource,
} from "./types.ts";
import { topologicalOrder, validateDag } from "./validate.ts";

export interface DurableDagInput {
  readonly dag: AgenticDagRecipe;
  readonly initialInput: Readonly<Record<string, unknown>>;
}

export interface DurableDagOutput {
  /** Outputs by terminal-node id. */
  readonly outputs: Readonly<Record<string, unknown>>;
  /** True when no node tripped an `abort`. Skipped failures still allow ok=true. */
  readonly ok: boolean;
  /** Per-node outputs (full state, not just terminals). */
  readonly nodeOutputs: Readonly<Record<string, unknown>>;
  /** Per-node errors (only failed nodes). */
  readonly errors: Readonly<Record<string, string>>;
  /** Node ids that were skipped (upstream blocker / failed condition / onError). */
  readonly skipped: ReadonlyArray<string>;
}

export interface CreateDagWorkflowConfig {
  readonly resolver: AgentResolver;
  /** Workflow name. Default `"agentic-dag"`. */
  readonly name?: string;
  /** Workflow type tag — surfaces in the dashboard. Default `"agentic-dag"`. */
  readonly type?: string;
}

/**
 * Build a workflow definition that durably executes any DAG passed to
 * it. Register the returned `Workflow` with the runner, then trigger
 * runs by passing `{ dag, initialInput }`. Each node call is journaled,
 * so on worker restart the run resumes from the last completed node.
 */
export function createDagWorkflow(
  config: CreateDagWorkflowConfig,
): Workflow<DurableDagInput, DurableDagOutput> {
  const { resolver, name = "agentic-dag", type = "agentic-dag" } = config;

  return workflow<DurableDagInput>({ name, type })
    .journaled("execute-dag", function* (ctx, input) {
      // Validation + ordering are pure; safe to re-run on every replay.
      validateDag(input.dag);
      const order = topologicalOrder(input.dag);

      const nodeById = new Map<string, DagNode>(input.dag.nodes.map((n) => [n.id, n]));
      const incoming = new Map<string, DagEdge[]>();
      for (const id of nodeById.keys()) incoming.set(id, []);
      for (const e of input.dag.edges) incoming.get(e.to)!.push(e);

      const outputs: Record<string, unknown> = {};
      const errors: Record<string, string> = {};
      const skipped: string[] = [];
      let aborted = false;

      for (const nodeId of order) {
        if (aborted) break;
        const node = nodeById.get(nodeId)!;

        const blockingReason = checkBlockers({
          incomingEdges: incoming.get(nodeId) ?? [],
          outputs,
          errors,
          skipped: new Set(skipped),
        });
        if (blockingReason) {
          skipped.push(nodeId);
          continue;
        }

        let nodeInput: Record<string, unknown>;
        try {
          nodeInput = resolveInputs(node, input.initialInput, outputs);
        } catch (err) {
          errors[nodeId] = err instanceof Error ? err.message : String(err);
          if ((node.onError ?? "abort") === "abort") {
            aborted = true;
            break;
          }
          skipped.push(nodeId);
          continue;
        }

        // Per-node journaled activity. On replay, this returns the
        // recorded output without invoking the agent again.
        try {
          const output = yield* ctx.activity(
            `node-${nodeId}`,
            async () => {
              const agent = await resolver(node.agentId);
              const run = await agent.invoke({ task: stringifyTask(nodeInput) });
              const text = await run.text;
              return node.outputPath !== undefined
                ? pickPath({ text, ...nodeInput }, node.outputPath)
                : text;
            },
            // Mark idempotent: on ambiguous-outcome (worker crashed
            // between pending-write and completion-write), the runner
            // re-runs the activity. Agent invocations are *not* truly
            // idempotent (LLMs are stochastic + many tools have side
            // effects), but the alternative — halting the whole DAG on
            // any worker crash — is much worse for the operator-authored
            // multi-specialist use case. Hosts that wire side-effecting
            // tools should mark them with their own approval gates.
            { idempotent: true },
          );
          outputs[nodeId] = output;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          errors[nodeId] = msg;
          if ((node.onError ?? "abort") === "abort") {
            aborted = true;
            break;
          }
          skipped.push(nodeId);
        }
      }

      const finalOutputs: Record<string, unknown> = {};
      for (const id of input.dag.terminals) {
        if (id in outputs) finalOutputs[id] = outputs[id];
      }

      return {
        outputs: finalOutputs,
        ok: !aborted,
        nodeOutputs: outputs,
        errors,
        skipped,
      };
    })
    .build();
}

// ---------------------------------------------------------------------------
// helpers — duplicated from executor.ts intentionally; the durable path
// must stay free of side-effects between yields, and re-using the
// executor's helpers (which are pure) is fine but importing them keeps
// this file's boundary clear.
// ---------------------------------------------------------------------------

function checkBlockers(args: {
  incomingEdges: DagEdge[];
  outputs: Record<string, unknown>;
  errors: Record<string, string>;
  skipped: Set<string>;
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
        return `edge condition not met from ${e.from}`;
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

function stringifyTask(input: Record<string, unknown>): string {
  const entries = Object.entries(input);
  if (entries.length === 1 && typeof entries[0]![1] === "string") {
    return entries[0]![1] as string;
  }
  return JSON.stringify(input, null, 2);
}
