// ---------------------------------------------------------------------------
// Durable DAG executor — walks an AgenticDagRecipe inside a journaled
// workflow so each node call becomes a checkpoint.
//
// Replay semantics:
//   - The generator body runs from the top on every attempt.
//   - validateDag + topologicalOrder are pure; safe to re-run each time.
//   - Each node's agent.invoke is wrapped in `ctx.activity(`node-${id}`, ...)`.
//     Completed activities return their journaled output without re-firing
//     the agent call, so a worker crash mid-DAG resumes from the last
//     completed node.
//
// Every node runs bound to the run's `scope` (namespace + resource, or the
// owner's instance of the node's agent), never unscoped. The host checks
// who may start a run in that scope before starting it.
//
// The host wires its AgentResolver into createDagWorkflow at boot; the
// resolver is closure-captured so it doesn't need to round-trip through
// JSON (which it can't — Agent instances aren't serializable).
// ---------------------------------------------------------------------------

import { workflow, type Workflow } from "@promin/workflow";
import type { AgentScope } from "../agent/types.ts";
import { composeAgentInstanceId, type AgentInstanceRegistry } from "../instance/types.ts";
import { checkBlockers, pickPath, resolveNodeInputs, stringifyTask } from "./dag-helpers.ts";
import type { AgentResolver, AgenticDagRecipe, DagEdge, DagNode, DagRunScope } from "./types.ts";
import { topologicalOrder, validateDag } from "./validate.ts";

/**
 * The scope one node runs in: the run's scope, with an owner-scoped run
 * placed at the owner's instance of the node's agent.
 */
async function nodeScope(params: {
  readonly runScope: DagRunScope;
  readonly agentId: string;
  readonly instances: AgentInstanceRegistry | undefined;
}): Promise<AgentScope> {
  const { runScope, agentId, instances } = params;
  if (runScope.ownerId === undefined) {
    return {
      namespaceId: runScope.namespaceId,
      ...(runScope.resourceId !== undefined && { resourceId: runScope.resourceId }),
    };
  }
  const key = {
    namespaceId: runScope.namespaceId,
    registeredAgentId: agentId,
    ownerId: runScope.ownerId,
  };
  const resourceId = instances
    ? (await instances.resolveOrCreate(key)).id
    : composeAgentInstanceId(key);
  return { namespaceId: runScope.namespaceId, resourceId, ownerId: runScope.ownerId };
}

export interface DurableDagInput {
  readonly dag: AgenticDagRecipe;
  readonly initialInput: Readonly<Record<string, unknown>>;
  /** Tenant scope every node runs in. See `DagRunScope`. */
  readonly scope: DagRunScope;
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
  /**
   * Instance registry. With it, a run scoped by `ownerId` resolves (or
   * creates) the owner's instance of each node's agent and runs the node
   * there; without it the node runs at the composed instance id without
   * recording an instance row.
   */
  readonly instances?: AgentInstanceRegistry;
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
  const { resolver, instances, name = "agentic-dag", type = "agentic-dag" } = config;

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
          nodeInput = resolveNodeInputs({ node, initial: input.initialInput, outputs });
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
              const scope = await nodeScope({
                runScope: input.scope,
                agentId: node.agentId,
                instances,
              });
              const resolved = await resolver({ agentId: node.agentId, scope });
              const agent = resolved.withScope(scope);
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
