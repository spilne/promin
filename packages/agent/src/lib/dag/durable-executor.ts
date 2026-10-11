// ---------------------------------------------------------------------------
// Durable DAG executor — walks a registered AgenticDagRecipe inside a
// journaled workflow; every node runs its agent's turn as a child workflow.
//
// A run names its DAG by id + version; the recipe is read from the DAG
// registry in the run's first journaled step (`load-dag`), so a run can
// only ever execute a registered DAG, and a replay sees the recipe the run
// started with.
//
// Every node is a branch of one `ctx.parallel`. A node starts as soon as
// all its upstream nodes have settled — no wave barrier — so independent
// paths run side by side. A node is
//
//   node-<id>-start   journaled: whether the node starts (not when the DAG
//                     was aborted by then), so a replay takes the same path
//   node-<id>-route   journaled: the node's scope (the owner's instance of
//                     its agent, when the run has an owner), the resolver's
//                     check (unknown / disabled agents refuse) and the turn
//                     input — so a replay never resolves it again
//   node-<id>         the agent's turn as a child workflow with id
//                     `<dag run id>.node-<id>`, linked to the DAG run
//
// A node failing with `onError: "abort"` aborts the DAG: nodes that have
// not started never start, and running ones are stopped (their turns end
// as cancelled). Waiting on upstream nodes is not journaled — it only
// orders the branches; every decision that depends on timing is.
//
// Replay semantics: a node whose child finished replays its outcome; a DAG
// run that stopped while a node ran (a crash, a restart) re-attaches to the
// same child run and resumes it from its own journal.
//
// Register the workflow once at boot under its fixed name, so recovery
// and the run's scanners find its definition after a restart.
// ---------------------------------------------------------------------------

import { workflow, type ActivityYield, type Workflow } from "@promin/workflow";
import type { AgentInput, AgentResult } from "../agent-action.ts";
import type { Agent, AgentScope } from "../agent/types.ts";
import { formatToolError } from "../agent-shared.ts";
import { isChildControlFlow } from "../delegation/run-delegated-call.ts";
import { agentTurnWorkflow } from "../delegation/sub-agent-tool.ts";
import { isDelegableAgent } from "../delegation/types.ts";
import { composeAgentInstanceId, type AgentInstanceRegistry } from "../instance/types.ts";
import { checkBlockers, pickPath, resolveNodeInputs, stringifyTask } from "./dag-helpers.ts";
import type { DagRegistry } from "./registry.ts";
import {
  isAgentRefusal,
  type AgentResolver,
  type AgenticDagRecipe,
  type DagEdge,
  type DagNode,
  type DagRunScope,
} from "./types.ts";
import { topologicalOrder, validateDag } from "./validate.ts";

/** Name the DAG workflow is registered under unless configured otherwise. */
export const DAG_WORKFLOW_NAME = "agentic-dag";

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

/** The child run id of a node: DAG run id + node id. */
export function dagNodeRunId(params: {
  readonly dagRunId: string;
  readonly nodeId: string;
}): string {
  return `${params.dagRunId}.node-${params.nodeId}`;
}

/** What a DAG run is started with: a registered DAG, by reference. */
export interface DurableDagInput {
  readonly dagId: string;
  readonly version: string;
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
  /** Registry the run's DAG is read from (by `dagId` + `version`). */
  readonly registry: DagRegistry;
  readonly resolver: AgentResolver;
  /**
   * Instance registry. With it, a run scoped by `ownerId` resolves (or
   * creates) the owner's instance of each node's agent and runs the node
   * there; without it the node runs at the composed instance id without
   * recording an instance row.
   */
  readonly instances?: AgentInstanceRegistry;
  /** Workflow name. Default `DAG_WORKFLOW_NAME`. */
  readonly name?: string;
  /** Workflow type tag — surfaces in the dashboard. Default `"agentic-dag"`. */
  readonly type?: string;
}

/** How one node settled. */
type NodeState =
  | { readonly kind: "output"; readonly value: unknown }
  | { readonly kind: "error"; readonly error: string }
  | { readonly kind: "skipped" }
  | { readonly kind: "not-started" };

type NodeOutcome =
  | { readonly ok: true; readonly output: unknown }
  | { readonly ok: false; readonly error: string };

type NodeRoute =
  | {
      readonly ok: true;
      readonly scope: AgentScope;
      readonly input: AgentInput;
      readonly kind: "turn" | "invoke";
    }
  | { readonly ok: false; readonly error: string };

interface Settled {
  readonly promise: Promise<NodeState>;
  resolve(state: NodeState): void;
  reject(err: unknown): void;
}

function settled(): Settled {
  let resolve!: (state: NodeState) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<NodeState>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

/** The recipe without the registry's timestamps. */
function recipeOf(dag: AgenticDagRecipe): AgenticDagRecipe {
  return {
    id: dag.id,
    version: dag.version,
    nodes: dag.nodes,
    edges: dag.edges,
    entry: dag.entry,
    terminals: dag.terminals,
    ...(dag.metadata !== undefined && { metadata: dag.metadata }),
  };
}

/**
 * Build the workflow that durably executes registered DAGs. Register it
 * once at boot (it carries the resolver, which can't be serialized), then
 * start runs with `{ dagId, version, initialInput, scope }`.
 */
export function createDagWorkflow(
  config: CreateDagWorkflowConfig,
): Workflow<DurableDagInput, DurableDagOutput> {
  const { registry, resolver, instances, name = DAG_WORKFLOW_NAME, type = "agentic-dag" } = config;

  return workflow<DurableDagInput>({ name, type })
    .journaled("execute-dag", function* (ctx, input) {
      const dag = yield* ctx.activity(
        "load-dag",
        async (): Promise<AgenticDagRecipe> => {
          const found = await registry.get(input.dagId, input.version);
          if (!found) {
            throw new Error(`dag_not_found: DAG "${input.dagId}" version ${input.version}`);
          }
          return recipeOf(found);
        },
        { idempotent: true },
      );
      // Validation + ordering are pure; safe to re-run on every replay.
      validateDag(dag);
      const order = topologicalOrder(dag);

      const nodeById = new Map<string, DagNode>(dag.nodes.map((n) => [n.id, n]));
      const incoming = new Map<string, DagEdge[]>();
      for (const id of nodeById.keys()) incoming.set(id, []);
      for (const e of dag.edges) incoming.get(e.to)!.push(e);

      const states = new Map<string, NodeState>();
      const settles = new Map<string, Settled>(order.map((id) => [id, settled()]));
      // Live abort: set when a node fails with onError "abort". It only
      // steers what happens next; whether a node started is journaled.
      const abort = new AbortController();

      const outputs = (): Record<string, unknown> => {
        const out: Record<string, unknown> = {};
        for (const [id, s] of states) if (s.kind === "output") out[id] = s.value;
        return out;
      };
      const errors = (): Record<string, string> => {
        const out: Record<string, string> = {};
        for (const [id, s] of states) if (s.kind === "error") out[id] = s.error;
        return out;
      };
      const skippedIds = (): Set<string> =>
        new Set([...states].filter(([, s]) => s.kind === "skipped").map(([id]) => id));

      function settle(node: DagNode, state: NodeState): void {
        states.set(node.id, state);
        if (state.kind === "error" && (node.onError ?? "abort") === "abort") abort.abort();
        settles.get(node.id)!.resolve(state);
      }

      function* nodeBranch(node: DagNode): Generator<ActivityYield, void, any> {
        try {
          const upstream = incoming.get(node.id)!.map((e) => settles.get(e.from)!.promise);
          if (upstream.length > 0) {
            // Ordering only, not journaled: upstream outcomes are.
            yield { _tag: "Activity", name: `await-${node.id}`, promise: Promise.all(upstream) };
          }
          // Downstream of an abort, or of a node that never started: never starts.
          const stopped = incoming.get(node.id)!.some((e) => {
            const from = states.get(e.from);
            return (
              from?.kind === "not-started" ||
              (from?.kind === "error" && (nodeById.get(e.from)!.onError ?? "abort") === "abort")
            );
          });
          if (stopped) return settle(node, { kind: "not-started" });
          const blocked = checkBlockers({
            incomingEdges: incoming.get(node.id)!,
            outputs: outputs(),
            errors: errors(),
            skipped: skippedIds(),
          });
          if (blocked) return settle(node, { kind: "skipped" });
          let nodeInput: Record<string, unknown>;
          try {
            nodeInput = resolveNodeInputs({
              node,
              initial: input.initialInput,
              outputs: outputs(),
            });
          } catch (err) {
            return settle(node, { kind: "error", error: formatToolError(err) });
          }
          const start = yield* ctx.activity(
            `node-${node.id}-start`,
            async () => !abort.signal.aborted,
            {
              idempotent: true,
            },
          );
          if (!start) return settle(node, { kind: "not-started" });
          const outcome = yield* runNode({ node, nodeInput, scope: input.scope });
          settle(
            node,
            outcome.ok
              ? { kind: "output", value: outcome.output }
              : { kind: "error", error: outcome.error },
          );
        } catch (err) {
          // Engine control flow (a suspended child): waiting branches end too.
          settles.get(node.id)!.reject(err);
          throw err;
        }
      }

      /** One node: route it (journaled), then run its agent's turn as a child. */
      function* runNode(params: {
        node: DagNode;
        nodeInput: Record<string, unknown>;
        scope: DagRunScope;
      }): Generator<ActivityYield, NodeOutcome, any> {
        const { node, nodeInput } = params;
        const task = stringifyTask(nodeInput);
        const routed: NodeRoute = yield* ctx.activity(
          `node-${node.id}-route`,
          async (): Promise<NodeRoute> => {
            const scope = await nodeScope({
              runScope: params.scope,
              agentId: node.agentId,
              instances,
            });
            let agent: Agent;
            try {
              agent = (await resolver({ agentId: node.agentId, scope })).withScope(scope);
            } catch (err) {
              // A refusal (unknown / disabled agent) is the node's outcome; any
              // other error (a store that is down) fails this activity.
              if (isAgentRefusal(err)) return { ok: false, error: err.message };
              throw err;
            }
            if (isDelegableAgent(agent)) {
              const turnInput = (await agent.prepareTurn({ task })).input;
              return { ok: true, scope, input: turnInput, kind: "turn" };
            }
            return { ok: true, scope, input: { task }, kind: "invoke" };
          },
          { idempotent: true },
        );
        if (!routed.ok) return routed;

        let result: AgentResult;
        try {
          result = yield* ctx.child<AgentResult>(
            {
              name: `node-${node.id}`,
              load: async () => {
                // Checked again when the node (re)starts: an agent disabled
                // since the node was routed does not run, and a run left in
                // flight is never driven by another kind of backend.
                const agent = (
                  await resolver({ agentId: node.agentId, scope: routed.scope })
                ).withScope(routed.scope);
                const kind = isDelegableAgent(agent) ? "turn" : "invoke";
                if (kind !== routed.kind) {
                  throw new Error(
                    `agent_backend_changed: "${node.agentId}" was routed as ${routed.kind}, now resolves as ${kind}`,
                  );
                }
                const turn = agentTurnWorkflow({
                  agent,
                  agentId: node.agentId,
                  signal: abort.signal,
                });
                return turn as unknown as Workflow<unknown, AgentResult>;
              },
            },
            {
              workflowId: dagNodeRunId({ dagRunId: ctx.workflowId, nodeId: node.id }),
              input: routed.input,
            },
          );
        } catch (err) {
          if (isChildControlFlow(err)) throw err;
          return { ok: false, error: formatToolError(err) };
        }
        if (result.finishReason === "cancelled") {
          return { ok: false, error: `node "${node.id}" was cancelled` };
        }
        const text = result.answer;
        return {
          ok: true,
          output:
            node.outputPath !== undefined
              ? pickPath({ text, ...nodeInput }, node.outputPath)
              : text,
        };
      }

      yield* ctx.parallel(order.map((id) => nodeBranch(nodeById.get(id)!)));

      // Assembled in topological order, so live runs and replays agree.
      const nodeOutputs: Record<string, unknown> = {};
      const nodeErrors: Record<string, string> = {};
      const skipped: string[] = [];
      let aborted = false;
      for (const id of order) {
        const state = states.get(id);
        const node = nodeById.get(id)!;
        if (state?.kind === "output") nodeOutputs[id] = state.value;
        if (state?.kind === "error") {
          nodeErrors[id] = state.error;
          if ((node.onError ?? "abort") === "abort") aborted = true;
          else skipped.push(id);
        }
        if (state?.kind === "skipped") skipped.push(id);
      }
      const finalOutputs: Record<string, unknown> = {};
      for (const id of dag.terminals) {
        if (id in nodeOutputs) finalOutputs[id] = nodeOutputs[id];
      }
      return {
        outputs: finalOutputs,
        ok: !aborted,
        nodeOutputs,
        errors: nodeErrors,
        skipped,
      };
    })
    .build();
}
