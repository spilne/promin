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
//                     was aborted by then) and, with a run budget, the
//                     node's share of it — so a replay takes the same path
//   node-<id>-route   journaled: the node's scope (the owner's instance of
//                     its agent, when the run has an owner), the resolver's
//                     check (unknown / disabled agents refuse) and the turn
//                     input — so a replay never resolves it again
//   node-<id>         the agent's turn as a child workflow with id
//                     `<dag run id>.node-<id>`, linked to the DAG run
//
// Node turns never suspend on approvals — nobody could decide them inside a
// DAG run — so an approval-gated tool call is refused (the model gets
// "Rejected: requires approval; not available in DAG runs").
//
// A node failing with `onError: "abort"` aborts the DAG: nodes that have
// not started never start, and running ones are stopped (their turns end
// as cancelled). Waiting on upstream nodes is not journaled — it only
// orders the branches; every decision that depends on timing is.
//
// Budget: a run started with `budget` shares it between its nodes. A node
// starting gets an equal share of what is free — the budget minus what
// settled nodes spent and what running nodes hold — among itself and the
// nodes that have not started yet; a settled node gives back what it did
// not spend. So the nodes together never get more than the run's budget,
// and an early node's leftovers flow to later ones. A node that runs out
// fails with `budget_exceeded`. The run reports what its nodes spent and,
// with a `usageRecorder`, records it (journaled `record-usage`; also when
// the run fails after its nodes spent).
//
// Replay semantics: a node whose child finished replays its outcome; a DAG
// run that stopped while a node ran (a crash, a restart) re-attaches to the
// same child run and resumes it from its own journal.
//
// Register the workflow once at boot under its fixed name, so recovery
// and the run's scanners find its definition after a restart.
// ---------------------------------------------------------------------------

import {
  SystemWallClock,
  workflow,
  type ActivityYield,
  type WallClock,
  type Workflow,
} from "@promin/workflow";
import { formatToolError, isEngineControl } from "../agent-shared.ts";
import {
  addUsage,
  EMPTY_USAGE,
  parseTurnBudget,
  remainingBudget,
  subtractBudget,
  type BudgetUsage,
  type TurnBudget,
} from "../budget/budget.ts";
import type { AgentInstanceRegistry } from "../instance/types.ts";
import { checkBlockers, resolveNodeInputs, resolveNodeInputSource } from "./dag-helpers.ts";
import { emptyDimension, startShare, waitingWeight } from "./node-budget.ts";
import { recipeIssues } from "./recipe-schema.ts";
import { runAgentAttempt } from "./runners/agent-attempt.ts";
import { childRunId, nodeKey, type AttemptOutcome, type NodeRunEnv } from "./runners/env.ts";
import { runFanOut } from "./runners/fan-out.ts";
import { runRepeat } from "./runners/repeat.ts";
import { runSubDagAttempt } from "./runners/sub-dag-attempt.ts";
import { applyTransform } from "./transform.ts";
import type { DagRegistry } from "./types.ts";
import {
  type AgentResolver,
  type AgenticDagRecipe,
  type DagEdge,
  type DagNode,
  type DagNodeStats,
  type DagParentFrame,
  type DagRunScope,
  type SubDagNode,
} from "./types.ts";
import { topologicalOrder, validateDag } from "./validate.ts";

export { DAG_APPROVAL_REFUSED } from "./runners/agent-attempt.ts";

/** Name the DAG workflow is registered under unless configured otherwise. */
export const DAG_WORKFLOW_NAME = "agentic-dag";

/** Maximum nesting depth below a root DAG run. */
export const MAX_DAG_DEPTH = 4;

/** Maximum child workflows a root DAG run and all descendants may start. */
export const MAX_CHILD_RUNS = 500;

/** The child run id of a node: DAG run id + node id. */
export function dagNodeRunId(params: {
  readonly dagRunId: string;
  readonly nodeId: string;
}): string {
  return childRunId({ runId: params.dagRunId, key: nodeKey(params.nodeId) });
}

/** What a DAG run is started with: a registered DAG, by reference. */
export interface DurableDagInput {
  readonly dagId: string;
  readonly version: string;
  readonly initialInput: Readonly<Record<string, unknown>>;
  /** Tenant scope every node runs in. See `DagRunScope`. */
  readonly scope: DagRunScope;
  /** What the whole run may spend, shared between its nodes. Unset: unlimited. */
  readonly budget?: TurnBudget;
  /** Present when another DAG run started this run. */
  readonly parent?: DagParentFrame;
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
  /** Per-node spend and duration, including skipped and not-started nodes. */
  readonly nodeStats: Readonly<Record<string, DagNodeStats>>;
  /**
   * What the run's nodes spent. A node that failed without a result is
   * counted at its whole share of the budget.
   */
  readonly spend: BudgetUsage;
  /** Child workflows this run and all descendant DAG runs started. */
  readonly childRuns: number;
}

export interface CreateDagWorkflowConfig {
  /** Registry the run's DAG is read from (by `dagId` + `version`). */
  readonly registry: DagRegistry;
  readonly resolver: AgentResolver;
  /** Time source used for journaled node start and settlement timestamps. */
  readonly clock?: WallClock;
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
  /**
   * Where a run's spend goes (namespace budgets): called once the run has
   * its outcome, and when it fails after its nodes spent, inside a
   * journaled activity — again only when a crash interrupts it, so make it
   * idempotent per `workflowId` (`UsageLedger.record` is). Node turns
   * record nothing themselves: the run records them all.
   */
  readonly usageRecorder?: DagUsageRecorder;
}

/** Records what a DAG run spent. */
export interface DagUsageRecorder {
  record(params: {
    /** The DAG run's workflow id. */
    readonly workflowId: string;
    readonly dagId: string;
    readonly scope: DagRunScope;
    readonly usage: BudgetUsage;
  }): Promise<void>;
}

/** How one node settled. */
type NodeState =
  | { readonly kind: "output"; readonly value: unknown }
  | { readonly kind: "error"; readonly error: string }
  | { readonly kind: "skipped" }
  | { readonly kind: "not-started" };

type SettlementStats = Pick<DagNodeStats, "items" | "failedItems" | "iterations" | "attempts">;

interface NodeSettlement {
  readonly state: NodeState["kind"];
  readonly error?: string;
  readonly spend: BudgetUsage;
  readonly startedAt: number | null;
  readonly endedAt: number;
  readonly stats?: SettlementStats;
}

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

function subtractUsage(params: {
  readonly usage: BudgetUsage;
  readonly excluded: BudgetUsage;
}): BudgetUsage {
  return {
    inputTokens: Math.max(0, params.usage.inputTokens - params.excluded.inputTokens),
    outputTokens: Math.max(0, params.usage.outputTokens - params.excluded.outputTokens),
    cacheReadTokens: Math.max(0, params.usage.cacheReadTokens - params.excluded.cacheReadTokens),
    cacheWriteTokens: Math.max(0, params.usage.cacheWriteTokens - params.excluded.cacheWriteTokens),
    costUsd: Math.max(0, params.usage.costUsd - params.excluded.costUsd),
    llmCalls: Math.max(0, params.usage.llmCalls - params.excluded.llmCalls),
    toolCalls: Math.max(0, params.usage.toolCalls - params.excluded.toolCalls),
  };
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
  return buildDagWorkflow(config);
}

/** Build a DAG workflow, optionally wired to a parent node's abort signal. */
export function buildDagWorkflow(
  config: CreateDagWorkflowConfig,
  options: { readonly signal?: AbortSignal } = {},
): Workflow<DurableDagInput, DurableDagOutput> {
  const {
    registry,
    name = DAG_WORKFLOW_NAME,
    type = "agentic-dag",
    clock = SystemWallClock,
  } = config;
  const usageRecorder = config.usageRecorder;

  return workflow<DurableDagInput>({ name, type })
    .journaled("execute-dag", function* (ctx, input) {
      const dag = yield* ctx.activity(
        "load-dag",
        async (): Promise<AgenticDagRecipe> => {
          const found = await registry.get({
            id: input.dagId,
            version: input.version,
          });
          if (!found) {
            throw new Error(`dag_not_found: DAG "${input.dagId}" version ${input.version}`);
          }
          return recipeOf(found);
        },
        { idempotent: true },
      );
      const issues = recipeIssues({
        nodes: dag.nodes,
        edges: dag.edges,
        entry: dag.entry,
        terminals: dag.terminals,
        ...(dag.metadata !== undefined && { metadata: dag.metadata }),
      });
      if (issues.length > 0) throw new Error(`dag_invalid: ${issues.join("; ")}`);
      // Validation + ordering are pure; safe to re-run on every replay.
      validateDag({ dag, dagId: dag.id });
      const order = topologicalOrder(dag);

      const nodeById = new Map<string, DagNode>(dag.nodes.map((n) => [n.id, n]));
      const incoming = new Map<string, DagEdge[]>();
      for (const id of nodeById.keys()) incoming.set(id, []);
      for (const e of dag.edges) incoming.get(e.to)!.push(e);

      const states = new Map<string, NodeState>();
      const settlements = new Map<string, NodeSettlement>();
      const fanOutErrors = new Map<string, Readonly<Record<number, string>>>();
      const settles = new Map<string, Settled>(order.map((id) => [id, settled()]));
      // Live abort: set when a node fails with onError "abort". It only
      // steers what happens next; whether a node started is journaled.
      const abort = new AbortController();
      const abortFromParent = (): void => abort.abort();
      if (options.signal?.aborted) abortFromParent();
      else options.signal?.addEventListener("abort", abortFromParent, { once: true });
      const env: NodeRunEnv = {
        ctx,
        input,
        config,
        clock,
        runSignal: abort.signal,
      };

      // Run budget: what settled nodes spent, and the shares running nodes hold.
      const runBudget = input.budget === undefined ? undefined : parseTurnBudget(input.budget);
      let spent: BudgetUsage = EMPTY_USAGE;
      let subDagSpend: BudgetUsage = EMPTY_USAGE;
      const held = new Map<string, TurnBudget>();
      let childRunsAvailable = input.parent?.childRunsAllowed ?? MAX_CHILD_RUNS;
      let childRuns = 0;
      interface SubDagReservation {
        readonly perRun: number;
        readonly total: number;
        readonly slots: number;
        readonly claimed: Set<string>;
      }
      const subDagReservations = new Map<string, SubDagReservation>();

      function* claimChildRun(key: string): Generator<ActivityYield, boolean, any> {
        const provisional = childRunsAvailable > 0;
        if (provisional) childRunsAvailable--;
        const recorded = yield* ctx.activity(`${key}-child-run`, async () => provisional, {
          idempotent: true,
        });
        if (provisional !== recorded) childRunsAvailable += provisional ? 1 : -1;
        if (recorded) childRuns++;
        return recorded;
      }

      const releaseUnusedSubDagGrants = (nodeId: string): void => {
        const reservation = subDagReservations.get(nodeId);
        if (!reservation) return;
        childRunsAvailable += reservation.total - reservation.claimed.size * reservation.perRun;
        subDagReservations.delete(nodeId);
      };
      /** A starting node's share: what is free, split with the nodes not started yet. */
      const shareFor = (node: DagNode, weight = waitingWeight(node)): TurnBudget | null => {
        if (!runBudget) return null;
        let free = remainingBudget({ budget: runBudget, usage: spent });
        for (const taken of held.values()) free = subtractBudget({ budget: free, taken });
        const othersWaiting = order
          .filter((id) => id !== node.id && !states.has(id) && !held.has(id))
          .reduce((sum, id) => sum + waitingWeight(nodeById.get(id)!), 0);
        return startShare({ free, weight, othersWaiting });
      };

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

      function* settleNode(params: {
        node: DagNode;
        state: NodeState;
        spend?: BudgetUsage;
        startedAt: number | null;
        stats?: SettlementStats;
      }): Generator<ActivityYield, void, any> {
        const { node, state } = params;
        const settlement = yield* ctx.activity(
          `${nodeKey(node.id)}-settle`,
          async (): Promise<NodeSettlement> => ({
            state: state.kind,
            ...(state.kind === "error" && { error: state.error }),
            spend: params.spend ?? EMPTY_USAGE,
            startedAt: params.startedAt,
            endedAt: clock.currentTimeMs(),
            ...(params.stats && { stats: params.stats }),
          }),
          { idempotent: true },
        );
        settlements.set(node.id, settlement);
        states.set(node.id, state);
        spent = addUsage(spent, settlement.spend);
        if (state.kind === "error" && (node.onError ?? "abort") === "abort") abort.abort();
        settles.get(node.id)!.resolve(state);
      }

      function* nodeBranch(node: DagNode): Generator<ActivityYield, void, any> {
        try {
          const upstream = incoming.get(node.id)!.map((e) => settles.get(e.from)!.promise);
          if (upstream.length > 0) {
            // Ordering only, not journaled: upstream outcomes are.
            yield {
              _tag: "Activity",
              name: `await-${node.id}`,
              promise: Promise.all(upstream),
            };
          }
          // Downstream of an abort, or of a node that never started: never starts.
          const stopped = incoming.get(node.id)!.some((e) => {
            const from = states.get(e.from);
            return (
              from?.kind === "not-started" ||
              (from?.kind === "error" && (nodeById.get(e.from)!.onError ?? "abort") === "abort")
            );
          });
          if (stopped) {
            return yield* settleNode({
              node,
              state: { kind: "not-started" },
              startedAt: null,
            });
          }
          const blocked = checkBlockers({
            incomingEdges: incoming.get(node.id)!,
            join: node.join ?? "all",
            outputs: outputs(),
            errors: errors(),
            skipped: skippedIds(),
          });
          if (blocked) {
            return yield* settleNode({
              node,
              state: { kind: "skipped" },
              startedAt: null,
            });
          }
          let nodeInput: Record<string, unknown>;
          try {
            nodeInput = resolveNodeInputs({
              node,
              initial: input.initialInput,
              outputs: outputs(),
            });
          } catch (err) {
            return yield* settleNode({
              node,
              state: { kind: "error", error: formatToolError(err) },
              startedAt: null,
            });
          }
          let fanOutItems: ReadonlyArray<unknown> | undefined;
          let weight = waitingWeight(node);
          if (node.kind !== "transform" && node.forEach !== undefined) {
            const resolved = resolveNodeInputSource({
              src: node.forEach.items,
              initial: input.initialInput,
              outputs: outputs(),
            });
            if (!Array.isArray(resolved)) {
              return yield* settleNode({
                node,
                state: {
                  kind: "error",
                  error: `not_an_array: forEach.items of node "${node.id}" is not an array`,
                },
                startedAt: null,
              });
            }
            if (resolved.length > node.forEach.maxItems) {
              return yield* settleNode({
                node,
                state: {
                  kind: "error",
                  error: `too_many_items: ${resolved.length} > ${node.forEach.maxItems} for node "${node.id}"`,
                },
                startedAt: null,
              });
            }
            fanOutItems = resolved;
            weight = resolved.length;
          }
          const key = nodeKey(node.id);
          const subDagSlots =
            node.kind !== "dag"
              ? 0
              : node.forEach !== undefined
                ? (fanOutItems?.length ?? 0)
                : node.repeat !== undefined
                  ? node.repeat.maxIterations
                  : 1;
          const proposedStart = abort.signal.aborted;
          const proposedGrantPerRun =
            !proposedStart && subDagSlots > 0
              ? Math.floor(Math.floor(childRunsAvailable / 2) / subDagSlots)
              : 0;
          const proposedGrantTotal = proposedGrantPerRun * subDagSlots;
          childRunsAvailable -= proposedGrantTotal;
          const start = yield* ctx.activity(
            `${key}-start`,
            async () => ({
              start: !proposedStart,
              budget:
                !proposedStart && node.kind !== "transform" && weight > 0
                  ? shareFor(node, weight)
                  : null,
              startedAt: clock.currentTimeMs(),
              childGrant: {
                perRun: proposedGrantPerRun,
                total: proposedGrantTotal,
                slots: subDagSlots,
              },
            }),
            { idempotent: true },
          );
          childRunsAvailable += proposedGrantTotal - start.childGrant.total;
          if (!start.start) {
            return yield* settleNode({
              node,
              state: { kind: "not-started" },
              startedAt: null,
            });
          }
          if (node.kind === "dag") {
            subDagReservations.set(node.id, {
              ...start.childGrant,
              claimed: new Set(),
            });
          }
          const share = start.budget ?? undefined;
          const empty =
            weight > 0 && share && !(node.kind !== "transform" && node.repeat !== undefined)
              ? emptyDimension(share)
              : undefined;
          if (empty) {
            releaseUnusedSubDagGrants(node.id);
            return yield* settleNode({
              node,
              state: {
                kind: "error",
                error: `budget_exceeded: no ${empty} left for node "${node.id}"`,
              },
              startedAt: start.startedAt,
            });
          }
          if (share) held.set(node.id, share);
          let outcome: AttemptOutcome;
          let stats: SettlementStats | undefined;
          if (node.kind === "agent") {
            const attempt = function* (
              attemptEnv: NodeRunEnv,
              attemptParams: Parameters<typeof runAgentAttempt>[1],
            ): Generator<ActivityYield, AttemptOutcome, any> {
              return yield* runAgentAttempt(attemptEnv, {
                ...attemptParams,
                claimChildRun: function* () {
                  return yield* claimChildRun(attemptParams.key);
                },
              });
            };
            if (node.forEach !== undefined && fanOutItems !== undefined) {
              const fanOut = yield* runFanOut(env, {
                node,
                forEach: node.forEach,
                items: fanOutItems,
                nodeInput,
                ...(share && { share }),
                attempt,
              });
              outcome = fanOut;
              fanOutErrors.set(node.id, fanOut.itemErrors);
              stats = {
                items: fanOutItems.length,
                failedItems: Object.keys(fanOut.itemErrors).length,
              };
            } else if (node.repeat !== undefined) {
              const repeated = yield* runRepeat(env, {
                node,
                repeat: node.repeat,
                nodeInput,
                ...(share && { share }),
                signal: abort.signal,
                attempt,
              });
              outcome = repeated;
              stats = { iterations: repeated.iterations };
            } else {
              outcome = yield* attempt(env, {
                node,
                nodeInput,
                key,
                ...(share && { budget: share }),
                signal: abort.signal,
              });
            }
          } else if (node.kind === "dag") {
            const attempt = function* (
              attemptEnv: NodeRunEnv,
              attemptParams: {
                readonly node: SubDagNode;
                readonly nodeInput: Record<string, unknown>;
                readonly key: string;
                readonly budget?: TurnBudget;
                readonly signal: AbortSignal;
              },
            ): Generator<ActivityYield, AttemptOutcome, any> {
              const reservation = subDagReservations.get(node.id)!;
              const claimed = function* (): Generator<ActivityYield, boolean, any> {
                const allowed = yield* claimChildRun(attemptParams.key);
                if (allowed) reservation.claimed.add(attemptParams.key);
                return allowed;
              };
              const result = yield* runSubDagAttempt(attemptEnv, {
                ...attemptParams,
                claimChildRun: claimed,
                childRunsAllowed: reservation.perRun,
              });
              if (reservation.claimed.has(attemptParams.key)) {
                const nested = result.childRuns ?? 0;
                childRuns += nested;
                childRunsAvailable += Math.max(0, reservation.perRun - nested);
              }
              return result;
            };
            if (node.forEach !== undefined && fanOutItems !== undefined) {
              const fanOut = yield* runFanOut(env, {
                node,
                forEach: node.forEach,
                items: fanOutItems,
                nodeInput,
                ...(share && { share }),
                attempt,
              });
              outcome = fanOut;
              fanOutErrors.set(node.id, fanOut.itemErrors);
              stats = {
                items: fanOutItems.length,
                failedItems: Object.keys(fanOut.itemErrors).length,
              };
            } else if (node.repeat !== undefined) {
              const repeated = yield* runRepeat(env, {
                node,
                repeat: node.repeat,
                nodeInput,
                ...(share && { share }),
                signal: abort.signal,
                attempt,
              });
              outcome = repeated;
              stats = { iterations: repeated.iterations };
            } else {
              outcome = yield* attempt(env, {
                node,
                nodeInput,
                key,
                ...(share && { budget: share }),
                signal: abort.signal,
              });
            }
            subDagSpend = addUsage(subDagSpend, outcome.spend);
          } else {
            const transformed = yield* ctx.activity(
              `${key}-transform`,
              async (): Promise<
                | { readonly ok: true; readonly output: unknown }
                | { readonly ok: false; readonly error: string }
              > => {
                try {
                  return {
                    ok: true,
                    output: applyTransform({ node, inputs: nodeInput }),
                  };
                } catch (error) {
                  return { ok: false, error: formatToolError(error) };
                }
              },
              { idempotent: true },
            );
            outcome = transformed.ok
              ? { ...transformed, spend: EMPTY_USAGE }
              : { ...transformed, retryable: false, spend: EMPTY_USAGE };
          }
          held.delete(node.id);
          releaseUnusedSubDagGrants(node.id);
          yield* settleNode({
            node,
            state: outcome.ok
              ? { kind: "output", value: outcome.output }
              : { kind: "error", error: outcome.error },
            spend: outcome.spend,
            startedAt: start.startedAt,
            ...(stats && { stats }),
          });
        } catch (err) {
          // Engine control flow (a suspended child): waiting branches end too.
          settles.get(node.id)!.reject(err);
          throw err;
        }
      }

      try {
        yield* ctx.parallel(order.map((id) => nodeBranch(nodeById.get(id)!)));
      } catch (err) {
        if (isEngineControl(err) || !usageRecorder) throw err;
        // What the nodes spent before the run failed still counts. Recorded
        // as a failure, like the run's other failed entries.
        const failedSpend = subtractUsage({ usage: spent, excluded: subDagSpend });
        yield* ctx.activity(
          "record-usage-failed",
          async (): Promise<void> => {
            try {
              await usageRecorder.record({
                workflowId: ctx.workflowId,
                dagId: input.dagId,
                scope: input.scope,
                usage: failedSpend,
              });
            } catch (recordErr) {
              console.error("[dag] usage record of a failed run failed:", recordErr);
            }
            throw err;
          },
          { idempotent: true },
        );
        throw err;
      } finally {
        options.signal?.removeEventListener("abort", abortFromParent);
      }
      if (usageRecorder) {
        const runSpend = subtractUsage({ usage: spent, excluded: subDagSpend });
        yield* ctx.activity(
          "record-usage",
          () =>
            usageRecorder.record({
              workflowId: ctx.workflowId,
              dagId: input.dagId,
              scope: input.scope,
              usage: runSpend,
            }),
          { idempotent: true },
        );
      }

      // Assembled in topological order, so live runs and replays agree.
      const nodeOutputs: Record<string, unknown> = {};
      const nodeErrors: Record<string, string> = {};
      const nodeStats: Record<string, DagNodeStats> = {};
      const skipped: string[] = [];
      let aborted = false;
      for (const id of order) {
        const state = states.get(id);
        const settlement = settlements.get(id)!;
        const node = nodeById.get(id)!;
        for (const [index, error] of Object.entries(fanOutErrors.get(id) ?? {})) {
          nodeErrors[`${id}.item-${index}`] = error;
        }
        nodeStats[id] = {
          spend: settlement.spend,
          durationMs:
            settlement.startedAt === null
              ? 0
              : Math.max(0, settlement.endedAt - settlement.startedAt),
          ...settlement.stats,
        };
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
        nodeStats,
        spend: spent,
        childRuns,
      };
    })
    .build();
}
