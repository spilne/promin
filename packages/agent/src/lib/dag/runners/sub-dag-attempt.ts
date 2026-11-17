import type { ActivityYield } from "@promin/workflow";

import { formatToolError } from "../../agent-shared.ts";
import { budgetAsUsage, EMPTY_USAGE } from "../../budget/budget.ts";
import { isChildControlFlow } from "../../delegation/run-delegated-call.ts";
import { buildDagWorkflow, MAX_DAG_DEPTH, type DurableDagOutput } from "../durable-executor.ts";
import type { SubDagNode } from "../types.ts";
import { childRunId, type AttemptOutcome, type NodeRunEnv } from "./env.ts";

type SubDagRoute =
  | {
      readonly ok: true;
      readonly depth: number;
      readonly ancestors: ReadonlyArray<string>;
    }
  | { readonly ok: false; readonly error: string };

/** Run one sub-DAG node attempt as a child of the current DAG run. */
export function* runSubDagAttempt(
  env: NodeRunEnv,
  params: {
    readonly node: SubDagNode;
    readonly nodeInput: Record<string, unknown>;
    readonly key: string;
    readonly budget?: import("../../budget/budget.ts").TurnBudget;
    readonly signal: AbortSignal;
    readonly claimChildRun?: () => Generator<ActivityYield, boolean, any>;
    readonly childRunsAllowed?: number;
  },
): Generator<ActivityYield, AttemptOutcome, any> {
  const { node, key } = params;
  const routed = yield* env.ctx.activity(
    `${key}-route`,
    async (): Promise<SubDagRoute> => {
      const depth = (env.input.parent?.depth ?? 0) + 1;
      if (depth > MAX_DAG_DEPTH) {
        return {
          ok: false,
          error: `dag_too_deep: DAG "${node.dagId}" would start at depth ${depth}`,
        };
      }
      const ancestors = [...(env.input.parent?.ancestors ?? []), env.input.dagId];
      if (node.dagId === env.input.dagId || ancestors.includes(node.dagId)) {
        return {
          ok: false,
          error: `dag_cycle: DAG "${node.dagId}" is already in the ancestor chain`,
        };
      }
      const found = await env.config.registry.get({ id: node.dagId, version: node.version });
      if (!found) {
        return {
          ok: false,
          error: `dag_not_found: DAG "${node.dagId}" version ${node.version}`,
        };
      }
      if (!found.enabled) {
        return { ok: false, error: `dag_disabled: DAG "${node.dagId}" is disabled` };
      }
      return { ok: true, depth, ancestors };
    },
    { idempotent: true },
  );
  if (!routed.ok) {
    return { ok: false, error: routed.error, retryable: false, spend: EMPTY_USAGE, childRuns: 0 };
  }
  if (params.claimChildRun && !(yield* params.claimChildRun())) {
    return {
      ok: false,
      error: "too_many_child_runs: root DAG run may start at most 500 child runs",
      retryable: false,
      spend: EMPTY_USAGE,
      childRuns: 0,
    };
  }

  let result: DurableDagOutput;
  try {
    result = yield* env.ctx.child<DurableDagOutput>(
      buildDagWorkflow(env.config, { signal: params.signal }),
      {
        workflowId: childRunId({ runId: env.ctx.workflowId, key }),
        input: {
          dagId: node.dagId,
          version: node.version,
          initialInput: params.nodeInput,
          scope: env.input.scope,
          ...(params.budget && { budget: params.budget }),
          parent: {
            depth: routed.depth,
            ancestors: routed.ancestors,
            childRunsAllowed: params.childRunsAllowed ?? 0,
          },
        },
      },
    );
  } catch (error) {
    if (isChildControlFlow(error)) throw error;
    return {
      ok: false,
      error: formatToolError(error),
      retryable: true,
      spend: params.budget ? budgetAsUsage(params.budget) : EMPTY_USAGE,
      childRuns: 0,
    };
  }

  if (!result.ok) {
    return {
      ok: false,
      error: `sub_dag_failed: ${JSON.stringify(result.errors)}`,
      retryable: false,
      spend: result.spend,
      childRuns: result.childRuns,
    };
  }
  return {
    ok: true,
    output: result.outputs,
    spend: result.spend,
    childRuns: result.childRuns,
  };
}
