import type { ActivityYield } from "@promin/workflow";
import { EMPTY_USAGE } from "../../budget/budget.ts";
import type { ApprovalNode } from "../types.ts";
import type { AttemptOutcome, NodeRunEnv } from "./env.ts";

export const DAG_APPROVAL_TOOL_NAME = "dag-approval";

/** Wait for an operator decision at an approval node. */
export function* runApproval(
  env: NodeRunEnv,
  params: {
    readonly node: ApprovalNode;
    readonly nodeInput: Record<string, unknown>;
  },
): Generator<ActivityYield, AttemptOutcome, any> {
  const { node, nodeInput } = params;
  yield* env.ctx.activity(
    `node-${node.id}-approval-${node.id}-start`,
    async () => ({
      toolName: DAG_APPROVAL_TOOL_NAME,
      toolInput: { prompt: node.prompt ?? null, inputs: nodeInput },
      requestedAt: env.clock.currentTimeMs(),
    }),
    { idempotent: true },
  );

  if (node.timeoutMs === undefined) {
    const decision = yield* env.ctx.approval(node.id);
    yield {
      _tag: "Activity",
      name: `node-${node.id}-approval-settling`,
      promise: Promise.resolve(),
    };
    if (env.runSignal.aborted) {
      return {
        ok: false,
        error: `node "${node.id}" was cancelled`,
        retryable: false,
        spend: EMPTY_USAGE,
      };
    }
    return {
      ok: true,
      output: {
        approved: decision.approved,
        ...(decision.reason !== undefined && { reason: decision.reason }),
        ...(decision.by !== undefined && { decidedBy: decision.by }),
        timedOut: false,
      },
      spend: EMPTY_USAGE,
    };
  }

  const outcome = yield* env.ctx.approval(node.id, { timeout: node.timeoutMs });
  yield {
    _tag: "Activity",
    name: `node-${node.id}-approval-settling`,
    promise: Promise.resolve(),
  };
  if (env.runSignal.aborted) {
    return {
      ok: false,
      error: `node "${node.id}" was cancelled`,
      retryable: false,
      spend: EMPTY_USAGE,
    };
  }
  if (!outcome.ok) {
    return (node.onTimeout ?? "reject") === "reject"
      ? {
          ok: true,
          output: { approved: false, timedOut: true },
          spend: EMPTY_USAGE,
        }
      : {
          ok: false,
          error: `approval_timeout: node "${node.id}" received no decision within ${node.timeoutMs}ms`,
          retryable: false,
          spend: EMPTY_USAGE,
        };
  }

  return {
    ok: true,
    output: {
      approved: outcome.value.approved,
      ...(outcome.value.reason !== undefined && { reason: outcome.value.reason }),
      ...(outcome.value.by !== undefined && { decidedBy: outcome.value.by }),
      timedOut: false,
    },
    spend: EMPTY_USAGE,
  };
}
