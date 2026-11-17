import { ChildWorkflowGoneError, type Workflow } from "@promin/workflow";
import type { AgentInput, AgentResult } from "../../agent-action.ts";
import type { Agent, AgentScope } from "../../agent/types.ts";
import { formatToolError } from "../../agent-shared.ts";
import { budgetAsUsage, EMPTY_USAGE, tightenBudget } from "../../budget/budget.ts";
import { childSpend, isChildControlFlow } from "../../delegation/run-delegated-call.ts";
import { agentTurnWorkflow } from "../../delegation/sub-agent-tool.ts";
import { isDelegableAgent } from "../../delegation/types.ts";
import { composeAgentInstanceId } from "../../instance/types.ts";
import type { ApprovalDecision } from "../../tool.ts";
import { stringifyTask } from "../dag-helpers.ts";
import { compileOutputSchema, extractJson, schemaInstruction } from "../output-schema.ts";
import { isAgentRefusal, type AgentNode } from "../types.ts";
import { childRunId, type RunAttempt } from "./env.ts";

/** The tool result a DAG node's model gets for an approval-gated call. */
export const DAG_APPROVAL_REFUSED = "requires approval; not available in DAG runs";

async function refuseApproval(): Promise<ApprovalDecision> {
  return { approved: false, reason: DAG_APPROVAL_REFUSED };
}

async function nodeScope(params: {
  readonly env: Parameters<RunAttempt<AgentNode>>[0];
  readonly agentId: string;
}): Promise<AgentScope> {
  const { env, agentId } = params;
  const runScope = env.input.scope;
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
  const resourceId = env.config.instances
    ? (await env.config.instances.resolveOrCreate(key)).id
    : composeAgentInstanceId(key);
  return { namespaceId: runScope.namespaceId, resourceId, ownerId: runScope.ownerId };
}

type NodeRoute =
  | {
      readonly ok: true;
      readonly scope: AgentScope;
      readonly input: AgentInput;
      readonly kind: "turn" | "invoke";
    }
  | { readonly ok: false; readonly error: string };

/** Route and run one agent-node attempt as a child workflow. */
export const runAgentAttempt: RunAttempt<AgentNode> = function* (env, params) {
  const { node, nodeInput, key } = params;
  const task = stringifyTask(nodeInput);
  const routed: NodeRoute = yield* env.ctx.activity(
    `${key}-route`,
    async (): Promise<NodeRoute> => {
      const scope = await nodeScope({ env, agentId: node.agentId });
      let agent: Agent;
      try {
        agent = (await env.config.resolver({ agentId: node.agentId, scope })).withScope(scope);
      } catch (err) {
        if (isAgentRefusal(err)) return { ok: false, error: err.message };
        throw err;
      }
      if (isDelegableAgent(agent)) {
        const turnInput = (await agent.prepareTurn({ task })).input;
        return {
          ok: true,
          scope,
          input: {
            ...turnInput,
            ...(node.output && { outputJsonSchema: node.output.schema }),
          },
          kind: "turn",
        };
      }
      const invokeTask = node.output
        ? `${JSON.stringify(nodeInput, null, 2)}\n\n${schemaInstruction(node.output.schema)}`
        : task;
      return { ok: true, scope, input: { task: invokeTask }, kind: "invoke" };
    },
    { idempotent: true },
  );
  if (!routed.ok) {
    return { ok: false, error: routed.error, retryable: false, spend: EMPTY_USAGE };
  }
  if (params.claimChildRun && !(yield* params.claimChildRun())) {
    return {
      ok: false,
      error: "too_many_child_runs: root DAG run may start at most 500 child runs",
      retryable: false,
      spend: EMPTY_USAGE,
    };
  }

  let result: AgentResult;
  try {
    result = yield* env.ctx.child<AgentResult>(
      {
        name: key,
        load: async () => {
          let resolved: Agent;
          try {
            resolved = await env.config.resolver({
              agentId: node.agentId,
              scope: routed.scope,
            });
          } catch (err) {
            if (isAgentRefusal(err)) throw new ChildWorkflowGoneError({ message: err.message });
            throw err;
          }
          const agent = resolved.withScope(routed.scope);
          const kind = isDelegableAgent(agent) ? "turn" : "invoke";
          if (kind !== routed.kind) {
            throw new ChildWorkflowGoneError({
              message: `agent_backend_changed: "${node.agentId}" was routed as ${routed.kind}, now resolves as ${kind}`,
            });
          }
          const turn = agentTurnWorkflow({
            agent,
            agentId: node.agentId,
            signal: params.signal,
            onApprovalRequired: refuseApproval,
          });
          return turn as unknown as Workflow<unknown, AgentResult>;
        },
      },
      {
        workflowId: childRunId({ runId: env.ctx.workflowId, key }),
        input: {
          ...routed.input,
          ...(params.budget && {
            budget: tightenBudget(routed.input.budget, params.budget),
          }),
        },
      },
    );
  } catch (err) {
    if (isChildControlFlow(err)) throw err;
    return {
      ok: false,
      error: formatToolError(err),
      retryable: true,
      spend: params.budget ? budgetAsUsage(params.budget) : EMPTY_USAGE,
    };
  }

  const spend = childSpend(result);
  if (node.output !== undefined) {
    if (routed.kind === "turn") {
      if (result.finishReason === "output_invalid") {
        return {
          ok: false,
          error: `output_schema_mismatch: ${result.answer}`,
          retryable: true,
          spend,
        };
      }
      if (result.finishReason !== "stop") {
        return {
          ok: false,
          error: `node "${node.id}" finished with ${result.finishReason}`,
          retryable: true,
          spend,
        };
      }
      if (result.output === undefined) {
        return {
          ok: false,
          error: `output_schema_mismatch: node "${node.id}" returned no structured output`,
          retryable: true,
          spend,
        };
      }
      return { ok: true, output: result.output, spend };
    }

    if (result.finishReason !== "stop") {
      return {
        ok: false,
        error: `node "${node.id}" finished with ${result.finishReason}`,
        retryable: true,
        spend,
      };
    }
    try {
      const value = extractJson(result.answer);
      const parsed = compileOutputSchema(node.output.schema).safeParse(value);
      if (!parsed.success) throw parsed.error;
      return { ok: true, output: parsed.data, spend };
    } catch (error) {
      return {
        ok: false,
        error: `output_schema_mismatch: ${formatToolError(error)}`,
        retryable: true,
        spend,
      };
    }
  }
  if (result.finishReason === "cancelled") {
    return {
      ok: false,
      error: `node "${node.id}" was cancelled`,
      retryable: false,
      spend,
    };
  }
  if (result.finishReason === "budget_exceeded") {
    return {
      ok: false,
      error: `budget_exceeded: node "${node.id}" ran out of budget`,
      retryable: false,
      spend,
    };
  }
  return { ok: true, output: result.answer, spend };
};
