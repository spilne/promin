import type { ActivityYield } from "@promin/workflow";

import { formatToolError } from "../../agent-shared.ts";
import { EMPTY_USAGE, llmCallUsage, type TurnBudget } from "../../budget/budget.ts";
import { decideWithRetry } from "../../decision/decide-with-retry.ts";
import type { DecideNode } from "../types.ts";
import { nodeKey, type AttemptOutcome, type NodeRunEnv } from "./env.ts";

/** Run and journal one typed decision call. */
export function* runDecide(
  env: NodeRunEnv,
  params: {
    readonly node: DecideNode;
    readonly nodeInput: Record<string, unknown>;
    readonly share?: TurnBudget;
    readonly signal: AbortSignal;
  },
): Generator<ActivityYield, AttemptOutcome, any> {
  const timeoutMs = params.node.timeoutMs;
  const timing =
    timeoutMs === undefined
      ? undefined
      : yield* env.ctx.activity(
          `${nodeKey(params.node.id)}-begin`,
          async () => ({ startedAt: env.clock.currentTimeMs() }),
          { idempotent: true },
        );
  const controller = new AbortController();
  const onParentAbort = (): void => controller.abort(params.signal.reason);
  if (params.signal.aborted) onParentAbort();
  else params.signal.addEventListener("abort", onParentAbort, { once: true });
  let timer: { clear(): void } | undefined;
  if (timeoutMs !== undefined && timing !== undefined && !controller.signal.aborted) {
    const expire = (): void =>
      controller.abort(new Error(`node "${params.node.id}" timed out after ${timeoutMs}ms`));
    const remaining = timing.startedAt + timeoutMs - env.clock.currentTimeMs();
    if (remaining <= 0) expire();
    else timer = env.clock.setTimeout(expire, remaining);
  }

  try {
    return yield* env.ctx.activity(
      `${nodeKey(params.node.id)}-decide`,
      async (): Promise<AttemptOutcome> => {
        if (env.config.decisions === undefined) {
          return {
            ok: false,
            error: "decision_failed: no decision models are configured",
            retryable: false,
            spend: EMPTY_USAGE,
          };
        }

        let resolved;
        try {
          resolved = await env.config.decisions({
            model: params.node.model,
            scope: env.input.scope,
          });
        } catch (error) {
          return {
            ok: false,
            error: `decision_failed: ${formatToolError(error)}`,
            retryable: false,
            spend: EMPTY_USAGE,
          };
        }

        if (params.share?.maxCostUsd !== undefined && resolved.rates === undefined) {
          return {
            ok: false,
            error: `model_unpriced: decision model "${params.node.model.provider}/${params.node.model.id}" has no pricing`,
            retryable: false,
            spend: EMPTY_USAGE,
          };
        }

        try {
          const result = await decideWithRetry({
            provider: resolved.provider,
            request: {
              state: params.nodeInput,
              questions: params.node.questions,
              signal: controller.signal,
            },
            clock: env.clock,
          });
          const usage = result.usage
            ? {
                inputTokens: result.usage.inputTokens,
                outputTokens: result.usage.outputTokens ?? 0,
              }
            : undefined;
          return {
            ok: true,
            output: result.answers,
            spend: llmCallUsage({ usage, rates: resolved.rates }),
          };
        } catch (error) {
          return {
            ok: false,
            error: `decision_failed: ${formatToolError(error)}`,
            retryable: false,
            spend: llmCallUsage({ usage: undefined, rates: resolved.rates }),
          };
        }
      },
      { idempotent: true },
    );
  } finally {
    timer?.clear();
    params.signal.removeEventListener("abort", onParentAbort);
  }
}
