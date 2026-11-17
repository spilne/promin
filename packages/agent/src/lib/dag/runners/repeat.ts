import type { ActivityYield } from "@promin/workflow";

import {
  addUsage,
  EMPTY_USAGE,
  remainingBudget,
  type BudgetUsage,
  type TurnBudget,
} from "../../budget/budget.ts";
import { evaluateCondition } from "../conditions.ts";
import { emptyDimension } from "../node-budget.ts";
import type { DagNode, Repeat } from "../types.ts";
import { nodeKey, type AttemptOutcome, type NodeRunEnv, type RunAttempt } from "./env.ts";

function exhausted(params: {
  readonly nodeId: string;
  readonly repeat: Repeat;
  readonly output: unknown;
  readonly hasOutput: boolean;
  readonly spend: BudgetUsage;
  readonly iterations: number;
  readonly reason: string;
  readonly attempts?: number;
}): AttemptOutcome & { readonly iterations: number } {
  if (params.repeat.onExhausted === "continue" && params.hasOutput) {
    return {
      ok: true,
      output: params.output,
      spend: params.spend,
      iterations: params.iterations,
      ...(params.attempts !== undefined && { attempts: params.attempts }),
    };
  }
  return {
    ok: false,
    error: `repeat_exhausted: node "${params.nodeId}" ${params.reason}`,
    retryable: false,
    spend: params.spend,
    iterations: params.iterations,
    ...(params.attempts !== undefined && { attempts: params.attempts }),
  };
}

/** Run sequential attempts until the condition holds or the repeat is exhausted. */
export function* runRepeat<N extends DagNode>(
  env: NodeRunEnv,
  params: {
    readonly node: N;
    readonly repeat: Repeat;
    readonly nodeInput: Record<string, unknown>;
    readonly share?: TurnBudget;
    readonly signal: AbortSignal;
    readonly attempt: RunAttempt<N>;
  },
): Generator<ActivityYield, AttemptOutcome & { readonly iterations: number }, any> {
  let spend: BudgetUsage = EMPTY_USAGE;
  let previous: unknown;
  let hasPrevious = false;
  let attempts = 0;
  let retried = false;

  for (let iteration = 0; iteration < params.repeat.maxIterations; iteration++) {
    const budget = params.share
      ? remainingBudget({ budget: params.share, usage: spend })
      : undefined;
    const empty = budget ? emptyDimension(budget) : undefined;
    if (empty) {
      return exhausted({
        nodeId: params.node.id,
        repeat: params.repeat,
        output: previous,
        hasOutput: hasPrevious,
        spend,
        iterations: iteration,
        reason: `ran out of ${empty} before iteration ${iteration}`,
        ...(retried && { attempts }),
      });
    }

    const outcome = yield* params.attempt(env, {
      node: params.node,
      nodeInput: {
        ...params.nodeInput,
        iteration,
        ...(iteration > 0 && { previous }),
      },
      key: `${nodeKey(params.node.id)}.iter-${iteration}`,
      ...(budget && { budget }),
      signal: params.signal,
    });
    spend = addUsage(spend, outcome.spend);
    attempts += outcome.attempts ?? 1;
    retried ||= outcome.attempts !== undefined;
    if (!outcome.ok)
      return {
        ...outcome,
        spend,
        iterations: iteration + 1,
        ...(retried && { attempts }),
      };

    previous = outcome.output;
    hasPrevious = true;
    if (
      evaluateCondition({
        condition: params.repeat.until,
        value: outcome.output,
      })
    ) {
      return {
        ok: true,
        output: outcome.output,
        spend,
        iterations: iteration + 1,
        ...(retried && { attempts }),
      };
    }
  }

  return exhausted({
    nodeId: params.node.id,
    repeat: params.repeat,
    output: previous,
    hasOutput: hasPrevious,
    spend,
    iterations: params.repeat.maxIterations,
    reason: `did not satisfy its condition after ${params.repeat.maxIterations} iterations`,
    ...(retried && { attempts }),
  });
}
