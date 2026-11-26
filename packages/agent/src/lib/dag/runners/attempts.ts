import type { ActivityYield } from "@promin/workflow";

import { addUsage, EMPTY_USAGE, remainingBudget, type BudgetUsage } from "../../budget/budget.ts";
import { emptyDimension } from "../node-budget.ts";
import type { AgentNode, SubDagNode } from "../types.ts";
import type { AttemptOutcome, RunAttempt } from "./env.ts";

/** Add a durable, per-attempt deadline to an agent or sub-DAG runner. */
export function withAttempts<N extends AgentNode | SubDagNode>(
  attempt: RunAttempt<N>,
): RunAttempt<N> {
  return function* (env, params): Generator<ActivityYield, AttemptOutcome, any> {
    let spend: BudgetUsage = EMPTY_USAGE;
    let childRuns = 0;
    const maxAttempts = params.node.retry?.maxAttempts ?? 1;
    let lastFailure: Extract<AttemptOutcome, { ok: false }> | undefined;

    for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
      const budget = params.budget
        ? remainingBudget({ budget: params.budget, usage: spend })
        : undefined;
      if (lastFailure !== undefined && budget !== undefined && emptyDimension(budget)) {
        return {
          ...lastFailure,
          spend,
          ...(childRuns > 0 && { childRuns }),
          ...(attemptNumber > 2 && { attempts: attemptNumber - 1 }),
        };
      }
      if (lastFailure !== undefined && params.signal.aborted) {
        return {
          ...lastFailure,
          spend,
          ...(childRuns > 0 && { childRuns }),
          ...(attemptNumber > 2 && { attempts: attemptNumber - 1 }),
        };
      }
      if (attemptNumber > 1 && (params.node.retry?.backoffMs ?? 0) > 0) {
        yield* env.ctx.sleep(params.node.retry!.backoffMs!);
      }

      const key = attemptNumber === 1 ? params.key : `${params.key}.try-${attemptNumber}`;
      const outcome = yield* runTimedAttempt(env, {
        attempt,
        params: {
          ...params,
          key,
          ...(budget !== undefined && { budget }),
        },
      });
      spend = addUsage(spend, outcome.spend);
      childRuns += outcome.childRuns ?? 0;
      const observed = {
        ...outcome,
        spend,
        ...(childRuns > 0 && { childRuns }),
        ...(attemptNumber > 1 && { attempts: attemptNumber }),
      };
      if (observed.ok || !observed.retryable || attemptNumber === maxAttempts) return observed;
      lastFailure = observed;
    }
    return lastFailure!;
  };
}

function* runTimedAttempt<N extends AgentNode | SubDagNode>(
  env: Parameters<RunAttempt<N>>[0],
  input: {
    readonly attempt: RunAttempt<N>;
    readonly params: Parameters<RunAttempt<N>>[1];
  },
): Generator<ActivityYield, AttemptOutcome, any> {
  const timeoutMs = input.params.node.timeoutMs;
  if (timeoutMs === undefined) return yield* input.attempt(env, input.params);

  const timing = yield* env.ctx.activity(
    `${input.params.key}-begin`,
    async () => ({ startedAt: env.clock.currentTimeMs() }),
    { idempotent: true },
  );
  const controller = new AbortController();
  let timedOut = false;
  const onParentAbort = (): void => controller.abort(input.params.signal.reason);
  if (input.params.signal.aborted) onParentAbort();
  else input.params.signal.addEventListener("abort", onParentAbort, { once: true });

  const expire = (): void => {
    timedOut = true;
    controller.abort(new Error(`node timeout after ${timeoutMs}ms`));
  };
  let timer: { clear(): void } | undefined;
  if (!controller.signal.aborted) {
    const remaining = timing.startedAt + timeoutMs - env.clock.currentTimeMs();
    if (remaining <= 0) expire();
    else timer = env.clock.setTimeout(expire, remaining);
  }

  try {
    const outcome = yield* input.attempt(env, {
      ...input.params,
      signal: controller.signal,
    });
    if (!timedOut) return outcome;
    return {
      ok: false,
      error: `node_timeout: node "${input.params.node.id}" ran past ${timeoutMs}ms`,
      retryable: true,
      spend: outcome.spend,
      ...(outcome.childRuns !== undefined && {
        childRuns: outcome.childRuns,
      }),
    };
  } finally {
    timer?.clear();
    input.params.signal.removeEventListener("abort", onParentAbort);
  }
}
