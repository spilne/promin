import type { ActivityYield } from "@promin/workflow";

import type { AgentNode, SubDagNode } from "../types.ts";
import type { AttemptOutcome, RunAttempt } from "./env.ts";

/** Add a durable, per-attempt deadline to an agent or sub-DAG runner. */
export function withAttempts<N extends AgentNode | SubDagNode>(
  attempt: RunAttempt<N>,
): RunAttempt<N> {
  return function* (env, params): Generator<ActivityYield, AttemptOutcome, any> {
    const timeoutMs = params.node.timeoutMs;
    if (timeoutMs === undefined) return yield* attempt(env, params);

    const timing = yield* env.ctx.activity(
      `${params.key}-begin`,
      async () => ({ startedAt: env.clock.currentTimeMs() }),
      { idempotent: true },
    );
    const controller = new AbortController();
    let timedOut = false;
    const onParentAbort = (): void => controller.abort(params.signal.reason);
    if (params.signal.aborted) onParentAbort();
    else params.signal.addEventListener("abort", onParentAbort, { once: true });

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
      const outcome = yield* attempt(env, {
        ...params,
        signal: controller.signal,
      });
      if (!timedOut) return outcome;
      return {
        ok: false,
        error: `node_timeout: node "${params.node.id}" ran past ${timeoutMs}ms`,
        retryable: true,
        spend: outcome.spend,
        ...(outcome.childRuns !== undefined && {
          childRuns: outcome.childRuns,
        }),
      };
    } finally {
      timer?.clear();
      params.signal.removeEventListener("abort", onParentAbort);
    }
  };
}
