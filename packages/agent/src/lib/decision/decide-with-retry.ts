import { type TimerHandle, type WallClock } from "@promin/workflow";

import { classifyLLMError } from "../adapters/llm-error-classification.ts";
import type { DecisionProvider, DecisionRequest, DecisionResult } from "./types.ts";

function sleep(params: {
  readonly ms: number;
  readonly clock: WallClock;
  readonly signal?: AbortSignal;
}): Promise<void> {
  return new Promise((resolve, reject) => {
    if (params.signal?.aborted) {
      reject(params.signal.reason ?? new Error("aborted"));
      return;
    }
    let timer: TimerHandle;
    const onAbort = () => {
      timer.clear();
      reject(params.signal?.reason ?? new Error("aborted"));
    };
    timer = params.clock.setTimeout(() => {
      params.signal?.removeEventListener("abort", onAbort);
      resolve();
    }, params.ms);
    params.signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Calls `provider.decide`, trying again on transient and rate-limit errors. */
export async function decideWithRetry(params: {
  readonly provider: DecisionProvider;
  readonly request: DecisionRequest;
  readonly clock: WallClock;
  readonly attempts?: number;
  readonly baseDelayMs?: number;
}): Promise<DecisionResult> {
  const attempts = params.attempts ?? 3;
  const baseDelayMs = params.baseDelayMs ?? 250;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await params.provider.decide(params.request);
    } catch (error) {
      if (params.request.signal?.aborted) {
        throw params.request.signal.reason ?? error;
      }
      const classification = classifyLLMError(error);
      if (
        attempt === attempts ||
        (classification !== "transient" && classification !== "rate_limit")
      ) {
        throw error;
      }
      await sleep({
        ms: baseDelayMs * 2 ** (attempt - 1),
        clock: params.clock,
        signal: params.request.signal,
      });
    }
  }
  throw new Error("Decision retry loop ended without a result");
}
