import type { ActivityYield } from "@promin/workflow";

import { addUsage, EMPTY_USAGE, type BudgetUsage } from "../../budget/budget.ts";
import { splitShare } from "../node-budget.ts";
import type { DagNode, ForEach } from "../types.ts";
import { nodeKey, type AttemptOutcome, type NodeRunEnv, type RunAttempt } from "./env.ts";

interface Gate {
  readonly promise: Promise<void>;
  resolve(): void;
}

function gate(): Gate {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function cancelled(nodeId: string, index: number): AttemptOutcome {
  return {
    ok: false,
    error: `node "${nodeId}" item ${index} was cancelled`,
    retryable: false,
    spend: EMPTY_USAGE,
  };
}

/** Run one node attempt for every item, subject to the node's concurrency cap. */
export function* runFanOut<N extends DagNode>(
  env: NodeRunEnv,
  params: {
    readonly node: N;
    readonly forEach: ForEach;
    readonly items: ReadonlyArray<unknown>;
    readonly nodeInput: Record<string, unknown>;
    readonly share?: import("../../budget/budget.ts").TurnBudget;
    readonly attempt: RunAttempt<N>;
  },
): Generator<
  ActivityYield,
  AttemptOutcome & { readonly itemErrors: Readonly<Record<number, string>> },
  any
> {
  if (params.items.length === 0) {
    return { ok: true, output: [], spend: EMPTY_USAGE, itemErrors: {} };
  }

  const controller = new AbortController();
  const abort = (): void => controller.abort();
  if (env.runSignal.aborted) abort();
  else env.runSignal.addEventListener("abort", abort, { once: true });

  const concurrency = Math.min(params.forEach.concurrency ?? 4, params.items.length);
  const gates = params.items.map(() => gate());
  for (let index = 0; index < concurrency; index++) gates[index]!.resolve();
  const queued = gates.slice(concurrency);
  controller.signal.addEventListener(
    "abort",
    () => {
      for (const waiting of gates) waiting.resolve();
    },
    { once: true },
  );

  const itemBudget = params.share
    ? splitShare({ share: params.share, parts: params.items.length })
    : undefined;
  const runItem = (index: number): Generator<ActivityYield, AttemptOutcome, any> =>
    (function* () {
      if (index >= concurrency) {
        yield {
          _tag: "Activity",
          name: `${nodeKey(params.node.id)}-item-${index}-wait`,
          promise: gates[index]!.promise,
        };
      }
      if (controller.signal.aborted) return cancelled(params.node.id, index);
      try {
        const outcome = yield* params.attempt(env, {
          node: params.node,
          nodeInput: {
            ...params.nodeInput,
            [params.forEach.as]: params.items[index],
            index,
          },
          key: `${nodeKey(params.node.id)}.item-${index}`,
          ...(itemBudget && { budget: itemBudget }),
          signal: controller.signal,
        });
        if (!outcome.ok && (params.forEach.onItemError ?? "null") === "fail") abort();
        return outcome;
      } finally {
        if (!controller.signal.aborted) queued.shift()?.resolve();
      }
    })();

  let outcomes: AttemptOutcome[];
  try {
    outcomes = yield* env.ctx.parallel(params.items.map((_, index) => runItem(index)));
  } catch (error) {
    abort();
    throw error;
  } finally {
    env.runSignal.removeEventListener("abort", abort);
  }

  let spend: BudgetUsage = EMPTY_USAGE;
  let attempts = 0;
  let retried = false;
  const itemErrors: Record<number, string> = {};
  const output: unknown[] = [];
  for (const [index, outcome] of outcomes.entries()) {
    spend = addUsage(spend, outcome.spend);
    attempts += outcome.attempts ?? 1;
    retried ||= outcome.attempts !== undefined;
    if (outcome.ok) output[index] = outcome.output;
    else {
      output[index] = null;
      itemErrors[index] = outcome.error;
    }
  }

  const failures = outcomes
    .map((outcome, index) => ({ outcome, index }))
    .filter(
      (entry): entry is { outcome: Extract<AttemptOutcome, { ok: false }>; index: number } =>
        !entry.outcome.ok,
    );
  if (failures.length === 0)
    return { ok: true, output, spend, itemErrors, ...(retried && { attempts }) };

  const substantive = failures.filter(({ outcome }) => !outcome.error.endsWith("was cancelled"));
  const first = (substantive[0] ?? failures[0])!;
  if ((params.forEach.onItemError ?? "null") === "fail") {
    return {
      ok: false,
      error: first.outcome.error,
      retryable: first.outcome.retryable,
      spend,
      itemErrors,
      ...(retried && { attempts }),
    };
  }
  if (failures.length === params.items.length) {
    return {
      ok: false,
      error: `all_items_failed: ${first.outcome.error}`,
      retryable: first.outcome.retryable,
      spend,
      itemErrors,
      ...(retried && { attempts }),
    };
  }
  return { ok: true, output, spend, itemErrors, ...(retried && { attempts }) };
}
