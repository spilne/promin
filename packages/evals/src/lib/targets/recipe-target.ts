// ---------------------------------------------------------------------------
// recipeTarget — run a registered agent recipe as an EvalTarget.
//
// Resolves the recipe into a `LocalAgent` (via `resolveLocalAgent`),
// invokes it per case, and maps the run through `toEvalOutput` so the
// trace + token/cost metrics ride along. `id` / `version` come from the
// recipe, so a run is identifiable as "recipe X at version v".
// ---------------------------------------------------------------------------

import { resolveLocalAgent } from "@promin/agent";
import type {
  AgentInput,
  ModelCostRates,
  ModelCostRegistry,
  RegisteredAgent,
  ResolveLocalAgentDeps,
} from "@promin/agent";
import { SystemClock, type Clock } from "@promin/core";
import type { EvalCase, EvalTarget } from "../types.ts";
import { toEvalOutput } from "./to-eval-output.ts";

export interface RecipeTargetConfig {
  readonly recipe: RegisteredAgent;
  /** Runtime injectables for `resolveLocalAgent` — runner, llm factory, tools. */
  readonly deps: ResolveLocalAgentDeps;
  /** Per-model cost rates — when supplied, runs report `metrics.costUsd`. */
  readonly costRegistry?: ModelCostRegistry;
  /** Map a case into the agent's input. Default: `{ task: <stringified input> }`. */
  readonly toInput?: (evalCase: EvalCase) => AgentInput;
  /** Time source for `latencyMs`. Default `SystemClock`. */
  readonly clock?: Clock;
}

/** Build an `EvalTarget` that runs an operator-authored agent recipe. */
export function recipeTarget(config: RecipeTargetConfig): EvalTarget {
  const { recipe, deps, costRegistry } = config;
  const clock = config.clock ?? SystemClock;
  const toInput = config.toInput ?? defaultToInput;
  const rates: ModelCostRates | undefined =
    costRegistry !== undefined && recipe.backend.type === "local"
      ? costRegistry.rates(recipe.backend.model.provider, recipe.backend.model.id)
      : undefined;

  return {
    id: recipe.id,
    version: recipe.version,
    async run(evalCase, opts) {
      const startedAt = clock.currentTimeMs();
      try {
        const agent = resolveLocalAgent(recipe, deps);
        const run = await agent.invoke(
          toInput(evalCase),
          opts?.signal !== undefined ? { signal: opts.signal } : undefined,
        );
        return await toEvalOutput(run, {
          latencyMs: clock.currentTimeMs() - startedAt,
          ...(rates !== undefined && { rates }),
        });
      } catch (err) {
        return {
          output: undefined,
          metrics: { latencyMs: clock.currentTimeMs() - startedAt },
          error: err instanceof Error ? err.message : String(err),
        };
      }
    },
  };
}

function defaultToInput(evalCase: EvalCase): AgentInput {
  const { input } = evalCase;
  return { task: typeof input === "string" ? input : JSON.stringify(input) };
}
