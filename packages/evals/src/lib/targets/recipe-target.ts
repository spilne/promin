// ---------------------------------------------------------------------------
// recipeTarget — run a registered agent recipe as an EvalTarget.
//
// Resolves the recipe into a `LocalAgent` (via `resolveLocalAgent`) per
// case and runs it through `agentTarget`. `id` / `version` come from the
// recipe, so a run is identifiable as "recipe X at version v".
// ---------------------------------------------------------------------------

import { resolveLocalAgent } from "@promin/agent";
import type {
  AgentInput,
  ModelCostRegistry,
  RegisteredAgent,
  ResolveLocalAgentDeps,
} from "@promin/agent";
import type { WallClock } from "@promin/workflow";
import type { EvalCase, EvalTarget } from "../types.ts";
import { agentTarget } from "./agent-target.ts";

export interface RecipeTargetConfig {
  readonly recipe: RegisteredAgent;
  /** Runtime injectables for `resolveLocalAgent` — runner, llm factory, tools. */
  readonly deps: ResolveLocalAgentDeps;
  /** Per-model cost rates — when supplied, runs report `metrics.costUsd`. */
  readonly costRegistry?: ModelCostRegistry;
  /** Map a case into the agent's input. Default: `{ task: <stringified input> }`. */
  readonly toInput?: (evalCase: EvalCase) => AgentInput;
  /** Time source for `latencyMs`. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** Build an `EvalTarget` that runs an operator-authored agent recipe. */
export function recipeTarget(config: RecipeTargetConfig): EvalTarget {
  const { recipe, deps, costRegistry } = config;
  const rates =
    costRegistry !== undefined && recipe.backend.type === "local"
      ? costRegistry.rates(recipe.backend.model.provider, recipe.backend.model.id)
      : undefined;
  return agentTarget({
    id: recipe.id,
    version: recipe.version,
    agent: () => resolveLocalAgent(recipe, deps),
    ...(config.toInput !== undefined && { toInput: config.toInput }),
    ...(rates !== undefined && { rates }),
    ...(config.clock !== undefined && { clock: config.clock }),
  });
}
