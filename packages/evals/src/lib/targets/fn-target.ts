// ---------------------------------------------------------------------------
// fnTarget — wrap a plain `input -> output` function as an EvalTarget.
//
// The simplest target: no agent, no recipe. Useful for scoring a pure
// transform, a prompt template, or a stub while wiring a suite.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { EvalCase, EvalTarget } from "../types.ts";

export interface FnTargetConfig {
  /** Target id — default `"fn"`. */
  readonly id?: string;
  /** Optional version, feeds run identity. */
  readonly version?: string;
  /** Time source for `latencyMs`. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** Build an `EvalTarget` from a function of the case input. */
export function fnTarget(
  fn: (input: unknown) => Promise<unknown> | unknown,
  config: FnTargetConfig = {},
): EvalTarget {
  const clock = config.clock ?? SystemWallClock;
  return {
    id: config.id ?? "fn",
    ...(config.version !== undefined && { version: config.version }),
    async run(evalCase: EvalCase) {
      const startedAt = clock.currentTimeMs();
      try {
        const output = await fn(evalCase.input);
        return { output, metrics: { latencyMs: clock.currentTimeMs() - startedAt } };
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
