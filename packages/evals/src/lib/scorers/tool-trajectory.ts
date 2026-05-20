// ---------------------------------------------------------------------------
// toolTrajectory — score the tool-call sequence of a run.
//
// Consumes `EvalOutput.trace` (the turn tree from `buildAgentTrace`),
// flattens the tool-call names in invocation order, and compares them to an
// expected sequence under one of three modes.
// ---------------------------------------------------------------------------

import type { AgentTrace } from "@promin/agent";
import type { Scorer } from "../types.ts";

export type TrajectoryMode = "exact" | "ordered" | "set";

export interface ToolTrajectoryConfig {
  /** Tool names the run is expected to call. */
  readonly expected: ReadonlyArray<string>;
  /**
   * - `exact`   — the call sequence must equal `expected` exactly.
   * - `ordered` — `expected` must appear as an ordered subsequence (default).
   * - `set`     — every `expected` tool must be called, order ignored.
   */
  readonly mode?: TrajectoryMode;
  /** Scorer id — default `"toolTrajectory"`. */
  readonly id?: string;
  readonly threshold?: number;
  readonly required?: boolean;
}

/** Build a scorer that judges the run's tool-call sequence. */
export function toolTrajectory(config: ToolTrajectoryConfig): Scorer {
  const id = config.id ?? "toolTrajectory";
  const mode = config.mode ?? "ordered";
  return {
    id,
    threshold: config.threshold ?? 1,
    ...(config.required !== undefined && { required: config.required }),
    async score({ output }) {
      if (output.trace === undefined) {
        return { scorerId: id, value: 0, reason: "output carries no trace to inspect" };
      }
      const actual = toolCallNames(output.trace);
      const value = scoreTrajectory(config.expected, actual, mode);
      return {
        scorerId: id,
        value,
        ...(value < 1 && {
          reason: `expected [${config.expected.join(", ")}] (${mode}), got [${actual.join(", ")}]`,
        }),
      };
    },
  };
}

/** Tool-call names in invocation order, flattened across every turn. */
export function toolCallNames(trace: AgentTrace): string[] {
  const names: string[] = [];
  for (const turn of trace.turns) {
    for (const child of turn.children) {
      if (child.kind === "assistant") {
        for (const call of child.toolCalls) names.push(call.name);
      }
    }
  }
  return names;
}

function scoreTrajectory(
  expected: ReadonlyArray<string>,
  actual: ReadonlyArray<string>,
  mode: TrajectoryMode,
): number {
  if (expected.length === 0) return actual.length === 0 ? 1 : 0;

  if (mode === "exact") {
    const equal =
      expected.length === actual.length && expected.every((name, i) => name === actual[i]);
    return equal ? 1 : 0;
  }

  if (mode === "set") {
    const called = new Set(actual);
    const present = expected.filter((name) => called.has(name)).length;
    return present / expected.length;
  }

  // ordered — longest common subsequence of `expected` within `actual`,
  // so an expected step missing from the run still credits the rest.
  return lcsLength(expected, actual) / expected.length;
}

/** Length of the longest common subsequence of two string sequences. */
function lcsLength(a: ReadonlyArray<string>, b: ReadonlyArray<string>): number {
  let prev = new Array<number>(b.length + 1).fill(0);
  for (let i = 1; i <= a.length; i += 1) {
    const curr = new Array<number>(b.length + 1).fill(0);
    for (let j = 1; j <= b.length; j += 1) {
      curr[j] =
        a[i - 1] === b[j - 1] ? (prev[j - 1] ?? 0) + 1 : Math.max(prev[j] ?? 0, curr[j - 1] ?? 0);
    }
    prev = curr;
  }
  return prev[b.length] ?? 0;
}
