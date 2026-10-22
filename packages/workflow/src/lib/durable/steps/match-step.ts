// ---------------------------------------------------------------------------
// `.match()` — multi-way branching, by selector key or first matching
// predicate. One DAG node; the chosen case is recorded on the step row.
// ---------------------------------------------------------------------------

import { TaggedError as PerfectTaggedError, fail } from "@spilne/perfect-core";
import type { Codec } from "@spilne/perfect-core/connect";
import type { TaggedError } from "../../shared/tagged-error.ts";
import { withOptionalStepCache } from "../step-cache.ts";
import {
  asStepEff,
  linearStepContext,
  type StepContext,
  type StepDefinition,
  type StepEff,
  type StepOptions,
} from "../step-definition.ts";
import { toStepPolicy } from "../step-policy.ts";

/**
 * Typed failure of a `.match()` step when no case applies and no `default`
 * was provided (selector keys are looked up as own properties of `cases`
 * only, so e.g. `"toString"` never hits `Object.prototype`). Carries the
 * step name, mode, and (for selector mode) the resolved key so debugging
 * prod failures doesn't require re-running the workflow.
 */
export class MatchError extends PerfectTaggedError("MatchError")<{
  readonly stepName: string;
  readonly mode: "selector" | "predicate";
  readonly selectorKey?: string;
  readonly message: string;
}>() {}

type MatchCaseFn<Input, Current, Output, E extends TaggedError> = (
  ctx: StepContext<Input, Current>,
) => StepEff<Output, E>;

/**
 * Two-mode params for `.match()`:
 * - **Selector**: `on` returns a key; `cases` is a record keyed by that string.
 * - **Predicate**: `cases` is an array of `{when, then}`; first match wins.
 *
 * `default` is optional in both modes; no match fails the step with `MatchError`.
 */
export type MatchParams<Input, Current, Output, E extends TaggedError> =
  | {
      readonly on: (value: Current) => string;
      readonly cases: Record<string, MatchCaseFn<Input, Current, Output, E>>;
      readonly default?: MatchCaseFn<Input, Current, Output, E>;
    }
  | {
      readonly cases: ReadonlyArray<{
        /**
         * Optional human-readable label for this case. Surfaced in DAG
         * visualization (Mermaid/DOT edge labels). Without a label, the case
         * shows up as `case[N]` in diagrams — useful for debugging but easy
         * to lose track of after refactors. Strongly recommended.
         */
        readonly label?: string;
        readonly when: (value: Current) => boolean;
        readonly then: MatchCaseFn<Input, Current, Output, E>;
      }>;
      readonly default?: MatchCaseFn<Input, Current, Output, E>;
    };

/**
 * Build viz metadata (case labels) from MatchParams for the DAG. The input
 * type is loose because this only reads the shape (`cases` keys/labels) — the
 * actual generic parameters of MatchParams are irrelevant to visualization.
 */
function matchVizMeta(params: {
  on?: unknown;
  cases: Record<string, unknown> | ReadonlyArray<{ label?: string }>;
  default?: unknown;
}): { cases: readonly string[]; hasDefault: boolean } {
  if (typeof params.on === "function") {
    return {
      cases: Object.keys(params.cases as Record<string, unknown>),
      hasDefault: params.default !== undefined,
    };
  }
  const arrayCases = params.cases as ReadonlyArray<{ label?: string }>;
  return {
    cases: arrayCases.map((c, i) => c.label ?? `case[${i}]`),
    hasDefault: params.default !== undefined,
  };
}

/** Tagged result of a `.match()` selection — carries both the branch fn and the
 *  audit label the runner persists on the step row. `label` is:
 *    - selector mode: the selector key or `"default"` when the default ran.
 *    - predicate mode: the matched case's `.label`, falling back to
 *      `case[N]` (matching the DAG viz fallback), or `"default"`. */
interface PickedMatchBranch<Input, Current, Output, E extends TaggedError> {
  readonly fn: MatchCaseFn<Input, Current, Output, E>;
  readonly mode: "selector" | "predicate";
  readonly label: string;
}

/**
 * Resolve which case fires for `prev`. Returns a `MatchError` (not thrown)
 * when nothing applies and there is no default, so the caller can surface
 * it as a typed failure. Exceptions thrown by the user's `on`/`when`
 * callbacks propagate as-is.
 */
function pickMatchBranch<Input, Current, Output, E extends TaggedError>(params: {
  readonly match: MatchParams<Input, Current, Output, E>;
  readonly prev: Current;
  readonly stepName: string;
}): PickedMatchBranch<Input, Current, Output, E> | MatchError {
  const { match, prev, stepName } = params;
  // Selector mode (`on` is a function, `cases` is a record).
  if ("on" in match && typeof match.on === "function") {
    const key = match.on(prev);
    // Own keys only: a selector value such as "constructor" or "toString"
    // must not resolve to an inherited `Object.prototype` member.
    const cases = match.cases as Record<string, MatchCaseFn<Input, Current, Output, E>>;
    const hit = Object.hasOwn(cases, key) ? cases[key] : undefined;
    if (hit) return { fn: hit, mode: "selector", label: key };
    if (match.default) return { fn: match.default, mode: "selector", label: "default" };
    return new MatchError({
      stepName,
      mode: "selector",
      selectorKey: key,
      message: `match step "${stepName}" — no case for selector key "${key}" and no default`,
    });
  }

  // Predicate mode (`cases` is an array).
  const cases = match.cases as ReadonlyArray<{
    when: (v: Current) => boolean;
    then: MatchCaseFn<Input, Current, Output, E>;
    label?: string;
  }>;
  for (let i = 0; i < cases.length; i++) {
    const c = cases[i]!;
    if (c.when(prev)) {
      return { fn: c.then, mode: "predicate", label: c.label ?? `case[${i}]` };
    }
  }
  if (match.default) return { fn: match.default, mode: "predicate", label: "default" };
  return new MatchError({
    stepName,
    mode: "predicate",
    message: `match step "${stepName}" — no predicate matched and no default`,
  });
}

export function createMatchStep(params: {
  readonly name: string;
  readonly dependsOn: string[];
  readonly match: MatchParams<unknown, unknown, unknown, TaggedError>;
  readonly options: StepOptions<unknown> | undefined;
  readonly codec: Codec<unknown>;
  /** Default step-cache namespace (the workflow name). */
  readonly cacheNamespace: string;
}): StepDefinition {
  const { name, dependsOn, match, options, codec } = params;
  return {
    name,
    dependsOn,
    kind: "match",
    codec,
    ...toStepPolicy(options),
    execute: (exec) => {
      const ctx = linearStepContext({ dependsOn, exec });
      const runBody = (): StepEff<unknown, TaggedError> => {
        const picked = pickMatchBranch({ match, prev: ctx.prev, stepName: name });
        // No case and no default: a typed failure, so step retry /
        // onFailure policies and `runSafe` callers see it like any other
        // step error.
        if (picked instanceof MatchError) return fail(picked);
        // Record the chosen case BEFORE running it — even if the branch
        // throws, the metadata is still there to debug "which case fired".
        exec.metadataRef.current = {
          matchCase: picked.label,
          matchMode: picked.mode,
        };
        return asStepEff({ result: picked.fn(ctx), stepName: name });
      };
      return withOptionalStepCache({
        cache: options?.cache,
        ctx,
        runBody,
        stepName: name,
        namespace: params.cacheNamespace,
        codec,
      });
    },
    viz: matchVizMeta(match),
  };
}
