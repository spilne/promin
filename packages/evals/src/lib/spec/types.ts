// ---------------------------------------------------------------------------
// EvalSpec — a serializable eval definition.
//
// Where a `.ts` file is the developer's eval, an `EvalSpec` is the
// operator-authored, dashboard-stored one — pure JSON, the same artifact
// model as a `RegisteredAgent` recipe or a `RegisteredDag`. Every seam has a
// JSON-config form here; `resolveEvalSpec` materializes them into the live
// dataset / targets / scorers a run needs.
// ---------------------------------------------------------------------------

import type { EvalCase } from "../types.ts";
import type { TrajectoryMode } from "../scorers/tool-trajectory.ts";

/** JSON form of an `EvalDataset`. */
export type DatasetSpec =
  | { readonly kind: "inline"; readonly id: string; readonly cases: ReadonlyArray<EvalCase> }
  | { readonly kind: "stored"; readonly datasetId: string }
  | { readonly kind: "jsonl"; readonly path: string; readonly id?: string };

/**
 * JSON form of an `EvalTarget`. Only recipe targets are serializable — an
 * `fnTarget` holds a closure and can't live in a spec.
 */
export type TargetSpec = {
  readonly kind: "recipe";
  readonly recipeId: string;
  readonly recipeVersion?: string;
};

/** Fields shared by every scorer spec. */
interface ScorerSpecBase {
  /** Override the scorer id. */
  readonly id?: string;
  /** Pass cutoff on `value`. */
  readonly threshold?: number;
  /** Whether a failing score fails the case. */
  readonly required?: boolean;
}

/** JSON form of a `Scorer` — one variant per scorer kind. */
export type ScorerSpec =
  | (ScorerSpecBase & { readonly kind: "exactMatch" })
  | (ScorerSpecBase & {
      readonly kind: "regexMatch";
      readonly pattern: string;
      readonly flags?: string;
    })
  | (ScorerSpecBase & { readonly kind: "jsonMatch"; readonly exact?: boolean })
  | (ScorerSpecBase & {
      readonly kind: "llmJudge";
      /** Judge LLM id — resolved against `ResolveEvalSpecDeps.judges`. */
      readonly judgeRef: string;
      readonly rubric: string;
    })
  | (ScorerSpecBase & {
      readonly kind: "toolTrajectory";
      readonly expected: ReadonlyArray<string>;
      readonly mode?: TrajectoryMode;
    })
  | (ScorerSpecBase & {
      readonly kind: "budget";
      readonly maxLatencyMs?: number;
      readonly maxCostUsd?: number;
      readonly maxTotalTokens?: number;
    });

/** A stored, version-keyed eval definition. */
export interface EvalSpec {
  readonly id: string;
  readonly version: string;
  readonly description?: string;
  readonly dataset: DatasetSpec;
  /** One or more targets — several runs as a matrix (`is v3 better than v2`). */
  readonly targets: ReadonlyArray<TargetSpec>;
  readonly scorers: ReadonlyArray<ScorerSpec>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/** Input to `EvalSpecRegistry.register` — `EvalSpec` minus the managed fields. */
export interface RegisterEvalSpecInput {
  readonly id: string;
  /** Defaults to `"v1"` when omitted. */
  readonly version?: string;
  readonly description?: string;
  readonly dataset: DatasetSpec;
  readonly targets: ReadonlyArray<TargetSpec>;
  readonly scorers: ReadonlyArray<ScorerSpec>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
}

/**
 * Persistent store of eval specs — version-keyed (one row per `(id,
 * version)`), the same shape as `DagRegistry`. InMemory / Sqlite / Postgres
 * backends share a conformance suite.
 */
export interface EvalSpecRegistry {
  /** Register or replace a spec at `(id, version)`. `createdAt` is preserved. */
  register(input: RegisterEvalSpecInput): Promise<EvalSpec>;
  /** Look up by id. Without `version`, returns the most recently updated one. */
  get(id: string, version?: string): Promise<EvalSpec | null>;
  /** Newest version of every spec id, in ascending `createdAt` order. */
  list(): Promise<EvalSpec[]>;
  /** All versions of one id, in ascending `createdAt` order. */
  versions(id: string): Promise<EvalSpec[]>;
  /** Remove a row, or all versions of `id` when `version` is omitted. */
  unregister(id: string, version?: string): Promise<void>;
}

/** Version assigned when a registration omits one. */
export const DEFAULT_EVAL_SPEC_VERSION = "v1";
