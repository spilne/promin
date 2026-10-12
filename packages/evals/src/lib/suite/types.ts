// ---------------------------------------------------------------------------
// EvalSuite — a serializable, versioned eval definition.
//
// Where a `.ts` file is the developer's eval, an `EvalSuite` is the
// operator-authored, dashboard-stored one: pure JSON, kept in a
// `VersionedRegistry` (kind `EVAL_SUITE_KIND`) like agent recipes and DAGs —
// immutable versions, an active pointer, promote and compare-and-set.
//
// A suite says WHAT to measure (dataset + scorers + run knobs); the subject
// it measures (an agent recipe version, a function) is chosen when it runs.
// `resolveEvalSuite` materializes the JSON into the live dataset and
// scorers a run needs.
// ---------------------------------------------------------------------------

import type { Versioned, VersionedRegistry } from "@promin/agent";
import type { TrajectoryMode } from "../scorers/tool-trajectory.ts";
import type { SetMatchMetric } from "../scorers/set-match.ts";
import type { EvalCase } from "../types.ts";

/** JSON form of an `EvalDataset`. */
export type DatasetSpec =
  | { readonly kind: "inline"; readonly id: string; readonly cases: ReadonlyArray<EvalCase> }
  | { readonly kind: "stored"; readonly datasetId: string }
  | { readonly kind: "jsonl"; readonly path: string; readonly id?: string };

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
      /** Judge LLM id — resolved against `ResolveEvalSuiteDeps.judges`. */
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
    })
  | (ScorerSpecBase & {
      readonly kind: "retrievalRecall";
      readonly toolNames?: ReadonlyArray<string>;
    })
  | (ScorerSpecBase & { readonly kind: "setMatch"; readonly metric?: SetMatchMetric });

/** The scorer kinds a suite may name. */
export const SCORER_SPEC_KINDS: ReadonlyArray<ScorerSpec["kind"]> = [
  "exactMatch",
  "regexMatch",
  "jsonMatch",
  "llmJudge",
  "toolTrajectory",
  "budget",
  "retrievalRecall",
  "setMatch",
];

export interface EvalSuiteMetadata {
  readonly tags: ReadonlyArray<string>;
  /** Agents the suite is written for (the dashboard offers it on their pages). */
  readonly agentIds: ReadonlyArray<string>;
}

/** A suite as stored: the body of one version. */
export interface EvalSuite {
  readonly description: string | null;
  readonly dataset: DatasetSpec;
  readonly scorers: ReadonlyArray<ScorerSpec>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  /** Longest one sample's target run may take (ms). */
  readonly caseTimeoutMs?: number;
  /**
   * A host running the suite durably may re-run a case a crash interrupted
   * (its target's side effects may then happen twice). Default `false`: an
   * interrupted case is recorded as failed (`interrupted`), not re-run.
   */
  readonly idempotentCases?: boolean;
  /** When set, the agent under test may only use these tools (by name). */
  readonly allowedTools?: ReadonlyArray<string>;
  readonly metadata: EvalSuiteMetadata;
}

/** What `register` takes: `EvalSuite` with optional metadata. */
export interface EvalSuiteInput {
  readonly description?: string | null;
  readonly dataset: DatasetSpec;
  readonly scorers: ReadonlyArray<ScorerSpec>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly caseTimeoutMs?: number;
  readonly idempotentCases?: boolean;
  readonly allowedTools?: ReadonlyArray<string>;
  readonly metadata?: {
    readonly tags?: ReadonlyArray<string>;
    readonly agentIds?: ReadonlyArray<string>;
  };
}

/** `list` filter: suites with this tag, suites written for this agent. */
export interface EvalSuiteFilter {
  readonly tag?: string;
  readonly agentId?: string;
}

/** One stored suite version. */
export type RegisteredEvalSuite = Versioned<EvalSuite>;

/** A `VersionedRegistry` of eval suites (`EVAL_SUITE_KIND`). */
export type EvalSuiteRegistry = VersionedRegistry<EvalSuite, EvalSuiteFilter, EvalSuiteInput>;
