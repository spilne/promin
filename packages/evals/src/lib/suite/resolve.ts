// ---------------------------------------------------------------------------
// resolveEvalSuite / runEvalSuite — turn a JSON EvalSuite into a live run.
//
// The dataset and scorer seams resolve here (their config is self-contained
// JSON plus a dataset store and judge LLMs). The subject under test comes
// from the caller: `runEvalSuite` runs the suite against each target as a
// matrix, diffing every target against the first.
// ---------------------------------------------------------------------------

import type { LLMProvider } from "@promin/agent";
import type { WallClock } from "@promin/workflow";
import { inlineDataset } from "../datasets/inline.ts";
import { jsonlDataset } from "../datasets/jsonl.ts";
import { runMatrix, type EvalMatrixResult } from "../runner.ts";
import { budget } from "../scorers/budget.ts";
import { exactMatch } from "../scorers/exact-match.ts";
import { jsonMatch } from "../scorers/json-match.ts";
import { llmJudge } from "../scorers/llm-judge.ts";
import { regexMatch } from "../scorers/regex-match.ts";
import { retrievalRecall } from "../scorers/retrieval-recall.ts";
import { setMatch } from "../scorers/set-match.ts";
import { toolTrajectory } from "../scorers/tool-trajectory.ts";
import { storedDataset } from "../storage/stored-dataset.ts";
import type { EvalDatasetStore, EvalRunStore } from "../storage/types.ts";
import type { EvalDataset, EvalReporter, EvalTarget, Scorer } from "../types.ts";
import type { DatasetSpec, EvalSuite, RegisteredEvalSuite, ScorerSpec } from "./types.ts";

/** Runtime injectables the seams that can't self-resolve need. */
export interface ResolveEvalSuiteDeps {
  /** Required only when the suite uses a `stored` dataset. */
  readonly datasetStore?: EvalDatasetStore;
  /** Judge LLMs by ref — required only for `llmJudge` scorer specs. */
  readonly judges?: Readonly<Record<string, LLMProvider>>;
  /**
   * Whether `jsonl` datasets (a file path on this host) may be read.
   * Default true; a server running operator-submitted suites turns it off.
   */
  readonly allowJsonl?: boolean;
}

/** A suite resolved into the live inputs `runEval` / `runMatrix` consume. */
export interface ResolvedEvalSuite {
  readonly dataset: EvalDataset;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly caseTimeoutMs?: number;
}

/** Materialize the dataset and scorers of an `EvalSuite`. Throws on an unresolvable seam. */
export function resolveEvalSuite(params: {
  readonly suite: EvalSuite;
  readonly deps?: ResolveEvalSuiteDeps;
}): ResolvedEvalSuite {
  const { suite } = params;
  const deps = params.deps ?? {};
  return {
    dataset: resolveDataset({ spec: suite.dataset, deps }),
    scorers: suite.scorers.map((spec) => resolveScorer({ spec, deps })),
    ...(suite.samplesPerCase !== undefined && { samplesPerCase: suite.samplesPerCase }),
    ...(suite.concurrency !== undefined && { concurrency: suite.concurrency }),
    ...(suite.passThreshold !== undefined && { passThreshold: suite.passThreshold }),
    ...(suite.caseTimeoutMs !== undefined && { caseTimeoutMs: suite.caseTimeoutMs }),
  };
}

/**
 * Resolve a suite and run it against each target as a matrix (one summary
 * per target, each later target diffed against the first). With `persist`,
 * every run is recorded, tagged with the suite's id and version when the
 * suite came from a registry.
 */
export async function runEvalSuite(params: {
  readonly suite: EvalSuite | RegisteredEvalSuite;
  readonly targets: ReadonlyArray<EvalTarget>;
  readonly deps?: ResolveEvalSuiteDeps;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  readonly persist?: {
    readonly store: EvalRunStore;
    readonly metadata?: Readonly<Record<string, unknown>>;
  };
  readonly signal?: AbortSignal;
  readonly clock?: WallClock;
}): Promise<EvalMatrixResult> {
  const resolved = resolveEvalSuite({
    suite: params.suite,
    ...(params.deps !== undefined && { deps: params.deps }),
  });
  const registered = isRegistered(params.suite) ? params.suite : undefined;
  return runMatrix({
    ...resolved,
    targets: params.targets,
    ...(params.reporters !== undefined && { reporters: params.reporters }),
    ...(params.signal !== undefined && { signal: params.signal }),
    ...(params.clock !== undefined && { clock: params.clock }),
    ...(params.persist !== undefined && {
      persist: {
        ...params.persist,
        ...(registered !== undefined && {
          suiteId: registered.id,
          suiteVersion: registered.version,
        }),
      },
    }),
  });
}

function isRegistered(suite: EvalSuite | RegisteredEvalSuite): suite is RegisteredEvalSuite {
  const versioned = suite as Partial<RegisteredEvalSuite>;
  return typeof versioned.id === "string" && typeof versioned.version === "string";
}

function resolveDataset(params: {
  readonly spec: DatasetSpec;
  readonly deps: ResolveEvalSuiteDeps;
}): EvalDataset {
  const { spec, deps } = params;
  switch (spec.kind) {
    case "inline":
      return inlineDataset({ cases: spec.cases, id: spec.id });
    case "jsonl":
      if (deps.allowJsonl === false) {
        throw new Error(`resolveEvalSuite: jsonl datasets ("${spec.path}") are not allowed here`);
      }
      return jsonlDataset({ path: spec.path, ...(spec.id !== undefined && { id: spec.id }) });
    case "stored":
      if (deps.datasetStore === undefined) {
        throw new Error(
          `resolveEvalSuite: dataset "${spec.datasetId}" is stored, but no datasetStore was supplied`,
        );
      }
      return storedDataset({ store: deps.datasetStore, datasetId: spec.datasetId });
  }
}

function resolveScorer(params: {
  readonly spec: ScorerSpec;
  readonly deps: ResolveEvalSuiteDeps;
}): Scorer {
  const { spec, deps } = params;
  const common = {
    ...(spec.id !== undefined && { id: spec.id }),
    ...(spec.threshold !== undefined && { threshold: spec.threshold }),
    ...(spec.required !== undefined && { required: spec.required }),
  };
  switch (spec.kind) {
    case "exactMatch":
      return withOverrides({ scorer: exactMatch, spec });
    case "regexMatch":
      return withOverrides({
        scorer: regexMatch({
          pattern: spec.pattern,
          ...(spec.flags !== undefined && { flags: spec.flags }),
          ...(spec.id !== undefined && { id: spec.id }),
        }),
        spec,
      });
    case "jsonMatch":
      return withOverrides({
        scorer: jsonMatch({
          ...(spec.exact !== undefined && { exact: spec.exact }),
          ...(spec.id !== undefined && { id: spec.id }),
        }),
        spec,
      });
    case "toolTrajectory":
      return toolTrajectory({
        expected: spec.expected,
        ...(spec.mode !== undefined && { mode: spec.mode }),
        ...common,
      });
    case "budget":
      return budget({
        ...(spec.maxLatencyMs !== undefined && { maxLatencyMs: spec.maxLatencyMs }),
        ...(spec.maxCostUsd !== undefined && { maxCostUsd: spec.maxCostUsd }),
        ...(spec.maxTotalTokens !== undefined && { maxTotalTokens: spec.maxTotalTokens }),
        ...common,
      });
    case "retrievalRecall":
      return retrievalRecall({
        ...(spec.toolNames !== undefined && { toolNames: spec.toolNames }),
        ...common,
      });
    case "setMatch":
      return setMatch({ ...(spec.metric !== undefined && { metric: spec.metric }), ...common });
    case "llmJudge": {
      const judge = deps.judges?.[spec.judgeRef];
      if (judge === undefined) {
        throw new Error(
          `resolveEvalSuite: llmJudge references judge "${spec.judgeRef}", absent from deps.judges`,
        );
      }
      return llmJudge({ judge, rubric: spec.rubric, ...common });
    }
  }
}

/**
 * Apply a spec's `id` / `threshold` / `required` to an already-built
 * scorer. Used for the deterministic scorers, whose factories don't take
 * those knobs.
 */
function withOverrides(params: {
  readonly scorer: Scorer;
  readonly spec: { readonly id?: string; readonly threshold?: number; readonly required?: boolean };
}): Scorer {
  const { scorer, spec } = params;
  if (spec.id === undefined && spec.threshold === undefined && spec.required === undefined) {
    return scorer;
  }
  return {
    id: spec.id ?? scorer.id,
    ...(scorer.threshold !== undefined && { threshold: scorer.threshold }),
    ...(scorer.required !== undefined && { required: scorer.required }),
    ...(spec.threshold !== undefined && { threshold: spec.threshold }),
    ...(spec.required !== undefined && { required: spec.required }),
    score: async (input) => ({ ...(await scorer.score(input)), scorerId: spec.id ?? scorer.id }),
  };
}
