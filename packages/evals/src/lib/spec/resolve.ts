// ---------------------------------------------------------------------------
// resolveEvalSpec / runEvalSpec — turn a JSON EvalSpec into a live run.
//
// The dataset and scorer seams resolve here (their config is self-contained
// JSON). The target seam delegates to a caller-supplied `resolveTarget`,
// because a recipe target needs an `AgentRegistry` + runtime deps — host
// concerns @promin/evals deliberately does not import.
// ---------------------------------------------------------------------------

import type { LLMProvider } from "@promin/agent";
import { jsonlDataset } from "../datasets/jsonl.ts";
import { inlineDataset } from "../datasets/inline.ts";
import { runMatrix, type EvalMatrixResult } from "../runner.ts";
import { budget } from "../scorers/budget.ts";
import { exactMatch } from "../scorers/exact-match.ts";
import { jsonMatch } from "../scorers/json-match.ts";
import { llmJudge } from "../scorers/llm-judge.ts";
import { regexMatch } from "../scorers/regex-match.ts";
import { toolTrajectory } from "../scorers/tool-trajectory.ts";
import { storedDataset } from "../storage/stored-dataset.ts";
import type { EvalDatasetStore } from "../storage/types.ts";
import type { EvalDataset, EvalTarget, Scorer } from "../types.ts";
import type { DatasetSpec, EvalSpec, ScorerSpec, TargetSpec } from "./types.ts";

/** Runtime injectables the seams that can't self-resolve need. */
export interface ResolveEvalSpecDeps {
  /** Materialize a target spec — a recipe target needs the host's AgentRegistry. */
  readonly resolveTarget: (spec: TargetSpec) => EvalTarget | Promise<EvalTarget>;
  /** Required only when the spec uses a `stored` dataset. */
  readonly datasetStore?: EvalDatasetStore;
  /** Judge LLMs by ref — required only for `llmJudge` scorer specs. */
  readonly judges?: Readonly<Record<string, LLMProvider>>;
}

/** A spec resolved into the live inputs `runMatrix` consumes. */
export interface ResolvedEvalSpec {
  readonly dataset: EvalDataset;
  readonly targets: ReadonlyArray<EvalTarget>;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
}

/** Materialize every seam of an `EvalSpec`. */
export async function resolveEvalSpec(
  spec: EvalSpec,
  deps: ResolveEvalSpecDeps,
): Promise<ResolvedEvalSpec> {
  const dataset = resolveDataset(spec.dataset, deps);
  const targets = await Promise.all(spec.targets.map((t) => deps.resolveTarget(t)));
  const scorers = spec.scorers.map((s) => resolveScorer(s, deps));
  return {
    dataset,
    targets,
    scorers,
    ...(spec.samplesPerCase !== undefined && { samplesPerCase: spec.samplesPerCase }),
    ...(spec.concurrency !== undefined && { concurrency: spec.concurrency }),
    ...(spec.passThreshold !== undefined && { passThreshold: spec.passThreshold }),
  };
}

/** Resolve and run an `EvalSpec` as a matrix (one summary + diffs per target). */
export async function runEvalSpec(
  spec: EvalSpec,
  deps: ResolveEvalSpecDeps,
): Promise<EvalMatrixResult> {
  const resolved = await resolveEvalSpec(spec, deps);
  return runMatrix({
    dataset: resolved.dataset,
    targets: resolved.targets,
    scorers: resolved.scorers,
    ...(resolved.samplesPerCase !== undefined && { samplesPerCase: resolved.samplesPerCase }),
    ...(resolved.concurrency !== undefined && { concurrency: resolved.concurrency }),
    ...(resolved.passThreshold !== undefined && { passThreshold: resolved.passThreshold }),
  });
}

function resolveDataset(spec: DatasetSpec, deps: ResolveEvalSpecDeps): EvalDataset {
  switch (spec.kind) {
    case "inline":
      return inlineDataset(spec.cases, spec.id);
    case "jsonl":
      return jsonlDataset(spec.path, spec.id);
    case "stored":
      if (deps.datasetStore === undefined) {
        throw new Error(
          `resolveEvalSpec: dataset "${spec.datasetId}" is stored, but no datasetStore was supplied`,
        );
      }
      return storedDataset(deps.datasetStore, spec.datasetId);
  }
}

function resolveScorer(spec: ScorerSpec, deps: ResolveEvalSpecDeps): Scorer {
  switch (spec.kind) {
    case "exactMatch":
      return withOverrides(exactMatch, spec);
    case "regexMatch":
      return withOverrides(
        regexMatch({
          pattern: spec.pattern,
          ...(spec.flags !== undefined && { flags: spec.flags }),
          ...(spec.id !== undefined && { id: spec.id }),
        }),
        spec,
      );
    case "jsonMatch":
      return withOverrides(
        jsonMatch({
          ...(spec.exact !== undefined && { exact: spec.exact }),
          ...(spec.id !== undefined && { id: spec.id }),
        }),
        spec,
      );
    case "toolTrajectory":
      return toolTrajectory({
        expected: spec.expected,
        ...(spec.mode !== undefined && { mode: spec.mode }),
        ...(spec.id !== undefined && { id: spec.id }),
        ...(spec.threshold !== undefined && { threshold: spec.threshold }),
        ...(spec.required !== undefined && { required: spec.required }),
      });
    case "budget":
      return budget({
        ...(spec.maxLatencyMs !== undefined && { maxLatencyMs: spec.maxLatencyMs }),
        ...(spec.maxCostUsd !== undefined && { maxCostUsd: spec.maxCostUsd }),
        ...(spec.maxTotalTokens !== undefined && { maxTotalTokens: spec.maxTotalTokens }),
        ...(spec.id !== undefined && { id: spec.id }),
        ...(spec.threshold !== undefined && { threshold: spec.threshold }),
        ...(spec.required !== undefined && { required: spec.required }),
      });
    case "llmJudge": {
      const judge = deps.judges?.[spec.judgeRef];
      if (judge === undefined) {
        throw new Error(
          `resolveEvalSpec: llmJudge references judge "${spec.judgeRef}", absent from deps.judges`,
        );
      }
      return llmJudge({
        judge,
        rubric: spec.rubric,
        ...(spec.id !== undefined && { id: spec.id }),
        ...(spec.threshold !== undefined && { threshold: spec.threshold }),
        ...(spec.required !== undefined && { required: spec.required }),
      });
    }
  }
}

/**
 * Apply a spec's `threshold` / `required` to an already-built scorer. Used
 * for the deterministic scorers, whose factories don't take those knobs.
 */
function withOverrides(
  scorer: Scorer,
  spec: { readonly threshold?: number; readonly required?: boolean },
): Scorer {
  if (spec.threshold === undefined && spec.required === undefined) return scorer;
  return {
    ...scorer,
    ...(spec.threshold !== undefined && { threshold: spec.threshold }),
    ...(spec.required !== undefined && { required: spec.required }),
  };
}
