// ---------------------------------------------------------------------------
// runEval / runMatrix — orchestration.
//
// runEval:  dataset x target x scorers, with bounded concurrency over
//           (case x sample) work units. `samplesPerCase > 1` yields a
//           per-case pass-rate — the honest unit, since LLM output is
//           nondeterministic.
//
// runMatrix: the same suite against several targets, with each non-first
//            target diffed against the first — the one-call answer to
//            "is recipe v3 better than v2".
// ---------------------------------------------------------------------------

import { SystemClock, type Clock } from "@promin/core";
import { diffRuns } from "./diff.ts";
import type {
  EvalCase,
  EvalCaseResult,
  EvalDataset,
  EvalOutput,
  EvalReporter,
  EvalRunSummary,
  EvalSample,
  EvalTarget,
  PerScorerSummary,
  RunDiff,
  Score,
  ScoredResult,
  Scorer,
} from "./types.ts";

export interface EvalRunConfig {
  readonly dataset: EvalDataset;
  readonly target: EvalTarget;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  /** Runs per case. Default 1; greater than 1 yields a per-case pass-rate. */
  readonly samplesPerCase?: number;
  /** Max concurrent (case x sample) work units. Default 1. */
  readonly concurrency?: number;
  /** A case passes when its pass-rate is at least this. Default 1. */
  readonly passThreshold?: number;
  readonly signal?: AbortSignal;
  readonly clock?: Clock;
}

/** Run one dataset against one target and score every output. */
export async function runEval(config: EvalRunConfig): Promise<EvalRunSummary> {
  const clock = config.clock ?? SystemClock;
  const samplesPerCase = Math.max(1, Math.trunc(config.samplesPerCase ?? 1));
  const concurrency = Math.max(1, Math.trunc(config.concurrency ?? 1));
  const passThreshold = config.passThreshold ?? 1;
  const reporters = config.reporters ?? [];
  const runOpts = config.signal !== undefined ? { signal: config.signal } : undefined;

  const cases: EvalCase[] = [];
  for await (const evalCase of config.dataset.cases()) cases.push(evalCase);

  // One slot per (case, sample) — filled by the worker pool below.
  const samplesByCase: EvalSample[][] = cases.map(() => []);
  const queue: Array<{ caseIndex: number; sampleIndex: number }> = [];
  for (let caseIndex = 0; caseIndex < cases.length; caseIndex += 1) {
    for (let sampleIndex = 0; sampleIndex < samplesPerCase; sampleIndex += 1) {
      queue.push({ caseIndex, sampleIndex });
    }
  }

  let cursor = 0;
  const worker = async (): Promise<void> => {
    while (cursor < queue.length) {
      if (config.signal?.aborted === true) break;
      const unit = queue[cursor];
      cursor += 1;
      if (unit === undefined) break;
      const evalCase = cases[unit.caseIndex];
      if (evalCase === undefined) break;
      const output = await config.target.run(evalCase, runOpts);
      const { scores, passed } = await scoreOutput(config.scorers, evalCase, output);
      const slots = samplesByCase[unit.caseIndex];
      if (slots !== undefined) {
        slots[unit.sampleIndex] = { sampleIndex: unit.sampleIndex, output, scores, passed };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, () => worker()));

  const caseResults: EvalCaseResult[] = cases.map((evalCase, caseIndex) => {
    const samples = (samplesByCase[caseIndex] ?? []).filter(
      (sample): sample is EvalSample => sample !== undefined,
    );
    const passedSamples = samples.filter((sample) => sample.passed).length;
    const passRate = samples.length === 0 ? 0 : passedSamples / samples.length;
    return { caseId: evalCase.id, samples, passRate, passed: passRate >= passThreshold };
  });

  const passedCases = caseResults.filter((c) => c.passed).length;
  const summary: EvalRunSummary = {
    targetId: config.target.id,
    ...(config.target.version !== undefined && { targetVersion: config.target.version }),
    datasetId: config.dataset.id,
    ranAt: clock.currentTimeMs(),
    totalCases: cases.length,
    passRate: cases.length === 0 ? 0 : passedCases / cases.length,
    perScorer: aggregateScorers(caseResults),
    samplesPerCase,
    caseResults,
  };

  for (const result of caseResults) {
    for (const reporter of reporters) await reporter.onCaseResult?.(result);
  }
  for (const reporter of reporters) await reporter.onComplete?.(summary);

  return summary;
}

export interface EvalMatrixConfig {
  readonly dataset: EvalDataset;
  /** Targets to compare. The first is the baseline for every diff. */
  readonly targets: ReadonlyArray<EvalTarget>;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  readonly samplesPerCase?: number;
  readonly concurrency?: number;
  readonly passThreshold?: number;
  readonly signal?: AbortSignal;
  readonly clock?: Clock;
}

export interface EvalMatrixResult {
  /** One summary per target, in `config.targets` order. */
  readonly summaries: ReadonlyArray<EvalRunSummary>;
  /** `targets[0]` vs each later target — empty when fewer than 2 targets. */
  readonly diffs: ReadonlyArray<RunDiff>;
}

/** Run one dataset against several targets and diff each against the first. */
export async function runMatrix(config: EvalMatrixConfig): Promise<EvalMatrixResult> {
  const summaries: EvalRunSummary[] = [];
  for (const target of config.targets) {
    summaries.push(
      await runEval({
        dataset: config.dataset,
        target,
        scorers: config.scorers,
        ...(config.reporters !== undefined && { reporters: config.reporters }),
        ...(config.samplesPerCase !== undefined && { samplesPerCase: config.samplesPerCase }),
        ...(config.concurrency !== undefined && { concurrency: config.concurrency }),
        ...(config.passThreshold !== undefined && { passThreshold: config.passThreshold }),
        ...(config.signal !== undefined && { signal: config.signal }),
        ...(config.clock !== undefined && { clock: config.clock }),
      }),
    );
  }
  const baseline = summaries[0];
  const diffs = baseline === undefined ? [] : summaries.slice(1).map((s) => diffRuns(baseline, s));
  return { summaries, diffs };
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/** Score one output with every scorer; derive each verdict from its threshold. */
async function scoreOutput(
  scorers: ReadonlyArray<Scorer>,
  evalCase: EvalCase,
  output: EvalOutput,
): Promise<{ scores: ScoredResult[]; passed: boolean }> {
  const scores: ScoredResult[] = [];
  for (const scorer of scorers) {
    const threshold = scorer.threshold ?? 1;
    const required = scorer.required ?? true;

    // A failed target run scores 0 everywhere — never call the scorer.
    if (output.error !== undefined) {
      scores.push({
        scorerId: scorer.id,
        value: 0,
        passed: false,
        threshold,
        required,
        reason: `target failed: ${output.error}`,
      });
      continue;
    }

    let score: Score;
    try {
      score = await scorer.score({
        input: evalCase.input,
        output,
        ...(evalCase.expected !== undefined && { expected: evalCase.expected }),
        ...(evalCase.metadata !== undefined && { metadata: evalCase.metadata }),
      });
    } catch (err) {
      // A buggy scorer fails its own score — it never aborts the run.
      scores.push({
        scorerId: scorer.id,
        value: 0,
        passed: false,
        threshold,
        required,
        reason: `scorer threw: ${err instanceof Error ? err.message : String(err)}`,
      });
      continue;
    }
    scores.push({ ...score, passed: score.value >= threshold, threshold, required });
  }
  const passed = scores.filter((s) => s.required).every((s) => s.passed);
  return { scores, passed };
}

/** Roll every sample's scores up into per-scorer mean value + pass-rate. */
function aggregateScorers(
  caseResults: ReadonlyArray<EvalCaseResult>,
): Record<string, PerScorerSummary> {
  const acc = new Map<string, { sum: number; count: number; passed: number }>();
  for (const caseResult of caseResults) {
    for (const sample of caseResult.samples) {
      for (const score of sample.scores) {
        let row = acc.get(score.scorerId);
        if (row === undefined) {
          row = { sum: 0, count: 0, passed: 0 };
          acc.set(score.scorerId, row);
        }
        row.sum += score.value;
        row.count += 1;
        if (score.passed) row.passed += 1;
      }
    }
  }
  const perScorer: Record<string, PerScorerSummary> = {};
  for (const [scorerId, row] of acc) {
    perScorer[scorerId] = {
      meanValue: row.count === 0 ? 0 : row.sum / row.count,
      passRate: row.count === 0 ? 0 : row.passed / row.count,
    };
  }
  return perScorer;
}
