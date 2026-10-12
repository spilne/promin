// ---------------------------------------------------------------------------
// runEval / runMatrix — orchestration.
//
// runEval:  dataset x target x scorers, with bounded concurrency over
//           (case x sample) work units. `samplesPerCase > 1` yields a
//           per-case pass-rate — the honest unit, since LLM output is
//           nondeterministic. Each case is reported (and, with `persist`,
//           recorded in the run store) as soon as its last sample is
//           scored. A sample that outlives `caseTimeoutMs` is scored as a
//           failed run, and an aborted run stops scheduling, records the
//           run as cancelled and rejects with `EvalRunAbortedError`.
//
// runMatrix: the same suite against several targets, with each non-first
//            target diffed against the first — the one-call answer to
//            "is recipe v3 better than v2".
//
// `runEvalCase` and `summarizeCases` are the building blocks: a durable
// host (zorya's eval workflow) journals one `runEvalCase` per case and
// rolls the results up with `summarizeCases`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type TimerHandle, type WallClock } from "@promin/workflow";
import { diffRuns } from "./diff.ts";
import type { EvalRunStore, EvalRunTotals } from "./storage/types.ts";
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

/** Upper bound on `concurrency`: more concurrent target runs than this is refused. */
export const MAX_EVAL_CONCURRENCY = 64;
/** Upper bound on `samplesPerCase`. */
export const MAX_EVAL_SAMPLES_PER_CASE = 100;

/** Where `runEval` records the run as it goes. */
export interface EvalRunPersistence {
  readonly store: EvalRunStore;
  /** Id of the stored run. Default: a fresh `eval-<uuid>`. */
  readonly runId?: string;
  readonly suiteId?: string;
  readonly suiteVersion?: string;
  readonly namespaceId?: string;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Knobs shared by `runEval`, `runMatrix` and `runEvalCase`. */
export interface EvalCaseOptions {
  /** Runs per case. Default 1; greater than 1 yields a per-case pass-rate. */
  readonly samplesPerCase?: number;
  /** A case passes when its pass-rate is at least this, 0..1. Default 1. */
  readonly passThreshold?: number;
  /**
   * Longest one sample's target run may take (ms). A run past it is
   * aborted (its signal fires) and scored as a failed run. Default: none.
   */
  readonly caseTimeoutMs?: number;
  /** Time source for the timeout and `ranAt`. Default `SystemWallClock`. */
  readonly clock?: WallClock;
}

export interface EvalRunConfig extends EvalCaseOptions {
  readonly dataset: EvalDataset;
  readonly target: EvalTarget;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  /** Max concurrent (case x sample) work units, 1..`MAX_EVAL_CONCURRENCY`. Default 1. */
  readonly concurrency?: number;
  /** Aborting stops the run: see `EvalRunAbortedError`. */
  readonly signal?: AbortSignal;
  /** Record the run in a store while it runs. */
  readonly persist?: EvalRunPersistence;
}

/** `runEval` was aborted. Carries the cases that finished before the abort. */
export class EvalRunAbortedError extends Error {
  readonly _tag = "EvalRunAbortedError";
  readonly caseResults: ReadonlyArray<EvalCaseResult>;
  readonly runId?: string;

  constructor(params: {
    readonly caseResults: ReadonlyArray<EvalCaseResult>;
    readonly runId?: string;
    readonly reason: unknown;
  }) {
    super(`eval run aborted: ${errorMessage(params.reason)}`, { cause: params.reason });
    this.name = "EvalRunAbortedError";
    this.caseResults = params.caseResults;
    if (params.runId !== undefined) this.runId = params.runId;
  }
}

/** Validated knobs. Throws `RangeError` for a value out of range. */
export interface ResolvedEvalOptions {
  readonly samplesPerCase: number;
  readonly passThreshold: number;
  readonly concurrency: number;
  readonly caseTimeoutMs: number | undefined;
}

/** Check the run knobs; throws `RangeError` naming the first bad one. */
export function resolveEvalOptions(params: {
  readonly samplesPerCase?: number;
  readonly passThreshold?: number;
  readonly concurrency?: number;
  readonly caseTimeoutMs?: number;
}): ResolvedEvalOptions {
  const samplesPerCase = params.samplesPerCase ?? 1;
  if (!isIntIn({ value: samplesPerCase, min: 1, max: MAX_EVAL_SAMPLES_PER_CASE })) {
    throw new RangeError(
      `samplesPerCase must be an integer in 1..${MAX_EVAL_SAMPLES_PER_CASE}, got ${samplesPerCase}`,
    );
  }
  const concurrency = params.concurrency ?? 1;
  if (!isIntIn({ value: concurrency, min: 1, max: MAX_EVAL_CONCURRENCY })) {
    throw new RangeError(
      `concurrency must be an integer in 1..${MAX_EVAL_CONCURRENCY}, got ${concurrency}`,
    );
  }
  const passThreshold = params.passThreshold ?? 1;
  if (!Number.isFinite(passThreshold) || passThreshold < 0 || passThreshold > 1) {
    throw new RangeError(`passThreshold must be in 0..1, got ${passThreshold}`);
  }
  const caseTimeoutMs = params.caseTimeoutMs;
  if (caseTimeoutMs !== undefined && !(Number.isFinite(caseTimeoutMs) && caseTimeoutMs > 0)) {
    throw new RangeError(`caseTimeoutMs must be a positive number, got ${caseTimeoutMs}`);
  }
  return { samplesPerCase, passThreshold, concurrency, caseTimeoutMs };
}

/** Run one dataset against one target and score every output. */
export async function runEval(config: EvalRunConfig): Promise<EvalRunSummary> {
  const clock = config.clock ?? SystemWallClock;
  const options = resolveEvalOptions(config);
  const { samplesPerCase, concurrency } = options;
  const reporters = config.reporters ?? [];
  const persist = config.persist;
  const runId = persist ? (persist.runId ?? `eval-${crypto.randomUUID()}`) : undefined;
  const ranAt = clock.currentTimeMs();

  const cases: EvalCase[] = [];
  for await (const evalCase of config.dataset.cases()) cases.push(evalCase);

  if (persist && runId !== undefined) {
    await persist.store.start({
      runId,
      targetId: config.target.id,
      ...(config.target.version !== undefined && { targetVersion: config.target.version }),
      datasetId: config.dataset.id,
      ...(persist.suiteId !== undefined && { suiteId: persist.suiteId }),
      ...(persist.suiteVersion !== undefined && { suiteVersion: persist.suiteVersion }),
      ...(persist.namespaceId !== undefined && { namespaceId: persist.namespaceId }),
      samplesPerCase,
      totalCases: cases.length,
      startedAt: ranAt,
      ...(persist.metadata !== undefined && { metadata: persist.metadata }),
    });
  }

  // The run stops on the caller's abort and on a reporter / store failure.
  const stop = new AbortController();
  let failure: { error: unknown } | undefined;
  const onCallerAbort = (): void => stop.abort(config.signal?.reason);
  if (config.signal?.aborted === true) onCallerAbort();
  else config.signal?.addEventListener("abort", onCallerAbort, { once: true });

  const samplesByCase: EvalSample[][] = cases.map(() => []);
  const results: Array<EvalCaseResult | undefined> = cases.map(() => undefined);
  const queue: Array<{ caseIndex: number; sampleIndex: number }> = [];
  for (let caseIndex = 0; caseIndex < cases.length; caseIndex += 1) {
    for (let sampleIndex = 0; sampleIndex < samplesPerCase; sampleIndex += 1) {
      queue.push({ caseIndex, sampleIndex });
    }
  }

  /** Report + record a case whose last sample just finished. */
  const finishCase = async (caseIndex: number): Promise<void> => {
    const evalCase = cases[caseIndex]!;
    const samples = [...samplesByCase[caseIndex]!].sort((a, b) => a.sampleIndex - b.sampleIndex);
    const result = caseResultOf({ caseId: evalCase.id, samples, options });
    results[caseIndex] = result;
    if (persist && runId !== undefined) {
      await persist.store.recordCase({ runId, caseIndex, result });
    }
    for (const reporter of reporters) await reporter.onCaseResult?.(result);
  };

  let cursor = 0;
  const worker = async (): Promise<void> => {
    while (cursor < queue.length && !stop.signal.aborted) {
      const unit = queue[cursor]!;
      cursor += 1;
      const sample = await runSample({
        evalCase: cases[unit.caseIndex]!,
        sampleIndex: unit.sampleIndex,
        target: config.target,
        scorers: config.scorers,
        caseTimeoutMs: options.caseTimeoutMs,
        signal: stop.signal,
        clock,
      });
      // A sample cut short by the abort is not a result.
      if (stop.signal.aborted) return;
      const slots = samplesByCase[unit.caseIndex]!;
      slots.push(sample);
      if (slots.length < samplesPerCase) continue;
      try {
        await finishCase(unit.caseIndex);
      } catch (error) {
        failure ??= { error };
        stop.abort(error);
      }
    }
  };
  try {
    await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, () => worker()));
  } finally {
    config.signal?.removeEventListener("abort", onCallerAbort);
  }

  const finished = results.filter((r): r is EvalCaseResult => r !== undefined);
  if (stop.signal.aborted) {
    if (persist && runId !== undefined) {
      await persist.store.finish({
        runId,
        status: failure ? "failed" : "cancelled",
        finishedAt: clock.currentTimeMs(),
        totals: totalsOf(finished),
        ...(failure && { error: errorMessage(failure.error) }),
      });
    }
    if (failure) throw failure.error;
    throw new EvalRunAbortedError({
      caseResults: finished,
      ...(runId !== undefined && { runId }),
      reason: stop.signal.reason,
    });
  }

  const summary = summarizeCases({
    ...(runId !== undefined && { runId }),
    targetId: config.target.id,
    ...(config.target.version !== undefined && { targetVersion: config.target.version }),
    datasetId: config.dataset.id,
    ranAt,
    samplesPerCase,
    caseResults: finished,
  });
  if (persist && runId !== undefined) {
    await persist.store.finish({
      runId,
      status: "completed",
      finishedAt: clock.currentTimeMs(),
      totals: totalsOf(summary.caseResults),
    });
  }
  for (const reporter of reporters) await reporter.onComplete?.(summary);
  return summary;
}

export interface EvalMatrixConfig extends EvalCaseOptions {
  readonly dataset: EvalDataset;
  /** Targets to compare. The first is the baseline for every diff. */
  readonly targets: ReadonlyArray<EvalTarget>;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly reporters?: ReadonlyArray<EvalReporter>;
  readonly concurrency?: number;
  readonly signal?: AbortSignal;
  /** Record each target's run (a fresh run id per target). */
  readonly persist?: Omit<EvalRunPersistence, "runId">;
}

export interface EvalMatrixResult {
  /** One summary per target, in `config.targets` order. */
  readonly summaries: ReadonlyArray<EvalRunSummary>;
  /** `targets[0]` vs each later target — empty when fewer than 2 targets. */
  readonly diffs: ReadonlyArray<RunDiff>;
}

/** Run one dataset against several targets and diff each against the first. */
export async function runMatrix(config: EvalMatrixConfig): Promise<EvalMatrixResult> {
  const { targets, persist, ...shared } = config;
  const summaries: EvalRunSummary[] = [];
  for (const target of targets) {
    summaries.push(await runEval({ ...shared, target, ...(persist && { persist }) }));
  }
  const baseline = summaries[0];
  const diffs =
    baseline === undefined
      ? []
      : summaries.slice(1).map((candidate) => diffRuns({ baseline, candidate }));
  return { summaries, diffs };
}

/**
 * Run every sample of one case, one after another, and roll them into the
 * case's result. Throws `RangeError` for out-of-range knobs.
 */
export async function runEvalCase(
  params: EvalCaseOptions & {
    readonly evalCase: EvalCase;
    readonly target: EvalTarget;
    readonly scorers: ReadonlyArray<Scorer>;
    readonly signal?: AbortSignal;
  },
): Promise<EvalCaseResult> {
  const options = resolveEvalOptions(params);
  const clock = params.clock ?? SystemWallClock;
  const samples: EvalSample[] = [];
  for (let sampleIndex = 0; sampleIndex < options.samplesPerCase; sampleIndex += 1) {
    samples.push(
      await runSample({
        evalCase: params.evalCase,
        sampleIndex,
        target: params.target,
        scorers: params.scorers,
        caseTimeoutMs: options.caseTimeoutMs,
        ...(params.signal !== undefined && { signal: params.signal }),
        clock,
      }),
    );
  }
  return caseResultOf({ caseId: params.evalCase.id, samples, options });
}

/** Roll case results up into a run summary. */
export function summarizeCases(params: {
  readonly runId?: string;
  readonly targetId: string;
  readonly targetVersion?: string;
  readonly datasetId: string;
  readonly ranAt: number;
  readonly samplesPerCase: number;
  readonly caseResults: ReadonlyArray<EvalCaseResult>;
}): EvalRunSummary {
  const totals = totalsOf(params.caseResults);
  return {
    ...(params.runId !== undefined && { runId: params.runId }),
    targetId: params.targetId,
    ...(params.targetVersion !== undefined && { targetVersion: params.targetVersion }),
    datasetId: params.datasetId,
    ranAt: params.ranAt,
    totalCases: params.caseResults.length,
    passRate: totals.passRate,
    perScorer: totals.perScorer,
    samplesPerCase: params.samplesPerCase,
    caseResults: params.caseResults,
  };
}

/** The totals a run store keeps for these case results. */
export function totalsOf(caseResults: ReadonlyArray<EvalCaseResult>): EvalRunTotals {
  const passed = caseResults.filter((c) => c.passed).length;
  return {
    passRate: caseResults.length === 0 ? 0 : passed / caseResults.length,
    perScorer: aggregateScorers(caseResults),
    scoredCases: caseResults.length,
  };
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function caseResultOf(params: {
  caseId: string;
  samples: EvalSample[];
  options: ResolvedEvalOptions;
}): EvalCaseResult {
  const { caseId, samples, options } = params;
  const passedSamples = samples.filter((sample) => sample.passed).length;
  const passRate = samples.length === 0 ? 0 : passedSamples / samples.length;
  return { caseId, samples, passRate, passed: passRate >= options.passThreshold };
}

/** One target run of a case, bounded by the timeout and the abort, then scored. */
async function runSample(params: {
  readonly evalCase: EvalCase;
  readonly sampleIndex: number;
  readonly target: EvalTarget;
  readonly scorers: ReadonlyArray<Scorer>;
  readonly caseTimeoutMs: number | undefined;
  readonly signal?: AbortSignal;
  readonly clock: WallClock;
}): Promise<EvalSample> {
  const output = await runTarget(params);
  const { scores, passed } = await scoreOutput({
    scorers: params.scorers,
    evalCase: params.evalCase,
    output,
  });
  return { sampleIndex: params.sampleIndex, output, scores, passed };
}

/**
 * Call the target with a signal that fires on the run's abort or the case
 * timeout. The call is raced against both, so a target that ignores its
 * signal cannot hold the run; a throwing target becomes a failed output.
 */
async function runTarget(params: {
  readonly evalCase: EvalCase;
  readonly target: EvalTarget;
  readonly caseTimeoutMs: number | undefined;
  readonly signal?: AbortSignal;
  readonly clock: WallClock;
}): Promise<EvalOutput> {
  const { clock, caseTimeoutMs, signal } = params;
  const startedAt = clock.currentTimeMs();
  const failed = (error: string): EvalOutput => ({
    output: undefined,
    metrics: { latencyMs: clock.currentTimeMs() - startedAt },
    error,
  });
  if (signal?.aborted === true) return failed(`aborted: ${errorMessage(signal.reason)}`);

  const controller = new AbortController();
  let timer: TimerHandle | undefined;
  let removeAbortListener = (): void => {};
  const cutShort = new Promise<EvalOutput>((resolve) => {
    if (caseTimeoutMs !== undefined) {
      timer = clock.setTimeout(() => {
        const message = `timed out after ${caseTimeoutMs}ms`;
        controller.abort(new Error(message));
        resolve(failed(message));
      }, caseTimeoutMs);
    }
    if (signal !== undefined) {
      const onAbort = (): void => {
        controller.abort(signal.reason);
        resolve(failed(`aborted: ${errorMessage(signal.reason)}`));
      };
      signal.addEventListener("abort", onAbort, { once: true });
      removeAbortListener = () => signal.removeEventListener("abort", onAbort);
    }
  });
  const run = (async (): Promise<EvalOutput> => {
    try {
      return await params.target.run(params.evalCase, { signal: controller.signal });
    } catch (err) {
      return failed(errorMessage(err));
    }
  })();
  try {
    return await Promise.race([run, cutShort]);
  } finally {
    timer?.clear();
    removeAbortListener();
  }
}

/** Score one output with every scorer; derive each verdict from its threshold. */
async function scoreOutput(params: {
  readonly scorers: ReadonlyArray<Scorer>;
  readonly evalCase: EvalCase;
  readonly output: EvalOutput;
}): Promise<{ scores: ScoredResult[]; passed: boolean }> {
  const { scorers, evalCase, output } = params;
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
      // A buggy scorer (or an unparsable judge reply) fails its own score —
      // it never aborts the run.
      scores.push({
        scorerId: scorer.id,
        value: 0,
        passed: false,
        threshold,
        required,
        reason: `scorer threw: ${errorMessage(err)}`,
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

function isIntIn(params: { value: number; min: number; max: number }): boolean {
  return Number.isInteger(params.value) && params.value >= params.min && params.value <= params.max;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
