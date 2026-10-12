import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { inlineDataset } from "../datasets/inline.ts";
import { diffRuns } from "../diff.ts";
import {
  EvalRunAbortedError,
  MAX_EVAL_CONCURRENCY,
  runEval,
  runEvalCase,
  runMatrix,
} from "../runner.ts";
import { exactMatch } from "../scorers/exact-match.ts";
import { InMemoryEvalRunStore } from "../storage/in-memory-run-store.ts";
import { fnTarget } from "../targets/fn-target.ts";
import type { EvalCase, EvalCaseResult, EvalOutput, EvalTarget } from "../types.ts";

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const upper = fnTarget({ fn: (input) => String(input).toUpperCase() });
const cases: EvalCase[] = [
  { id: "1", input: "a", expected: "A" },
  { id: "2", input: "b", expected: "B" },
];

/** Yield to the event loop until `condition` holds (bounded by iterations, not time). */
async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 10_000; i += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("condition never held");
}

/** A target whose runs finish only when the test releases them, by case id. */
function gatedTarget(): {
  target: EvalTarget;
  started: string[];
  release: (caseId: string) => void;
  signals: Map<string, AbortSignal>;
} {
  const started: string[] = [];
  const gates = new Map<string, () => void>();
  const signals = new Map<string, AbortSignal>();
  const target: EvalTarget = {
    id: "gated",
    async run(evalCase, opts) {
      started.push(evalCase.id);
      if (opts?.signal !== undefined) signals.set(evalCase.id, opts.signal);
      await new Promise<void>((resolve) => gates.set(evalCase.id, resolve));
      return { output: String(evalCase.input).toUpperCase(), metrics: { latencyMs: 0 } };
    },
  };
  return {
    target,
    started,
    signals,
    release: (caseId) => gates.get(caseId)?.(),
  };
}

describe("runEval", () => {
  it("scores every case and stamps ranAt (the start) via the clock", async () => {
    const summary = await runEval({
      dataset: inlineDataset({ cases, id: "ds" }),
      target: upper,
      scorers: [exactMatch],
      clock: FakeWallClock.create(5000),
    });
    expect(summary.totalCases).toBe(2);
    expect(summary.passRate).toBe(1);
    expect(summary.ranAt).toBe(5000);
    expect(summary.datasetId).toBe("ds");
    expect(summary.perScorer.exactMatch?.passRate).toBe(1);
  });

  it("fails a case when the threshold-derived verdict fails", async () => {
    const summary = await runEval({
      dataset: inlineDataset({ cases: [{ id: "1", input: "a", expected: "WRONG" }] }),
      target: fnTarget({ fn: (input) => String(input) }),
      scorers: [exactMatch],
    });
    expect(summary.passRate).toBe(0);
    expect(summary.caseResults[0]?.passed).toBe(false);
  });

  it("reports a per-case pass-rate across samples and honours passThreshold", async () => {
    let calls = 0;
    const target = fnTarget({
      fn: () => {
        calls += 1;
        return calls % 2 === 0 ? "good" : "bad";
      },
    });
    const dataset = inlineDataset({ cases: [{ id: "1", input: "x", expected: "good" }] });

    const strict = await runEval({ dataset, target, scorers: [exactMatch], samplesPerCase: 2 });
    expect(strict.caseResults[0]?.passRate).toBe(0.5);
    expect(strict.caseResults[0]?.passed).toBe(false);

    calls = 0;
    const lenient = await runEval({
      dataset,
      target,
      scorers: [exactMatch],
      samplesPerCase: 2,
      passThreshold: 0.5,
    });
    expect(lenient.caseResults[0]?.passed).toBe(true);
  });

  it("bounds concurrency", async () => {
    let active = 0;
    let peak = 0;
    const target = fnTarget({
      fn: async () => {
        active += 1;
        peak = Math.max(peak, active);
        await sleep(5);
        active -= 1;
        return "x";
      },
    });
    const many = Array.from({ length: 8 }, (_unused, i): EvalCase => ({ id: String(i), input: i }));
    await runEval({ dataset: inlineDataset({ cases: many }), target, scorers: [], concurrency: 3 });
    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBeGreaterThan(1);
  });

  it("refuses a concurrency, sample count or threshold it cannot honour", async () => {
    const dataset = inlineDataset({ cases });
    // NaN used to start zero workers and report every case as failed.
    for (const concurrency of [Number.NaN, 0, 1.5, MAX_EVAL_CONCURRENCY + 1]) {
      await expect(
        runEval({ dataset, target: upper, scorers: [exactMatch], concurrency }),
      ).rejects.toBeInstanceOf(RangeError);
    }
    for (const samplesPerCase of [Number.NaN, 0, 2.5]) {
      await expect(
        runEval({ dataset, target: upper, scorers: [exactMatch], samplesPerCase }),
      ).rejects.toBeInstanceOf(RangeError);
    }
    await expect(
      runEval({ dataset, target: upper, scorers: [exactMatch], passThreshold: 1.5 }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      runEval({ dataset, target: upper, scorers: [exactMatch], caseTimeoutMs: 0 }),
    ).rejects.toBeInstanceOf(RangeError);
  });

  it("scores a failed target run as 0 without throwing", async () => {
    const summary = await runEval({
      dataset: inlineDataset({ cases: [{ id: "1", input: "a", expected: "A" }] }),
      target: fnTarget({
        fn: () => {
          throw new Error("target boom");
        },
      }),
      scorers: [exactMatch],
    });
    expect(summary.passRate).toBe(0);
    const score = summary.caseResults[0]?.samples[0]?.scores[0];
    expect(score?.value).toBe(0);
    expect(score?.reason).toContain("target boom");
  });

  it("reports each case as soon as it finishes, before the run completes", async () => {
    const gated = gatedTarget();
    const reported: string[] = [];
    let completed = false;
    const done = runEval({
      dataset: inlineDataset({ cases }),
      target: gated.target,
      scorers: [exactMatch],
      concurrency: 2,
      reporters: [
        { onCaseResult: (result) => void reported.push(result.caseId) },
        { onComplete: () => void (completed = true) },
      ],
    });
    await until(() => gated.started.length === 2);
    gated.release("2");
    await until(() => reported.length === 1);
    expect(reported).toEqual(["2"]);
    expect(completed).toBe(false);
    gated.release("1");
    const summary = await done;
    expect(reported).toEqual(["2", "1"]);
    // The summary keeps dataset order.
    expect(summary.caseResults.map((c) => c.caseId)).toEqual(["1", "2"]);
  });

  it("records the run in the store while it runs", async () => {
    const store = new InMemoryEvalRunStore();
    const gated = gatedTarget();
    const done = runEval({
      dataset: inlineDataset({ cases, id: "ds" }),
      target: gated.target,
      scorers: [exactMatch],
      persist: { store, runId: "r1", suiteId: "suite", suiteVersion: "v2" },
      clock: FakeWallClock.create(7_000),
    });
    await until(() => gated.started.length === 1);
    expect((await store.get("r1"))?.status).toBe("running");
    gated.release("1");
    await until(() => gated.started.length === 2);
    const midway = await store.get("r1");
    expect(midway?.completedCases).toBe(1);
    expect(midway?.caseResults.map((c) => c.caseId)).toEqual(["1"]);
    gated.release("2");
    const summary = await done;
    expect(summary.runId).toBe("r1");
    const stored = await store.get("r1");
    expect(stored?.status).toBe("completed");
    expect(stored?.suiteId).toBe("suite");
    expect(stored?.suiteVersion).toBe("v2");
    expect(stored?.startedAt).toBe(7_000);
    expect(stored?.totals?.passRate).toBe(1);
    expect(stored?.caseResults.length).toBe(2);
  });

  it("times a sample out on the clock, aborts its run and scores it as failed", async () => {
    const clock = FakeWallClock.create(0);
    let signal: AbortSignal | undefined;
    const hanging: EvalTarget = {
      id: "hang",
      run: (_evalCase, opts) => {
        signal = opts?.signal;
        return new Promise<EvalOutput>(() => {});
      },
    };
    const done = runEval({
      dataset: inlineDataset({ cases: [{ id: "1", input: "a", expected: "A" }] }),
      target: hanging,
      scorers: [exactMatch],
      caseTimeoutMs: 500,
      clock,
    });
    await until(() => clock.pendingCount() > 0);
    clock.advance(499);
    expect(signal?.aborted).toBe(false);
    clock.advance(1);
    const summary = await done;
    expect(signal?.aborted).toBe(true);
    expect(summary.passRate).toBe(0);
    expect(summary.caseResults[0]?.samples[0]?.output.error).toBe("timed out after 500ms");
    expect(summary.caseResults[0]?.samples[0]?.output.metrics.latencyMs).toBe(500);
    expect(clock.pendingCount()).toBe(0);
  });

  it("stops on abort, passes the abort to running targets and records the run as cancelled", async () => {
    const store = new InMemoryEvalRunStore();
    const gated = gatedTarget();
    const three = [...cases, { id: "3", input: "c", expected: "C" }];
    const controller = new AbortController();
    const done = runEval({
      dataset: inlineDataset({ cases: three }),
      target: gated.target,
      scorers: [exactMatch],
      concurrency: 2,
      signal: controller.signal,
      persist: { store, runId: "r-abort" },
    });
    await until(() => gated.started.length === 2);
    gated.release("1");
    await until(() => gated.started.length === 3);
    controller.abort(new Error("operator cancelled"));

    const err = await done.then(
      () => undefined,
      (e: unknown) => e,
    );
    if (!(err instanceof EvalRunAbortedError)) throw new Error(`expected an abort, got ${err}`);
    expect(err.caseResults.map((c: EvalCaseResult) => c.caseId)).toEqual(["1"]);
    expect(err.runId).toBe("r-abort");
    expect(err.message).toContain("operator cancelled");
    expect(gated.signals.get("2")?.aborted).toBe(true);
    expect(gated.signals.get("3")?.aborted).toBe(true);
    const stored = await store.get("r-abort");
    expect(stored?.status).toBe("cancelled");
    expect(stored?.caseResults.map((c) => c.caseId)).toEqual(["1"]);
  });

  it("records the run as failed when a reporter throws", async () => {
    const store = new InMemoryEvalRunStore();
    await expect(
      runEval({
        dataset: inlineDataset({ cases }),
        target: upper,
        scorers: [exactMatch],
        reporters: [
          {
            onCaseResult: () => {
              throw new Error("reporter down");
            },
          },
        ],
        persist: { store, runId: "r-fail" },
      }),
    ).rejects.toThrow("reporter down");
    const stored = await store.get("r-fail");
    expect(stored?.status).toBe("failed");
    expect(stored?.error).toBe("reporter down");
  });
});

describe("runEvalCase", () => {
  it("runs every sample of one case and rolls them up", async () => {
    let calls = 0;
    const result = await runEvalCase({
      evalCase: { id: "x", input: "a", expected: "A" },
      target: fnTarget({ fn: () => (++calls === 1 ? "A" : "nope") }),
      scorers: [exactMatch],
      samplesPerCase: 2,
      passThreshold: 0.5,
    });
    expect(result.samples.map((s) => s.passed)).toEqual([true, false]);
    expect(result.passRate).toBe(0.5);
    expect(result.passed).toBe(true);
  });
});

describe("runMatrix + diffRuns", () => {
  it("diffs each target against the first and flags regressions", async () => {
    const { summaries, diffs } = await runMatrix({
      dataset: inlineDataset({ cases }),
      targets: [
        fnTarget({ fn: (input) => String(input).toUpperCase(), id: "good" }),
        fnTarget({ fn: () => "nope", id: "bad" }),
      ],
      scorers: [exactMatch],
    });
    expect(summaries.length).toBe(2);
    expect(diffs.length).toBe(1);
    expect(diffs[0]?.passRateDelta).toBe(-1);
    expect([...(diffs[0]?.regressions ?? [])].sort()).toEqual(["1", "2"]);
  });

  it("marks an improved case", async () => {
    const dataset = inlineDataset({ cases: [{ id: "1", input: "a", expected: "A" }] });
    const baseline = await runEval({
      dataset,
      target: fnTarget({ fn: () => "x" }),
      scorers: [exactMatch],
    });
    const candidate = await runEval({ dataset, target: upper, scorers: [exactMatch] });
    const diff = diffRuns({ baseline, candidate });
    expect(diff.perCase[0]?.status).toBe("improved");
    expect(diff.regressions).toEqual([]);
  });
});
