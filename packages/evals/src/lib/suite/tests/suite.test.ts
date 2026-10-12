import { describe, expect, it } from "bun:test";
import { isRegistryInputError } from "@promin/agent";
import { versionedRegistryKindTestSuite } from "@promin/agent/testing";
import { InMemoryVersionedRegistry } from "@promin/agent";
import { InMemoryEvalRunStore } from "../../storage/in-memory-run-store.ts";
import { InMemoryEvalDatasetStore } from "../../storage/in-memory-dataset-store.ts";
import { fnTarget } from "../../targets/fn-target.ts";
import { checkEvalGate } from "../gate.ts";
import { InMemoryEvalGateRegistry, InMemoryEvalSuiteRegistry } from "../registries.ts";
import { EVAL_GATE_FIXTURE, EVAL_SUITE_FIXTURE } from "../registry-fixtures.ts";
import { resolveEvalSuite, runEvalSuite } from "../resolve.ts";
import type { EvalSuiteInput } from "../types.ts";

versionedRegistryKindTestSuite({
  name: "InMemoryVersionedRegistry (eval suites)",
  factory: ({ kind, clock }) => new InMemoryVersionedRegistry({ kind, clock }),
  fixture: EVAL_SUITE_FIXTURE,
});
versionedRegistryKindTestSuite({
  name: "InMemoryVersionedRegistry (eval gates)",
  factory: ({ kind, clock }) => new InMemoryVersionedRegistry({ kind, clock }),
  fixture: EVAL_GATE_FIXTURE,
});

const SUITE: EvalSuiteInput = {
  description: "upper-casing",
  dataset: {
    kind: "inline",
    id: "ds",
    cases: [
      { id: "1", input: "a", expected: "A" },
      { id: "2", input: "b", expected: "B" },
    ],
  },
  scorers: [{ kind: "exactMatch" }],
  metadata: { agentIds: ["bot"] },
};

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected a rejection");
}

describe("EVAL_SUITE_KIND", () => {
  it("normalizes a suite and versions it like any registry entry", async () => {
    const registry = new InMemoryEvalSuiteRegistry();
    const v1 = await registry.register({ id: "upper", ...SUITE });
    expect(v1.version).toBe("v1");
    expect(v1.active).toBe(true);
    expect(v1.metadata).toEqual({ tags: [], agentIds: ["bot"] });
    const v2 = await registry.register({ id: "upper", ...SUITE, samplesPerCase: 3 });
    expect(v2.version).toBe("v2");
    expect(v2.active).toBe(false); // a draft until promoted
    expect((await registry.get({ id: "upper" }))?.version).toBe("v1");
    await registry.promote({ id: "upper", version: "v2" });
    expect((await registry.get({ id: "upper" }))?.samplesPerCase).toBe(3);
    expect((await registry.list({ filter: { agentId: "bot" } })).items.map((s) => s.id)).toEqual([
      "upper",
    ]);
  });

  it("refuses a malformed suite", async () => {
    const registry = new InMemoryEvalSuiteRegistry();
    const bad: Array<Partial<EvalSuiteInput>> = [
      { scorers: [] },
      { scorers: [{ kind: "exactMatch" }, { kind: "exactMatch" }] },
      { dataset: { kind: "inline", id: "ds", cases: [] } },
      { dataset: { kind: "stored", datasetId: "" } },
      { samplesPerCase: 0 },
      { concurrency: 1_000 },
      { passThreshold: 2 },
      { caseTimeoutMs: -1 },
    ];
    for (const over of bad) {
      const err = await rejection(registry.register({ id: "s", ...SUITE, ...over }));
      expect(isRegistryInputError(err)).toBe(true);
    }
  });
});

describe("resolveEvalSuite / runEvalSuite", () => {
  it("runs a registered suite against targets and records the runs with its identity", async () => {
    const registry = new InMemoryEvalSuiteRegistry();
    const suite = await registry.register({ id: "upper", ...SUITE });
    const store = new InMemoryEvalRunStore();
    const result = await runEvalSuite({
      suite,
      targets: [
        fnTarget({ fn: (input) => String(input).toUpperCase(), id: "bot", version: "v1" }),
        fnTarget({ fn: () => "nope", id: "bot", version: "v2" }),
      ],
      persist: { store },
    });
    expect(result.summaries.map((s) => s.passRate)).toEqual([1, 0]);
    expect(result.diffs[0]?.regressions).toEqual(["1", "2"]);
    const runs = await store.list({ suiteId: "upper", suiteVersion: "v1" });
    expect(runs.map((r) => r.targetVersion).sort()).toEqual(["v1", "v2"]);
  });

  it("applies scorer id and threshold overrides", () => {
    const resolved = resolveEvalSuite({
      suite: {
        description: null,
        dataset: SUITE.dataset,
        scorers: [
          { kind: "regexMatch", pattern: "x", id: "has-x", threshold: 0.5, required: false },
        ],
        metadata: { tags: [], agentIds: [] },
      },
    });
    expect(resolved.scorers[0]?.id).toBe("has-x");
    expect(resolved.scorers[0]?.threshold).toBe(0.5);
    expect(resolved.scorers[0]?.required).toBe(false);
  });

  it("resolves a stored dataset, and refuses jsonl or missing deps when asked", async () => {
    const datasets = new InMemoryEvalDatasetStore();
    await datasets.save({ datasetId: "saved", cases: [{ id: "1", input: "a", expected: "A" }] });
    const base = {
      description: null,
      scorers: SUITE.scorers,
      metadata: { tags: [], agentIds: [] },
    };
    const resolved = resolveEvalSuite({
      suite: { ...base, dataset: { kind: "stored", datasetId: "saved" } },
      deps: { datasetStore: datasets },
    });
    expect(resolved.dataset.id).toBe("saved");
    expect(() =>
      resolveEvalSuite({ suite: { ...base, dataset: { kind: "stored", datasetId: "saved" } } }),
    ).toThrow("datasetStore");
    expect(() =>
      resolveEvalSuite({
        suite: { ...base, dataset: { kind: "jsonl", path: "/etc/passwd" } },
        deps: { allowJsonl: false },
      }),
    ).toThrow("not allowed");
    expect(() =>
      resolveEvalSuite({
        suite: {
          ...base,
          scorers: [{ kind: "llmJudge", judgeRef: "main", rubric: "r" }],
          dataset: SUITE.dataset,
        },
      }),
    ).toThrow("judge");
  });
});

describe("checkEvalGate", () => {
  it("passes a version only on its newest completed run of the suite at the bar", async () => {
    const runs = new InMemoryEvalRunStore();
    const gates = new InMemoryEvalGateRegistry();
    const gate = await gates.register({ id: "bot", suiteId: "upper", minScore: 0.75 });
    const record = async (params: {
      runId: string;
      version: string;
      startedAt: number;
      passRate: number;
      status?: "completed" | "failed";
    }) => {
      await runs.start({
        runId: params.runId,
        targetId: "bot",
        targetVersion: params.version,
        datasetId: "ds",
        suiteId: "upper",
        suiteVersion: "v1",
        samplesPerCase: 1,
        totalCases: 4,
        startedAt: params.startedAt,
      });
      await runs.finish({
        runId: params.runId,
        status: params.status ?? "completed",
        finishedAt: params.startedAt + 1,
        totals: {
          passRate: params.passRate,
          perScorer: { exactMatch: { meanValue: 0.6, passRate: params.passRate } },
          scoredCases: 4,
        },
      });
    };

    const none = await checkEvalGate({
      gate,
      runs,
      agentId: "bot",
      version: "v2",
      activeSuiteVersion: "v1",
    });
    expect(none.passed).toBe(false);
    expect(none.latest).toBeNull();
    expect(none.reason).toContain("no completed run");

    await record({ runId: "old", version: "v2", startedAt: 1, passRate: 1 });
    await record({ runId: "new", version: "v2", startedAt: 2, passRate: 0.5 });
    await record({ runId: "crashed", version: "v2", startedAt: 3, passRate: 1, status: "failed" });
    await record({ runId: "other-version", version: "v3", startedAt: 4, passRate: 1 });
    const failing = await checkEvalGate({
      gate,
      runs,
      agentId: "bot",
      version: "v2",
      activeSuiteVersion: "v1",
    });
    expect(failing.passed).toBe(false);
    expect(failing.latest?.runId).toBe("new");
    expect(failing.score).toBe(0.5);

    expect(
      (await checkEvalGate({ gate, runs, agentId: "bot", version: "v3", activeSuiteVersion: "v1" }))
        .passed,
    ).toBe(true);
    const byScorer = await checkEvalGate({
      gate: { ...gate, scorerId: "exactMatch", minScore: 0.5 },
      runs,
      agentId: "bot",
      version: "v2",
      activeSuiteVersion: "v1",
    });
    expect(byScorer.passed).toBe(true);
    expect(byScorer.score).toBe(0.6);
    const pinned = await checkEvalGate({
      gate: { ...gate, suiteVersion: "v9" },
      runs,
      agentId: "bot",
      version: "v3",
      activeSuiteVersion: "v1",
    });
    expect(pinned.passed).toBe(false);
    // A run of another suite version (a draft) never counts for an unpinned gate.
    expect(
      (await checkEvalGate({ gate, runs, agentId: "bot", version: "v3", activeSuiteVersion: "v2" }))
        .passed,
    ).toBe(false);
    expect(
      (await checkEvalGate({ gate, runs, agentId: "bot", version: "v3", activeSuiteVersion: null }))
        .reason,
    ).toContain("no active version");
  });

  it("refuses a gate outside 0..1", async () => {
    const gates = new InMemoryEvalGateRegistry();
    expect(
      isRegistryInputError(
        await rejection(gates.register({ id: "bot", suiteId: "s", minScore: 2 })),
      ),
    ).toBe(true);
  });
});
