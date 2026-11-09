import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { resolveStepConcurrency, resolveStepGroup } from "../runner/step-concurrency.ts";
import { mergeWaits, summarizeWaits, type WorkflowWait } from "../workflow-waits.ts";
import type { StepDefinition } from "../step-definition.ts";

const stepDef = (queue?: StepDefinition["queue"]): StepDefinition =>
  ({
    name: "call",
    dependsOn: [],
    kind: "normal",
    ...(queue && { queue }),
  }) as unknown as StepDefinition;

const base = {
  workflowInput: { tenant: "acme" },
  stepInput: { tenant: "acme" },
  workflowId: "wf-1",
  attempt: 1,
  results: {},
};

describe("step group resolution", () => {
  it("stamps the group's limits and the evaluated fairness key; a step's group wins over the workflow's", () => {
    const resolved = resolveStepGroup({
      ...base,
      stepDef: stepDef({
        group: {
          name: "openai",
          maxConcurrent: 2,
          maxStarts: 60,
          windowMs: 60_000,
          fairnessKey: (ctx) => (ctx.input as { tenant: string }).tenant,
        },
      }),
      workflowQueue: { group: { name: "ignored", maxConcurrent: 9 } },
    });
    expect(resolved).toEqual({
      groupName: "openai",
      groupMaxConcurrent: 2,
      groupMaxStarts: 60,
      groupWindowMs: 60_000,
      fairnessKey: "acme",
    });
  });

  it("falls back to the workflow's group (fairness key from the workflow input) and to none", () => {
    expect(
      resolveStepGroup({
        ...base,
        stepDef: stepDef(),
        workflowQueue: {
          group: { name: "wf-group", maxConcurrent: 1, fairnessKey: (i: any) => i.tenant },
        },
      }),
    ).toEqual({ groupName: "wf-group", groupMaxConcurrent: 1, fairnessKey: "acme" });
    expect(resolveStepGroup({ ...base, stepDef: stepDef() })).toBeNull();
  });

  it("rejects limits the queue could not enforce", () => {
    const resolve = (group: {
      name: string;
      maxStarts?: number;
      windowMs?: number;
      maxConcurrent?: number;
    }) => resolveStepGroup({ ...base, stepDef: stepDef({ group }) });
    expect(() => resolve({ name: "g", maxStarts: 5 })).toThrow("go together");
    expect(() => resolve({ name: "g", windowMs: 1000 })).toThrow("go together");
    expect(() => resolve({ name: "g", maxConcurrent: 0 })).toThrow("positive integer");
    expect(() => resolve({ name: "", maxConcurrent: 1 })).toThrow("name");
    expect(resolve({ name: "g" })).toEqual({ groupName: "g" });
  });

  it("a step-level queue with only a group leaves the workflow's concurrency cap in force", () => {
    const concurrency = resolveStepConcurrency({
      ...base,
      workflowName: "wf",
      stepDef: stepDef({ group: { name: "g", maxConcurrent: 1 } }),
      workflowQueue: { concurrencyLimit: 3 },
    });
    expect(concurrency).toEqual({ scope: "wf", key: "__all__", limit: 3 });
  });

  it("an in-process run ignores groups (no queue involved) and completes", async () => {
    const wf = workflow<number>({ name: "inline-group" })
      .step("call", ({ prev }) => succeed(prev + 1), {
        queue: { group: { name: "openai", maxConcurrent: 1 } },
      })
      .build();
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    expect(await runner.run({ workflow: wf, workflowId: "w", input: 1 })).toBe(2);
  });
});

describe("step-queue waits", () => {
  const since = new Date("2026-01-01T00:00:00Z");
  const queued: WorkflowWait = {
    kind: "step-queue",
    stepName: "call",
    since,
    until: new Date(since.getTime() + 90_000),
    groupName: "openai",
    reason: "rate",
    attempt: 1,
  };

  it("summarises the group and the time until the window frees a slot", () => {
    expect(summarizeWaits({ waits: [queued], now: since })).toBe(
      "queued behind rate limit of group openai (step call, next slot in 1m30s)",
    );
    expect(
      summarizeWaits({
        waits: [{ ...queued, reason: "concurrency", until: undefined }],
        now: since,
      }),
    ).toBe("queued behind concurrency limit of group openai (step call)");
  });

  it("merge keeps stored waits and drops a queued wait for a step already parked, ordered by step", () => {
    const sleeping: WorkflowWait = { kind: "sleep", stepName: "call", since };
    const other: WorkflowWait = { ...queued, stepName: "a-first" };
    expect(
      mergeWaits({ stored: [sleeping], extra: [queued, other] }).map((w) => [w.stepName, w.kind]),
    ).toEqual([
      ["a-first", "step-queue"],
      ["call", "sleep"],
    ]);
  });
});
