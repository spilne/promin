// ---------------------------------------------------------------------------
// `.mapOver()` per-element resume: after a partial failure or a crash, the
// map step runs again only for elements without a saved result.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { LosslessJsonCodec, type Codec } from "@spilne/perfect-core/connect";
import { InMemoryWorkflowStorage } from "../../in-memory-storage.ts";
import type { StepEff } from "../../step-definition.ts";
import { workflow } from "../../workflow-builder.ts";
import { createWorkflowRunner } from "../../workflow-runner.ts";
import type {
  FenceGuard,
  SaveTaskFailureParams,
  SaveTaskResultParams,
} from "../../workflow-storage.ts";

class Flaky extends TaggedError("Flaky")<{ readonly message: string }>() {}

/**
 * Element body that records each call and fails the first `failures[i]`
 * calls of element `i` with a typed `Flaky`.
 */
function scripted(failures: Record<number, number> = {}) {
  const calls: number[] = [];
  const seen = new Map<number, number>();
  const fn = (n: number): StepEff<number, Flaky> => {
    calls.push(n);
    const k = (seen.get(n) ?? 0) + 1;
    seen.set(n, k);
    if (k <= (failures[n] ?? 0)) return fail(new Flaky({ message: `element ${n} call ${k}` }));
    return succeed(n * 10);
  };
  return { calls, fn };
}

const ITEMS = [0, 1, 2, 3, 4];

describe("mapOver per-element resume", () => {
  it("a workflow retry after a partial failure runs only the failed and unstarted elements", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const { calls, fn } = scripted({ 2: 1 });
    const wf = workflow<number[]>({
      name: "map-wf-retry",
      retry: { maxRetries: 1, baseDelayMs: 1 },
    })
      .step("items", ({ input }) => succeed(input))
      .mapOver("m", { array: "items", concurrency: 1 }, fn)
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-1", input: ITEMS });

    expect(result).toEqual([0, 10, 20, 30, 40]);
    // First pass: 0, 1, then 2 fails (concurrency 1: 3 and 4 never start).
    // The retry runs 2, 3, 4 only.
    expect(calls).toEqual([0, 1, 2, 2, 3, 4]);
  });

  it("a step-level retry runs only the elements earlier attempts did not finish", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const { calls, fn } = scripted({ 3: 2 });
    const wf = workflow<number[]>({ name: "map-step-retry" })
      .step("items", ({ input }) => succeed(input))
      .mapOver("m", { array: "items", concurrency: 1 }, fn, {
        retry: { maxRetries: 2, baseDelayMs: 1 },
      })
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-2", input: ITEMS });

    expect(result).toEqual([0, 10, 20, 30, 40]);
    expect(calls).toEqual([0, 1, 2, 3, 3, 3, 4]);
  });

  it("a crash-resume runs only the elements without a saved task row", async () => {
    const storage = new InMemoryWorkflowStorage();
    const workflowId = "wf-3";
    // A run that stopped inside the map step: the source step completed and
    // elements 0, 1 and 3 have saved results.
    await storage.createWorkflow({ workflowId, workflowName: "map-crash", input: ITEMS });
    await storage.saveStepResult({
      workflowId,
      stepName: "items",
      result: LosslessJsonCodec.encode(ITEMS),
      durationMs: 0,
      startedAt: new Date(),
    });
    for (const i of [0, 1, 3]) {
      await storage.saveTaskResult({
        workflowId,
        stepName: "m",
        taskIndex: i,
        result: LosslessJsonCodec.encode(i * 10),
      });
    }
    await storage.saveTaskFailure({ workflowId, stepName: "m", taskIndex: 2, error: "lost" });

    const { calls, fn } = scripted();
    const wf = workflow<number[]>({ name: "map-crash" })
      .step("items", ({ input }) => succeed(input))
      .mapOver("m", { array: "items" }, fn)
      .build();
    const result = await createWorkflowRunner({ storage }).run({
      workflow: wf,
      workflowId,
      input: ITEMS,
    });

    expect(result).toEqual([0, 10, 20, 30, 40]);
    expect(calls.sort()).toEqual([2, 4]);
    const tasks = storage.getWorkflow(workflowId)?.steps["m"]?.tasks ?? [];
    expect(tasks.every((t) => t.status === "completed")).toBe(true);
  });

  it("saved elements are decoded with the element codec", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const tagged: Codec<{ n: number }> = {
      encode: (v) => `n=${v.n}`,
      decode: (raw) => ({ n: Number(String(raw).slice(2)) }),
    };
    const calls: number[] = [];
    let failOnce = true;
    const wf = workflow<number[]>({ name: "map-codec", retry: { maxRetries: 1, baseDelayMs: 1 } })
      .step("items", ({ input }) => succeed(input))
      .mapOver(
        "m",
        { array: "items", concurrency: 1 },
        (n): StepEff<{ n: number }, Flaky> => {
          calls.push(n);
          if (n === 2 && failOnce) {
            failOnce = false;
            return fail(new Flaky({ message: "once" }));
          }
          return succeed({ n });
        },
        { element: { codec: tagged } },
      )
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-4", input: [1, 2] });

    expect(result).toEqual([{ n: 1 }, { n: 2 }]);
    expect(calls).toEqual([1, 2, 2]);
    const state = storage.getWorkflow("wf-4");
    expect(state?.steps["m"]?.tasks?.map((t) => t.result)).toEqual(["n=1", "n=2"]);
    expect(state?.steps["m"]?.result).toEqual(["n=1", "n=2"]);
  });

  it("an element that fails past its retries is recorded as a failed task row", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const { calls, fn } = scripted({ 1: 5 });
    const wf = workflow<number[]>({ name: "map-fail-row" })
      .step("items", ({ input }) => succeed(input))
      .mapOver("m", { array: "items", concurrency: 1 }, fn, {
        element: { retry: { maxRetries: 1, baseDelayMs: 1 } },
      })
      .build();

    const r = await runner.runSafe({ workflow: wf, workflowId: "wf-5", input: [0, 1, 2] });

    expect((r.error as { _tag?: string } | null)?._tag).toBe("Flaky");
    // Element 1 ran twice (one element retry); element 2 never started.
    expect(calls).toEqual([0, 1, 1]);
    const tasks = storage.getWorkflow("wf-5")?.steps["m"]?.tasks ?? [];
    const byIndex = new Map(tasks.map((t) => [t.taskIndex, t]));
    expect(byIndex.get(0)?.status).toBe("completed");
    expect(byIndex.get(1)?.status).toBe("failed");
    expect(byIndex.get(1)?.error).toBe("element 1 call 2");
    expect(byIndex.has(2)).toBe(false);
  });

  it("element.retry retries one element in place, with the element attempt in ctx", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const attempts: Array<[number, number]> = [];
    const seen = new Map<number, number>();
    const wf = workflow<number[]>({ name: "map-el-retry" })
      .step("items", ({ input }) => succeed(input))
      .mapOver(
        "m",
        { array: "items", concurrency: 1 },
        (n, ctx): StepEff<number, Flaky> => {
          attempts.push([n, ctx.attempt]);
          const k = (seen.get(n) ?? 0) + 1;
          seen.set(n, k);
          return n === 1 && k < 3 ? fail(new Flaky({ message: "again" })) : succeed(n);
        },
        { element: { retry: { maxRetries: 2, baseDelayMs: 1 } } },
      )
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-6", input: [0, 1, 2] });

    expect(result).toEqual([0, 1, 2]);
    expect(attempts).toEqual([
      [0, 1],
      [1, 1],
      [1, 2],
      [1, 3],
      [2, 1],
    ]);
  });

  it("task rows are written under the run's fence guard", async () => {
    const guards: Array<FenceGuard | undefined> = [];
    class SpyStorage extends InMemoryWorkflowStorage {
      override async saveTaskResult({ guard, ...params }: SaveTaskResultParams): Promise<void> {
        guards.push(guard);
        return super.saveTaskResult({ ...params, guard });
      }
      override async saveTaskFailure({ guard, ...params }: SaveTaskFailureParams): Promise<void> {
        guards.push(guard);
        return super.saveTaskFailure({ ...params, guard });
      }
    }
    const storage = new SpyStorage();
    const runner = createWorkflowRunner({ storage });
    const { fn } = scripted({ 2: 1 });
    const wf = workflow<number[]>({ name: "map-fence", retry: { maxRetries: 1, baseDelayMs: 1 } })
      .step("items", ({ input }) => succeed(input))
      .mapOver("m", { array: "items", concurrency: 1 }, fn)
      .build();

    await runner.run({ workflow: wf, workflowId: "wf-7", input: [0, 1, 2] });

    // 0, 1 saved; 2 failed; 2 saved on the retry.
    expect(guards).toHaveLength(4);
    expect(guards.every((g) => g !== undefined)).toBe(true);
  });
});
