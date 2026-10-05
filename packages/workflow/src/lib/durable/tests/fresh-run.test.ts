import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00Z";

describe("workflow fresh run (onExpiry: fresh-run)", () => {
  it("re-executes all steps when TTL expires with fresh-run", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let runCount = 0;

    const wf = workflow({ name: "fresh-test" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({
        idempotency: { ttl: 1, onExpiry: "fresh-run" },
      });

    const r1 = await runner.run({ workflow: wf, workflowId: "fr-1", input: {} });
    expect(r1).toEqual({ value: 1 });
    expect(runCount).toBe(1);

    clock.advance(10);

    const r2 = await runner.run({ workflow: wf, workflowId: "fr-1", input: {} });
    expect(r2).toEqual({ value: 2 });
    expect(runCount).toBe(2);
  });

  it("returns cached result within TTL even with fresh-run", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let runCount = 0;

    const wf = workflow({ name: "fresh-cached" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({
        idempotency: { ttl: 60_000, onExpiry: "fresh-run" },
      });

    await runner.run({ workflow: wf, workflowId: "fr-2", input: {} });
    const r2 = await runner.run({ workflow: wf, workflowId: "fr-2", input: {} });
    expect(r2).toEqual({ value: 1 });
    expect(runCount).toBe(1);
  });

  it("increments run counter on each fresh run", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });

    const wf = workflow({ name: "run-counter" })
      .stepAsync("compute", async () => ({ ok: true }))
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } });

    await runner.run({ workflow: wf, workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(1);

    clock.advance(10);
    await runner.run({ workflow: wf, workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(2);

    clock.advance(10);
    await runner.run({ workflow: wf, workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(3);
  });

  it("preserves step history from previous runs", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let callCount = 0;

    const wf = workflow({ name: "history-test" })
      .stepAsync("fetch", async () => {
        callCount++;
        return { data: `result-${callCount}` };
      })
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } });

    await runner.run({ workflow: wf, workflowId: "fr-4", input: {} });
    clock.advance(10);
    await runner.run({ workflow: wf, workflowId: "fr-4", input: {} });

    const current = storage.getWorkflow("fr-4");
    expect(current?.run).toBe(2);
    expect(current?.steps.fetch?.result).toEqual({ data: "result-2" });

    const history = storage.getStepHistory("fr-4");
    expect(history.length).toBe(1);
    expect(history[0].run).toBe(1);
    expect(history[0].result).toEqual({ data: "result-1" });
  });

  it("multi-step workflow re-executes all steps", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const calls: string[] = [];

    const wf = workflow({ name: "multi-step" })
      .stepAsync("step-a", async () => {
        calls.push("a");
        return { a: true };
      })
      .stepAsync("step-b", async ({ prev }) => {
        calls.push("b");
        return { ...prev, b: true };
      })
      .stepAsync("step-c", async ({ prev }) => {
        calls.push("c");
        return { ...prev, c: true };
      })
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } });

    const r1 = await runner.run({ workflow: wf, workflowId: "fr-5", input: {} });
    expect(r1).toEqual({ a: true, b: true, c: true });
    expect(calls).toEqual(["a", "b", "c"]);

    clock.advance(10);

    calls.length = 0;
    const r2 = await runner.run({ workflow: wf, workflowId: "fr-5", input: {} });
    expect(r2).toEqual({ a: true, b: true, c: true });
    expect(calls).toEqual(["a", "b", "c"]);
  });

  it("onExpiry: replay does not start fresh run", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    let runCount = 0;

    const wf = workflow({ name: "replay-test" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({ idempotency: { ttl: 1, onExpiry: "replay" } });

    await runner.run({ workflow: wf, workflowId: "fr-6", input: {} });
    expect(runCount).toBe(1);

    clock.advance(10);
    const r2 = await runner.run({ workflow: wf, workflowId: "fr-6", input: {} });
    // Replays from storage — same result, no re-execution
    expect(r2).toEqual({ value: 1 });
    expect(storage.getWorkflow("fr-6")?.run).toBe(1);
  });
});
