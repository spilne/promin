import { describe, it, expect } from "bun:test";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

describe("workflow fresh run (onExpiry: fresh-run)", () => {
  it("re-executes all steps when TTL expires with fresh-run", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "fresh-test" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({
        idempotency: { ttl: 1, onExpiry: "fresh-run" },
      })
      .bind(storage);

    const r1 = await wf.run({ workflowId: "fr-1", input: {} });
    expect(r1).toEqual({ value: 1 });
    expect(runCount).toBe(1);

    await new Promise((r) => setTimeout(r, 10));

    const r2 = await wf.run({ workflowId: "fr-1", input: {} });
    expect(r2).toEqual({ value: 2 });
    expect(runCount).toBe(2);
  });

  it("returns cached result within TTL even with fresh-run", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "fresh-cached" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({
        idempotency: { ttl: 60_000, onExpiry: "fresh-run" },
      })
      .bind(storage);

    await wf.run({ workflowId: "fr-2", input: {} });
    const r2 = await wf.run({ workflowId: "fr-2", input: {} });
    expect(r2).toEqual({ value: 1 });
    expect(runCount).toBe(1);
  });

  it("increments run counter on each fresh run", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow({ name: "run-counter" })
      .stepAsync("compute", async () => ({ ok: true }))
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } })
      .bind(storage);

    await wf.run({ workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(1);

    await new Promise((r) => setTimeout(r, 10));
    await wf.run({ workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(2);

    await new Promise((r) => setTimeout(r, 10));
    await wf.run({ workflowId: "fr-3", input: {} });
    expect(storage.getWorkflow("fr-3")?.run).toBe(3);
  });

  it("preserves step history from previous runs", async () => {
    const storage = new InMemoryWorkflowStorage();
    let callCount = 0;

    const wf = workflow({ name: "history-test" })
      .stepAsync("fetch", async () => {
        callCount++;
        return { data: `result-${callCount}` };
      })
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } })
      .bind(storage);

    await wf.run({ workflowId: "fr-4", input: {} });
    await new Promise((r) => setTimeout(r, 10));
    await wf.run({ workflowId: "fr-4", input: {} });

    const current = storage.getWorkflow("fr-4");
    expect(current?.run).toBe(2);
    expect(current?.steps.fetch?.result).toEqual({ data: "result-2" });

    const history = storage.getStepHistory("fr-4");
    expect(history.length).toBe(1);
    expect(history[0].run).toBe(1);
    expect(history[0].result).toEqual({ data: "result-1" });
  });

  it("multi-step workflow re-executes all steps", async () => {
    const storage = new InMemoryWorkflowStorage();
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
      .build({ idempotency: { ttl: 1, onExpiry: "fresh-run" } })
      .bind(storage);

    const r1 = await wf.run({ workflowId: "fr-5", input: {} });
    expect(r1).toEqual({ a: true, b: true, c: true });
    expect(calls).toEqual(["a", "b", "c"]);

    await new Promise((r) => setTimeout(r, 10));

    calls.length = 0;
    const r2 = await wf.run({ workflowId: "fr-5", input: {} });
    expect(r2).toEqual({ a: true, b: true, c: true });
    expect(calls).toEqual(["a", "b", "c"]);
  });

  it("onExpiry: replay does not start fresh run", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "replay-test" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build({ idempotency: { ttl: 1, onExpiry: "replay" } })
      .bind(storage);

    await wf.run({ workflowId: "fr-6", input: {} });
    expect(runCount).toBe(1);

    await new Promise((r) => setTimeout(r, 10));
    const r2 = await wf.run({ workflowId: "fr-6", input: {} });
    // Replays from storage — same result, no re-execution
    expect(r2).toEqual({ value: 1 });
    expect(storage.getWorkflow("fr-6")?.run).toBe(1);
  });
});
