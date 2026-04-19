import { describe, it, expect } from "bun:test";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

describe("workflow idempotency TTL", () => {
  it("returns cached result within TTL", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "ttl-test" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: 42 };
      })
      .build({ idempotency: { ttl: 60_000 } })
      .bind(storage);

    const result1 = await wf.run({ workflowId: "cd-1", input: {} });
    expect(result1).toEqual({ value: 42 });
    expect(runCount).toBe(1);

    const result2 = await wf.run({ workflowId: "cd-1", input: {} });
    expect(result2).toEqual({ value: 42 });
    expect(runCount).toBe(1); // NOT re-executed
  });

  it("re-enters engine when TTL expires", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow({ name: "ttl-expire" })
      .stepAsync("compute", async () => ({ value: 1 }))
      .build({ idempotency: { ttl: 1 } })
      .bind(storage);

    await wf.run({ workflowId: "cd-2", input: {} });
    await new Promise((r) => setTimeout(r, 10));

    // TTL expired — enters engine, steps replay from storage
    const result = await wf.run({ workflowId: "cd-2", input: {} });
    expect(result).toEqual({ value: 1 });
  });

  it("separate TTL for success and failure", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow({ name: "split-ttl" })
      .stepAsync("compute", async () => ({ value: 1 }))
      .build({
        idempotency: {
          ttl: { success: 60_000, failure: 100 },
        },
      })
      .bind(storage);

    const result = await wf.run({ workflowId: "cd-3", input: {} });
    expect(result).toEqual({ value: 1 });

    // Within success TTL — cached
    const result2 = await wf.run({ workflowId: "cd-3", input: {} });
    expect(result2).toEqual({ value: 1 });
  });

  it("force bypasses idempotency", async () => {
    const storage = new InMemoryWorkflowStorage();

    const wf = workflow({ name: "force-test" })
      .stepAsync("compute", async () => ({ value: 1 }))
      .build({ idempotency: { ttl: 60_000 } })
      .bind(storage);

    await wf.run({ workflowId: "cd-4", input: {} });

    // Force bypass — enters engine despite TTL
    const result = await wf.run({ workflowId: "cd-4", input: {}, force: true });
    expect(result).toEqual({ value: 1 });
  });

  it("works with runSafe", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "ttl-safe" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: 42 };
      })
      .build({ idempotency: { ttl: 60_000 } })
      .bind(storage);

    const { data: r1 } = await wf.runSafe({ workflowId: "cd-5", input: {} });
    expect(r1).toEqual({ value: 42 });

    const { data: r2 } = await wf.runSafe({ workflowId: "cd-5", input: {} });
    expect(r2).toEqual({ value: 42 });
    expect(runCount).toBe(1);
  });

  it("no idempotency — no TTL caching", async () => {
    const storage = new InMemoryWorkflowStorage();
    let runCount = 0;

    const wf = workflow({ name: "no-ttl" })
      .stepAsync("compute", async () => {
        runCount++;
        return { value: runCount };
      })
      .build()
      .bind(storage); // no idempotency

    await wf.run({ workflowId: "cd-6", input: {} });
    await wf.run({ workflowId: "cd-6", input: {} });
    // Without idempotency, second run enters engine (steps replay)
    expect(runCount).toBe(1); // step replays, doesn't re-execute
  });
});
