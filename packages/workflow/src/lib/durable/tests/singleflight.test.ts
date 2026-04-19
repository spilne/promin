import { describe, it, expect } from "bun:test";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

const idempotency = { ttl: 60_000, onInFlight: "join" as const };

describe("workflow singleflight", () => {
  it("start() returns immediately without blocking", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "nonblocking" })
      .stepAsync("slow", async () => {
        await new Promise((r) => setTimeout(r, 100));
        return { done: true };
      })
      .build({ idempotency });

    const handle = await runner.start({ workflow: wf, workflowId: "sf-1", input: {} });
    expect(handle.workflowId).toBe("sf-1");

    const status = await handle.status();
    expect(status).not.toBeNull();

    const result = await handle.result({ timeoutMs: 5_000 });
    expect(result).toEqual({ done: true });
  });

  it("multiple callers share one execution", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "singleflight" })
      .stepAsync("compute", async () => {
        executionCount++;
        await new Promise((r) => setTimeout(r, 50));
        return { value: 42 };
      })
      .build({ idempotency });

    const handleA = await runner.start({ workflow: wf, workflowId: "sf-2", input: {} });
    const handleB = await runner.start({ workflow: wf, workflowId: "sf-2", input: {} });

    expect(handleA.workflowId).toBe("sf-2");
    expect(handleB.workflowId).toBe("sf-2");

    const [resultA, resultB] = await Promise.all([
      handleA.result({ timeoutMs: 5_000 }),
      handleB.result({ timeoutMs: 5_000 }),
    ]);

    expect(resultA).toEqual({ value: 42 });
    expect(resultB).toEqual({ value: 42 });
    expect(executionCount).toBe(1);
  });

  it("three concurrent callers, one execution", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "triple" })
      .stepAsync("compute", async () => {
        executionCount++;
        await new Promise((r) => setTimeout(r, 50));
        return { ok: true };
      })
      .build({ idempotency });

    const [h1, h2, h3] = await Promise.all([
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
    ]);

    const [r1, r2, r3] = await Promise.all([
      h1.result({ timeoutMs: 5_000 }),
      h2.result({ timeoutMs: 5_000 }),
      h3.result({ timeoutMs: 5_000 }),
    ]);

    expect(r1).toEqual({ ok: true });
    expect(r2).toEqual({ ok: true });
    expect(r3).toEqual({ ok: true });
    expect(executionCount).toBe(1);
  });

  it("different workflowIds execute independently", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "independent" })
      .stepAsync("compute", async () => {
        executionCount++;
        return { value: executionCount };
      })
      .build({ idempotency });

    const handleA = await runner.start({ workflow: wf, workflowId: "sf-4a", input: {} });
    const handleB = await runner.start({ workflow: wf, workflowId: "sf-4b", input: {} });

    const [resultA, resultB] = await Promise.all([
      handleA.result({ timeoutMs: 5_000 }),
      handleB.result({ timeoutMs: 5_000 }),
    ]);

    expect(resultA).toEqual({ value: 1 });
    expect(resultB).toEqual({ value: 2 });
    expect(executionCount).toBe(2);
  });

  it("completed workflow returns cached result within TTL", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "cached" })
      .stepAsync("compute", async () => {
        executionCount++;
        return { run: executionCount };
      })
      .build({ idempotency });

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-5", input: {} });
    await h1.result({ timeoutMs: 5_000 });
    expect(executionCount).toBe(1);

    // Within TTL — should return cached, not re-execute
    const h2 = await runner.start({ workflow: wf, workflowId: "sf-5", input: {} });
    const status = await h2.status();
    expect(status?.state).toBe("completed");
    expect(status?.result).toEqual({ run: 1 });
  });

  it("suspended workflow is joined, not re-started", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "suspended-test" })
      .stepAsync("first", async () => {
        executionCount++;
        return { step: 1 };
      })
      .waitForSignal("approval", { signalName: "approve", timeoutMs: 60_000 })
      .build({ idempotency });

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-6", input: {} });
    await new Promise((r) => setTimeout(r, 100));

    const h2 = await runner.start({ workflow: wf, workflowId: "sf-6", input: {} });
    expect(h1.workflowId).toBe("sf-6");
    expect(h2.workflowId).toBe("sf-6");
    expect(executionCount).toBe(1);

    const status = await h2.status();
    expect(status?.state).toBe("suspended");
  });

  it("concurrent start() with same ID should execute only once", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executionCount = 0;

    const wf = workflow({ name: "race-test" })
      .stepAsync("compute", async () => {
        executionCount++;
        await new Promise((r) => setTimeout(r, 50));
        return { value: 42 };
      })
      .build({ idempotency });

    // Fire both start() concurrently — no await between them
    const [handleA, handleB] = await Promise.all([
      runner.start({ workflow: wf, workflowId: "race-1", input: {} }),
      runner.start({ workflow: wf, workflowId: "race-1", input: {} }),
    ]);

    const [resultA, resultB] = await Promise.all([
      handleA.result({ timeoutMs: 5_000 }),
      handleB.result({ timeoutMs: 5_000 }),
    ]);

    expect(resultA).toEqual({ value: 42 });
    expect(resultB).toEqual({ value: 42 });
    // BUG: without atomic createWorkflow, both callers may create the workflow
    // and executionCount could be 2
    expect(executionCount).toBe(1);
  });

  it("without idempotency, start() throws on in-flight workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "no-idempotency" })
      .stepAsync("slow", async () => {
        await new Promise((r) => setTimeout(r, 100));
        return { done: true };
      })
      .build(); // no idempotency config

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-7", input: {} });

    // Second start while running — should throw
    try {
      await runner.start({ workflow: wf, workflowId: "sf-7", input: {} });
      expect(true).toBe(false); // should not reach
    } catch (e: any) {
      expect(e._tag).toBe("WorkflowLockError");
    }

    await h1.result({ timeoutMs: 5_000 });
  });

  // -------------------------------------------------------------------------
  // deriveId — workflowId derived from input at the call site
  // -------------------------------------------------------------------------
  //
  // Post-refactor the runner takes an explicit `workflowId`; callers derive
  // it themselves at the call site. That's a minor ergonomic loss over the
  // old `wf.start(input)` shortcut, but keeps the runner API orthogonal
  // (one way to start; deriveId is just a user-side helper).

  it("deriveId generates workflowId from input", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const deriveId = (input: { userId: string; orderId: string }) =>
      `order:${input.userId}:${input.orderId}`;
    const wf = workflow({ name: "derive" })
      .stepAsync("compute", async () => ({ done: true }))
      .build({ idempotency });

    const input = { userId: "u1", orderId: "o5" };
    const handle = await runner.start({ workflow: wf, workflowId: deriveId(input), input });
    expect(handle.workflowId).toBe("order:u1:o5");

    const result = await handle.result({ timeoutMs: 5_000 });
    expect(result).toEqual({ done: true });
  });

  it("deriveId deduplicates same logical request", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let executions = 0;

    const deriveId = (input: { id: string }) => `key:${input.id}`;
    const wf = workflow({ name: "derive-dedup" })
      .stepAsync("compute", async () => {
        executions++;
        await new Promise((r) => setTimeout(r, 50));
        return { value: 42 };
      })
      .build({ idempotency });

    const input = { id: "abc" };
    const h1 = await runner.start({ workflow: wf, workflowId: deriveId(input), input });
    const h2 = await runner.start({ workflow: wf, workflowId: deriveId(input), input });

    expect(h1.workflowId).toBe("key:abc");
    expect(h2.workflowId).toBe("key:abc");

    const [r1, r2] = await Promise.all([
      h1.result({ timeoutMs: 5_000 }),
      h2.result({ timeoutMs: 5_000 }),
    ]);

    expect(r1).toEqual({ value: 42 });
    expect(r2).toEqual({ value: 42 });
    expect(executions).toBe(1);
  });
});
