// ---------------------------------------------------------------------------
// Per-call idempotency key — `runner.run({ idempotencyKey, idempotencyKeyTTL })`.
//
// Solves the auto-mint case (Zorya's `workflows.trigger()` mints a fresh
// UUID workflowId per call). Without the key, repeated triggers produce
// distinct workflowIds → no dedup. The key resolves to a workflowId so
// repeats land on the same run while the key is unexpired.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("ctx.run({ idempotencyKey })", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("resolves to the same workflowId on repeated calls within TTL", async () => {
    let calls = 0;
    const wf = workflow<{ n: number }>({ name: "compute" })
      .step("multiply", ({ input }) => {
        calls++;
        return Pipeline.succeed(input.n * 2);
      })
      .build();

    const runner = createWorkflowRunner({ storage });

    // Fresh UUIDs simulate the auto-mint path.
    const id1 = crypto.randomUUID();
    const id2 = crypto.randomUUID();
    expect(id1).not.toBe(id2);

    const r1 = await runner.run({
      workflow: wf,
      workflowId: id1,
      input: { n: 7 },
      idempotencyKey: "stripe-evt-42",
      idempotencyKeyTTL: 60_000,
    });
    expect(r1).toBe(14);
    expect(calls).toBe(1);

    // Second call uses a different auto-minted id but same key — should
    // resolve to the same run, return its cached result, and NOT
    // re-execute the body.
    const r2 = await runner.run({
      workflow: wf,
      workflowId: id2,
      input: { n: 7 },
      idempotencyKey: "stripe-evt-42",
      idempotencyKeyTTL: 60_000,
    });
    expect(r2).toBe(14);
    // Body ran exactly once.
    expect(calls).toBe(1);

    // Storage rows: only id1 was created; id2 never got a workflow row
    // because the redirect happened before createWorkflow.
    const wf1 = await storage.loadWorkflow(id1);
    const wf2 = await storage.loadWorkflow(id2);
    expect(wf1).not.toBeNull();
    expect(wf2).toBeNull();
  });

  it("distinct keys map to distinct workflowIds", async () => {
    const wf = workflow<{ n: number }>({ name: "compute" })
      .step("multiply", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    const runner = createWorkflowRunner({ storage });

    const id1 = crypto.randomUUID();
    const id2 = crypto.randomUUID();

    await runner.run({
      workflow: wf,
      workflowId: id1,
      input: { n: 1 },
      idempotencyKey: "key-A",
      idempotencyKeyTTL: 60_000,
    });
    await runner.run({
      workflow: wf,
      workflowId: id2,
      input: { n: 2 },
      idempotencyKey: "key-B",
      idempotencyKeyTTL: 60_000,
    });

    expect(await storage.loadWorkflow(id1)).not.toBeNull();
    expect(await storage.loadWorkflow(id2)).not.toBeNull();
  });

  it("expired key allows a new claim with a different workflowId", async () => {
    const wf = workflow<{ n: number }>({ name: "compute" })
      .step("multiply", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    const runner = createWorkflowRunner({ storage });

    const id1 = crypto.randomUUID();
    await runner.run({
      workflow: wf,
      workflowId: id1,
      input: { n: 1 },
      idempotencyKey: "ephemeral",
      idempotencyKeyTTL: 1, // 1ms — expires immediately
    });

    // Wait so the key is unambiguously past expiry.
    await new Promise((r) => setTimeout(r, 5));

    const id2 = crypto.randomUUID();
    await runner.run({
      workflow: wf,
      workflowId: id2,
      input: { n: 2 },
      idempotencyKey: "ephemeral",
      idempotencyKeyTTL: 60_000,
    });

    // Both rows exist — the key reused the slot for a fresh run.
    expect(await storage.loadWorkflow(id1)).not.toBeNull();
    expect(await storage.loadWorkflow(id2)).not.toBeNull();

    // The current key→id mapping points to id2 (the live, unexpired entry).
    const hit = await storage.findWorkflowByIdempotencyKey({
      workflowName: "compute",
      idempotencyKey: "ephemeral",
      now: new Date(),
    });
    expect(hit?.workflowId).toBe(id2);
  });

  it("rejects calls with a key but no TTL", async () => {
    const wf = workflow<{ n: number }>({ name: "compute" })
      .step("multiply", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    const runner = createWorkflowRunner({ storage });

    await expect(
      runner.run({
        workflow: wf,
        workflowId: crypto.randomUUID(),
        input: { n: 1 },
        idempotencyKey: "key",
        // idempotencyKeyTTL omitted — must throw
      } as any),
    ).rejects.toThrow(/idempotencyKey.*requires.*idempotencyKeyTTL/i);
  });

  it("workflows with the same key but different names don't collide", async () => {
    const wfA = workflow<{ n: number }>({ name: "A" })
      .step("multiply", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();
    const wfB = workflow<{ n: number }>({ name: "B" })
      .step("triple", ({ input }) => Pipeline.succeed(input.n * 3))
      .build();

    const runner = createWorkflowRunner({ storage });

    const idA = crypto.randomUUID();
    const idB = crypto.randomUUID();
    const rA = await runner.run({
      workflow: wfA,
      workflowId: idA,
      input: { n: 4 },
      idempotencyKey: "shared",
      idempotencyKeyTTL: 60_000,
    });
    const rB = await runner.run({
      workflow: wfB,
      workflowId: idB,
      input: { n: 4 },
      idempotencyKey: "shared",
      idempotencyKeyTTL: 60_000,
    });
    expect(rA).toBe(8);
    expect(rB).toBe(12);

    // Both rows exist — keys are scoped to (workflowName, key).
    expect(await storage.loadWorkflow(idA)).not.toBeNull();
    expect(await storage.loadWorkflow(idB)).not.toBeNull();
  });

  it("composes with workflow-level idempotency.ttl for result caching", async () => {
    let calls = 0;
    const wf = workflow<{ n: number }>({ name: "compute" })
      .step("multiply", ({ input }) => {
        calls++;
        return Pipeline.succeed(input.n * 2);
      })
      .build({ idempotency: { ttl: 60_000, onInFlight: "join" } });

    const runner = createWorkflowRunner({ storage });
    const id1 = crypto.randomUUID();
    const id2 = crypto.randomUUID();

    // First call: fresh run.
    const r1 = await runner.run({
      workflow: wf,
      workflowId: id1,
      input: { n: 5 },
      idempotencyKey: "compose-test",
      idempotencyKeyTTL: 60_000,
    });
    expect(r1).toBe(10);
    expect(calls).toBe(1);

    // Second call: key resolves → workflowId=id1 → workflow-level
    // idempotency cache returns the result without re-executing.
    const r2 = await runner.run({
      workflow: wf,
      workflowId: id2,
      input: { n: 5 },
      idempotencyKey: "compose-test",
      idempotencyKeyTTL: 60_000,
    });
    expect(r2).toBe(10);
    expect(calls).toBe(1);
  });
});
