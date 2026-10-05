// ---------------------------------------------------------------------------
// Singleflight: concurrent `start()` calls on one workflowId share one
// execution. Step bodies park on a gate the test opens, so "still in
// flight" never depends on how long a real-time sleep lasts, and
// `handle.result()` polls on a FakeWallClock the test advances.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const idempotency = { ttl: 60_000, onInFlight: "join" as const };

/** A promise the test resolves by hand. */
function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((r) => (open = r));
  return { promise, open };
}

function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const runner = createWorkflowRunner({ storage, clock });
  return { clock, storage, runner };
}

/**
 * Await `promise` while advancing the clock (one `result()` poll interval at
 * a time) so handle polls on the fake clock make progress.
 */
async function drive<T>(clock: FakeWallClock, promise: Promise<T>): Promise<T> {
  let settled = false;
  const tracked = promise.finally(() => (settled = true));
  tracked.catch(() => {});
  for (let i = 0; i < 100 && !settled; i++) {
    for (let j = 0; j < 20 && !settled; j++) await new Promise<void>((r) => setImmediate(r));
    if (!settled) clock.advance(1_000);
  }
  return tracked;
}

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

describe("workflow singleflight", () => {
  it("start() returns immediately without blocking", async () => {
    const { clock, runner } = setup();
    const slow = gate();

    const wf = workflow({ name: "nonblocking" })
      .stepAsync("slow", async () => {
        await slow.promise;
        return { done: true };
      })
      .build({ idempotency });

    const handle = await runner.start({ workflow: wf, workflowId: "sf-1", input: {} });
    expect(handle.workflowId).toBe("sf-1");

    // The step is still parked, so start() did not wait for it.
    const status = await handle.status();
    expect(["pending", "running"]).toContain(status?.state ?? "missing");

    slow.open();
    const result = await drive(clock, handle.result({ timeoutMs: 5_000 }));
    expect(result).toEqual({ done: true });
  });

  it("multiple callers share one execution", async () => {
    const { clock, runner } = setup();
    const release = gate();
    let executionCount = 0;

    const wf = workflow({ name: "singleflight" })
      .stepAsync("compute", async () => {
        executionCount++;
        await release.promise;
        return { value: 42 };
      })
      .build({ idempotency });

    const handleA = await runner.start({ workflow: wf, workflowId: "sf-2", input: {} });
    const handleB = await runner.start({ workflow: wf, workflowId: "sf-2", input: {} });

    expect(handleA.workflowId).toBe("sf-2");
    expect(handleB.workflowId).toBe("sf-2");

    release.open();
    const [resultA, resultB] = await drive(
      clock,
      Promise.all([handleA.result({ timeoutMs: 5_000 }), handleB.result({ timeoutMs: 5_000 })]),
    );

    expect(resultA).toEqual({ value: 42 });
    expect(resultB).toEqual({ value: 42 });
    expect(executionCount).toBe(1);
  });

  it("three concurrent callers, one execution", async () => {
    const { clock, runner } = setup();
    const release = gate();
    let executionCount = 0;

    const wf = workflow({ name: "triple" })
      .stepAsync("compute", async () => {
        executionCount++;
        await release.promise;
        return { ok: true };
      })
      .build({ idempotency });

    const [h1, h2, h3] = await Promise.all([
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
      runner.start({ workflow: wf, workflowId: "sf-3", input: {} }),
    ]);

    release.open();
    const [r1, r2, r3] = await drive(
      clock,
      Promise.all([
        h1.result({ timeoutMs: 5_000 }),
        h2.result({ timeoutMs: 5_000 }),
        h3.result({ timeoutMs: 5_000 }),
      ]),
    );

    expect(r1).toEqual({ ok: true });
    expect(r2).toEqual({ ok: true });
    expect(r3).toEqual({ ok: true });
    expect(executionCount).toBe(1);
  });

  it("different workflowIds execute independently", async () => {
    const { clock, runner } = setup();
    let executionCount = 0;

    const wf = workflow({ name: "independent" })
      .stepAsync("compute", async () => {
        executionCount++;
        return { value: executionCount };
      })
      .build({ idempotency });

    const handleA = await runner.start({ workflow: wf, workflowId: "sf-4a", input: {} });
    const handleB = await runner.start({ workflow: wf, workflowId: "sf-4b", input: {} });

    const [resultA, resultB] = await drive(
      clock,
      Promise.all([handleA.result({ timeoutMs: 5_000 }), handleB.result({ timeoutMs: 5_000 })]),
    );

    expect(resultA).toEqual({ value: 1 });
    expect(resultB).toEqual({ value: 2 });
    expect(executionCount).toBe(2);
  });

  it("completed workflow returns cached result within TTL", async () => {
    const { clock, runner } = setup();
    let executionCount = 0;

    const wf = workflow({ name: "cached" })
      .stepAsync("compute", async () => {
        executionCount++;
        return { run: executionCount };
      })
      .build({ idempotency });

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-5", input: {} });
    await drive(clock, h1.result({ timeoutMs: 5_000 }));
    expect(executionCount).toBe(1);

    // Within TTL — should return cached, not re-execute
    const h2 = await runner.start({ workflow: wf, workflowId: "sf-5", input: {} });
    const status = await h2.status();
    expect(status?.state).toBe("completed");
    expect(status?.result).toEqual({ run: 1 });
    expect(executionCount).toBe(1);
  });

  it("suspended workflow is joined, not re-started", async () => {
    const { storage, runner } = setup();
    let executionCount = 0;

    const wf = workflow({ name: "suspended-test" })
      .stepAsync("first", async () => {
        executionCount++;
        return { step: 1 };
      })
      .waitForSignal("approval", { signalName: "approve", timeoutMs: 60_000 })
      .build({ idempotency });

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-6", input: {} });
    await waitFor(async () => (await storage.loadWorkflow("sf-6"))?.status === "suspended");

    const h2 = await runner.start({ workflow: wf, workflowId: "sf-6", input: {} });
    expect(h1.workflowId).toBe("sf-6");
    expect(h2.workflowId).toBe("sf-6");
    expect(executionCount).toBe(1);

    const status = await h2.status();
    expect(status?.state).toBe("suspended");
  });

  it("concurrent start() with same ID should execute only once", async () => {
    const { clock, runner } = setup();
    const release = gate();
    let executionCount = 0;

    const wf = workflow({ name: "race-test" })
      .stepAsync("compute", async () => {
        executionCount++;
        await release.promise;
        return { value: 42 };
      })
      .build({ idempotency });

    // Fire both start() concurrently — no await between them
    const [handleA, handleB] = await Promise.all([
      runner.start({ workflow: wf, workflowId: "race-1", input: {} }),
      runner.start({ workflow: wf, workflowId: "race-1", input: {} }),
    ]);

    release.open();
    const [resultA, resultB] = await drive(
      clock,
      Promise.all([handleA.result({ timeoutMs: 5_000 }), handleB.result({ timeoutMs: 5_000 })]),
    );

    expect(resultA).toEqual({ value: 42 });
    expect(resultB).toEqual({ value: 42 });
    // Only one of the two concurrent starts takes the run's lock.
    expect(executionCount).toBe(1);
  });

  it("without idempotency, start() throws on in-flight workflow", async () => {
    const { clock, runner } = setup();
    const slow = gate();

    const wf = workflow({ name: "no-idempotency" })
      .stepAsync("slow", async () => {
        await slow.promise;
        return { done: true };
      })
      .build(); // no idempotency config

    const h1 = await runner.start({ workflow: wf, workflowId: "sf-7", input: {} });

    // Second start while the first still holds the run — rejected.
    await expect(
      runner.start({ workflow: wf, workflowId: "sf-7", input: {} }),
    ).rejects.toMatchObject({ _tag: "WorkflowLockError" });

    slow.open();
    expect(await drive(clock, h1.result({ timeoutMs: 5_000 }))).toEqual({ done: true });
  });

  // -------------------------------------------------------------------------
  // deriveId — workflowId derived from input at the call site
  // -------------------------------------------------------------------------
  //
  // The runner takes an explicit `workflowId`; callers derive it at the call
  // site (deriveId is just a user-side helper).

  it("deriveId generates workflowId from input", async () => {
    const { clock, runner } = setup();

    const deriveId = (input: { userId: string; orderId: string }) =>
      `order:${input.userId}:${input.orderId}`;
    const wf = workflow({ name: "derive" })
      .stepAsync("compute", async () => ({ done: true }))
      .build({ idempotency });

    const input = { userId: "u1", orderId: "o5" };
    const handle = await runner.start({ workflow: wf, workflowId: deriveId(input), input });
    expect(handle.workflowId).toBe("order:u1:o5");

    const result = await drive(clock, handle.result({ timeoutMs: 5_000 }));
    expect(result).toEqual({ done: true });
  });

  it("deriveId deduplicates same logical request", async () => {
    const { clock, runner } = setup();
    const release = gate();
    let executions = 0;

    const deriveId = (input: { id: string }) => `key:${input.id}`;
    const wf = workflow({ name: "derive-dedup" })
      .stepAsync("compute", async () => {
        executions++;
        await release.promise;
        return { value: 42 };
      })
      .build({ idempotency });

    const input = { id: "abc" };
    const h1 = await runner.start({ workflow: wf, workflowId: deriveId(input), input });
    const h2 = await runner.start({ workflow: wf, workflowId: deriveId(input), input });

    expect(h1.workflowId).toBe("key:abc");
    expect(h2.workflowId).toBe("key:abc");

    release.open();
    const [r1, r2] = await drive(
      clock,
      Promise.all([h1.result({ timeoutMs: 5_000 }), h2.result({ timeoutMs: 5_000 })]),
    );

    expect(r1).toEqual({ value: 42 });
    expect(r2).toEqual({ value: 42 });
    expect(executions).toBe(1);
  });
});
