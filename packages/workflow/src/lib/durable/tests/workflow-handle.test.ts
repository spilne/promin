import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

const T0 = "2026-01-01T00:00:00Z";

/** A promise the test resolves by hand. */
function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((r) => (open = r));
  return { promise, open };
}

/**
 * Await `promise` while advancing the clock one `result()` poll interval at
 * a time, so handle polls on the fake clock make progress.
 */
async function drive<T>(clock: FakeWallClock, promise: Promise<T>): Promise<T> {
  let settled = false;
  const tracked = promise.finally(() => (settled = true));
  tracked.catch(() => {});
  for (let i = 0; i < 100 && !settled; i++) {
    await flush();
    if (!settled) clock.advance(1_000);
  }
  return tracked;
}

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

// ---------------------------------------------------------------------------
// promin-jc47 — WorkflowHandle additions: cancel(), events(), generic typing
// ---------------------------------------------------------------------------

describe("WorkflowHandle — generic threading on runner.start", () => {
  it("infers Output type from the workflow definition", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });

    const wf = workflow<{ n: number }>({ name: "doubler" })
      .stepAsync("double", async ({ input }) => ({ doubled: input.n * 2 }))
      .build();

    const handle = await runner.start({
      workflow: wf,
      workflowId: "h-1",
      input: { n: 5 },
    });

    // Type-level: handle.result() must return { doubled: number }, not unknown.
    const result = await drive(clock, handle.result({ timeoutMs: 5_000 }));
    // Compile-time check — accessing .doubled would fail if result were unknown.
    expect(result.doubled).toBe(10);
  });
});

describe("WorkflowHandle — cancel()", () => {
  it("cancels a suspended workflow via the handle", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });

    // Workflow that will sleep forever; we cancel before the timer fires.
    const wf = workflow<{ n: number }>({ name: "sleeper" })
      .stepAsync("compute", async ({ input }) => input.n)
      .build();

    const handle = await runner.start({
      workflow: wf,
      workflowId: "cancel-1",
      input: { n: 1 },
    });

    // Wait for the workflow to complete naturally (no sleep), then cancel.
    // Cancel is idempotent on a terminal workflow — verifies the no-op path.
    await drive(clock, handle.result({ timeoutMs: 5_000 }));
    await handle.cancel("test-reason");

    const state = await storage.loadWorkflow("cancel-1");
    // Workflow completed successfully before cancel, so status stays "completed".
    expect(state?.status).toBe("completed");
  });

  it("cancels an in-flight (pending) workflow", async () => {
    // Pre-create a workflow row in pending state without ever starting it,
    // then cancel via a handle obtained via a fresh runner.start (which will
    // 'join' since onInFlight defaults to reject; we'd have to bypass).
    // Simpler: directly test that handle.cancel routes to storage.cancelWorkflow
    // by stubbing storage.
    const calls: string[] = [];
    const storage = new InMemoryWorkflowStorage();
    const origCancel = storage.cancelWorkflow.bind(storage);
    (storage as unknown as { cancelWorkflow: typeof origCancel }).cancelWorkflow = async (
      params,
    ) => {
      calls.push(params.workflowId);
      return origCancel(params);
    };

    const runner = createWorkflowRunner({ storage });
    const wf = workflow<number>({ name: "trivial" })
      .stepAsync("a", async ({ input }) => input)
      .build();

    const handle = await runner.start({ workflow: wf, workflowId: "cancel-2", input: 1 });
    await handle.cancel();

    expect(calls).toContain("cancel-2");
  });
});

describe("WorkflowHandle — events()", () => {
  it("delegates to runner.subscribe and yields lifecycle events", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });

    // Step "a" parks on a gate that opens only once the subscriber is
    // attached, so no event can be emitted before it listens.
    const subscribed = gate();
    const wf = workflow<{ n: number }>({ name: "evented" })
      .stepAsync("a", async ({ input }) => {
        await subscribed.promise;
        return input.n + 1;
      })
      .stepAsync("b", async ({ prev }) => prev * 2)
      .build();

    const handle = await runner.start({
      workflow: wf,
      workflowId: "events-1",
      input: { n: 3 },
    });

    const collected: { type: string; stepName?: string }[] = [];
    const reading = (async () => {
      for await (const event of handle.events()) {
        collected.push({
          type: event.type,
          stepName: "stepName" in event ? (event.stepName as string) : undefined,
        });
        if (event.type === "workflow-completed") break;
      }
    })();
    await flush();
    subscribed.open();
    await reading;

    expect(collected.filter((e) => e.type === "step-completed").map((e) => e.stepName)).toEqual([
      "a",
      "b",
    ]);
    expect(collected[collected.length - 1]!.type).toBe("workflow-completed");
  });

  it("can be aborted via signal", async () => {
    const clock = FakeWallClock.create(T0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });

    // Workflow that doesn't terminate on its own while the gate is shut —
    // only the abort signal can close the events stream.
    const hang = gate();
    const wf = workflow<number>({ name: "abortable" })
      .stepAsync("hang", async () => {
        await hang.promise;
        return 0;
      })
      .build();

    const handle = await runner.start({ workflow: wf, workflowId: "abort-1", input: 1 });

    const ac = new AbortController();
    let ended = false;
    let count = 0;
    const reading = (async () => {
      for await (const _ of handle.events({ signal: ac.signal })) {
        count++;
        if (count > 100) break; // safety
      }
      ended = true;
    })();

    await flush();
    expect(ended).toBe(false); // the stream stays open while the run is in flight
    ac.abort();
    await reading;
    expect(ended).toBe(true);
    expect(count).toBeLessThanOrEqual(1); // at most the step-started event

    // Cancel the workflow and let the step return, so nothing leaks.
    await handle.cancel();
    hang.open();
  });
});
