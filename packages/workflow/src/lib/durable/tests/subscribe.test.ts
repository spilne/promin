import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import type { WorkflowRunEvent } from "../workflow-state.ts";

class StepBoom extends Data.TaggedError("StepBoom")<{ readonly message: string }> {}

async function collect(
  it: AsyncIterable<WorkflowRunEvent>,
  maxMs = 2000,
): Promise<WorkflowRunEvent[]> {
  const events: WorkflowRunEvent[] = [];
  const deadline = Date.now() + maxMs;
  for await (const ev of it) {
    events.push(ev);
    if (Date.now() > deadline) break;
  }
  return events;
}

describe("subscribe — run-scoped event stream", () => {
  it("emits step-started before each step body runs", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-started" })
      .step("a", ({ input }) => Pipeline.succeed(input + 1))
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();

    const eventsP = collect(runner.subscribe("sub-started-1"));
    await runner.run({ workflow: wf, workflowId: "sub-started-1", input: 5 });
    const events = await eventsP;

    const started = events.filter(
      (e): e is Extract<WorkflowRunEvent, { type: "step-started" }> => e.type === "step-started",
    );
    expect(started.map((e) => e.stepName)).toEqual(["a", "b"]);
    // step-started must precede step-completed for the same step.
    const aStart = events.findIndex((e) => e.type === "step-started" && e.stepName === "a");
    const aDone = events.findIndex((e) => e.type === "step-completed" && e.stepName === "a");
    expect(aStart).toBeGreaterThanOrEqual(0);
    expect(aDone).toBeGreaterThan(aStart);
  });

  it("emits step-completed and workflow-completed events for a successful run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-ok" })
      .step("a", ({ input }) => Pipeline.succeed(input + 1))
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();

    const eventsP = collect(runner.subscribe("sub-ok-1"));

    await runner.run({ workflow: wf, workflowId: "sub-ok-1", input: 5 });
    const events = await eventsP;

    const types = events.map((e) => e.type);
    // With step-started firing, each step contributes started + completed.
    expect(types).toEqual([
      "step-started",
      "step-completed",
      "step-started",
      "step-completed",
      "workflow-completed",
    ]);

    const stepEvents = events.filter(
      (e): e is Extract<WorkflowRunEvent, { type: "step-completed" }> =>
        e.type === "step-completed",
    );
    expect(stepEvents.map((e) => e.stepName)).toEqual(["a", "b"]);
    expect(stepEvents[0]!.result).toBe(6);
    expect(stepEvents[1]!.result).toBe(12);

    const final = events[events.length - 1]!;
    expect(final.type).toBe("workflow-completed");
  });

  it("emits step-failed and workflow-failed on failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-fail" })
      .step("boom", () => Pipeline.fail(new StepBoom({ message: "bang" })))
      .build();

    const eventsP = collect(runner.subscribe("sub-fail-1"));
    await runner.runSafe({ workflow: wf, workflowId: "sub-fail-1", input: 0 });
    const events = await eventsP;

    expect(events.map((e) => e.type)).toContain("step-failed");
    expect(events.map((e) => e.type)).toContain("workflow-failed");
    expect(events[events.length - 1]!.type).toBe("workflow-failed");
  });

  it("emits workflow-failed and closes on cancellation", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    await storage.createWorkflow({
      workflowId: "sub-cancel-1",
      workflowName: "sub-cancel",
      input: {},
    });

    const eventsP = collect(runner.subscribe("sub-cancel-1"));
    await runner.handle("sub-cancel-1").cancel();
    const events = await eventsP;

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ type: "workflow-failed", error: "Cancelled" });
  });

  it("emits workflow-tripwire on tripwire exit", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-trip" })
      .tripwire("gate", {
        when: (x) => x > 0,
        reason: (x) => ({ value: x }),
      })
      .build();

    const eventsP = collect(runner.subscribe("sub-trip-1"));
    await runner.runSafe({ workflow: wf, workflowId: "sub-trip-1", input: 7 });
    const events = await eventsP;

    const trip = events.find(
      (e): e is Extract<WorkflowRunEvent, { type: "workflow-tripwire" }> =>
        e.type === "workflow-tripwire",
    );
    expect(trip).toBeDefined();
    expect(trip!.stepName).toBe("gate");
    expect(trip!.reason).toEqual({ value: 7 });
    expect(events[events.length - 1]!.type).toBe("workflow-tripwire");
  });

  it("stream closes after terminal event", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-close" })
      .step("x", ({ input }) => Pipeline.succeed(input))
      .build();

    const it = runner.subscribe("sub-close-1")[Symbol.asyncIterator]();
    const p1 = it.next();
    const p2 = it.next();
    const p3 = it.next();

    await runner.run({ workflow: wf, workflowId: "sub-close-1", input: 1 });

    // Single step produces three events: step-started, step-completed,
    // workflow-completed. All three in-flight next() calls resolve with
    // their event; the fourth sees done=true.
    const r1 = await p1;
    const r2 = await p2;
    const r3 = await p3;
    expect(r1.done).toBe(false);
    expect(r2.done).toBe(false);
    expect(r3.done).toBe(false);

    const r4 = await it.next();
    expect(r4.done).toBe(true);
  });

  it("multiple concurrent subscribers each see every event", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-multi" })
      .step("a", ({ input }) => Pipeline.succeed(input))
      .step("b", ({ prev }) => Pipeline.succeed(prev + 1))
      .build();

    const a = collect(runner.subscribe("sub-multi-1"));
    const b = collect(runner.subscribe("sub-multi-1"));

    await runner.run({ workflow: wf, workflowId: "sub-multi-1", input: 10 });

    const [evA, evB] = await Promise.all([a, b]);
    // Two steps × (started + completed) + workflow-completed = 5.
    expect(evA.length).toBe(5);
    expect(evB.length).toBe(5);
    expect(evA.map((e) => e.type)).toEqual(evB.map((e) => e.type));
  });

  it("abort signal terminates the stream", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const ctl = new AbortController();
    const it = runner.subscribe("sub-abort-1", { signal: ctl.signal })[Symbol.asyncIterator]();

    const p = it.next();
    ctl.abort();
    const r = await p;
    expect(r.done).toBe(true);
  });

  it("subscriber that attaches mid-run sees from-now events only", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-late" })
      .stepAsync("a", async ({ input }) => {
        // yield control so the subscribe call that happens after run() starts
        // can attach before step "b" completes
        await new Promise((r) => setTimeout(r, 0));
        return input + 1;
      })
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();

    const runP = runner.run({ workflow: wf, workflowId: "sub-late-1", input: 5 });
    // Attach AFTER the run has started. We can't deterministically land in the
    // middle, so we simply assert the subscriber receives the terminal event
    // even when it may have missed some earlier events.
    const eventsP = collect(runner.subscribe("sub-late-1"));

    await runP;
    const events = await eventsP;
    expect(events[events.length - 1]!.type).toBe("workflow-completed");
  });

  it("falls back to polling when storage has no native subscribe", async () => {
    const storage = new InMemoryWorkflowStorage();
    // Shadow the native push path so the runner picks the polling branch.
    (storage as unknown as { subscribeToWorkflow: unknown }).subscribeToWorkflow = undefined;
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-poll" })
      .step("a", ({ input }) => Pipeline.succeed(input + 1))
      .step("b", ({ prev }) => Pipeline.succeed(prev * 2))
      .build();

    // Fast poll so the test finishes quickly but still goes through the
    // polling code path.
    const eventsP = collect(runner.subscribe("sub-poll-1", { pollIntervalMs: 20 }));
    await runner.run({ workflow: wf, workflowId: "sub-poll-1", input: 5 });
    const events = await eventsP;

    // Polling diffs snapshots so all transitions must still surface — the
    // exact ordering between step-completed and workflow-completed depends
    // on whether they land in the same tick, but every terminal run ends
    // with the workflow-completed event.
    expect(events.map((e) => e.type)).toContain("step-completed");
    expect(events[events.length - 1]!.type).toBe("workflow-completed");
  });

  it("polling fallback surfaces workflow-failed", async () => {
    const storage = new InMemoryWorkflowStorage();
    (storage as unknown as { subscribeToWorkflow: unknown }).subscribeToWorkflow = undefined;
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sub-poll-fail" })
      .step("boom", () => Pipeline.fail(new StepBoom({ message: "x" })))
      .build();

    const eventsP = collect(runner.subscribe("sub-poll-fail-1", { pollIntervalMs: 20 }));
    await runner.runSafe({ workflow: wf, workflowId: "sub-poll-fail-1", input: 0 });
    const events = await eventsP;

    expect(events[events.length - 1]!.type).toBe("workflow-failed");
  });
});
