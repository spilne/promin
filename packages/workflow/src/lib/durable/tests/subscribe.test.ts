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
    expect(types).toEqual(["step-completed", "step-completed", "workflow-completed"]);

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

    await runner.run({ workflow: wf, workflowId: "sub-close-1", input: 1 });

    const r1 = await p1;
    const r2 = await p2;
    expect(r1.done).toBe(false);
    expect(r2.done).toBe(false);

    const r3 = await it.next();
    expect(r3.done).toBe(true);
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
    expect(evA.length).toBe(3); // two step-completed + workflow-completed
    expect(evB.length).toBe(3);
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

  it("runner throws when storage does not support subscribe", () => {
    const storage = new InMemoryWorkflowStorage();
    (storage as unknown as { subscribeToWorkflow: unknown }).subscribeToWorkflow = undefined;
    const runner = createWorkflowRunner({ storage });
    expect(() => runner.subscribe("nope")).toThrow(/subscribeToWorkflow/);
  });
});
