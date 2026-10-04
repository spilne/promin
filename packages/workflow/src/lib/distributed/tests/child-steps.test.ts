// ---------------------------------------------------------------------------
// `.subworkflow()` steps under the distributed runner: they have their own
// step kind (`child`) and run on the coordinator, so no worker has to host
// the subworkflow step; the child's own ordinary steps go to the queue.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { DistributedWorkflowRunner } from "../coordinator.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { createWorker } from "../worker.ts";
import { buildStubWorkflow } from "../stub-workflow.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { createSleepScanner } from "../sleep-scanner.ts";
import { InMemoryWorkflowStorage } from "../../durable/in-memory-storage.ts";
import { workflow } from "../../durable/durable-pipeline.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000 && !(await predicate()); i++) {
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

const child = workflow<{ id: number }>({ name: "enrich-child" })
  .step("lookup", ({ input }) => succeed(input.id * 100))
  .build();

const parent = workflow<{ n: number }>({ name: "enrich-parent" })
  .step("load", ({ input }) => succeed(input.n))
  .subworkflow("enrich", child, {
    input: (prev) => ({ id: prev }),
    workflowId: (prev) => `enrich-${prev}`,
  })
  .step("after", ({ prev }) => succeed((prev as number) + 1))
  .build();

describe("subworkflow steps under the distributed runner", () => {
  it("are built with the `child` step kind", () => {
    expect(parent._definition.steps.map((s) => [s.name, s.kind])).toEqual([
      ["load", "normal"],
      ["enrich", "child"],
      ["after", "normal"],
    ]);
    expect(parent.dag.steps.find((s) => s.name === "enrich")?.kind).toBe("child");
  });

  it("run on the coordinator: the run completes without any worker hosting the subworkflow step", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 10,
      clock,
    });
    // Hosts the parent's and the child's ordinary steps — not "enrich".
    const registry = new MapStepRegistry();
    registry.register("load", (ctx) => succeed((ctx.input as { n: number }).n));
    registry.register("lookup", (ctx) => succeed((ctx.input as { id: number }).id * 100));
    registry.register("after", (ctx) => succeed((ctx.deps["enrich"] as number) + 1));
    const worker = createWorker({ storage, stepQueue: queue, registry, pollIntervalMs: 10, clock });
    void worker.start();

    await runner.submit({ workflow: parent, workflowId: "p", input: { n: 3 } });
    await waitFor(async () => {
      if (clock.pendingCount() > 0) clock.advance(10);
      return (await storage.loadWorkflow("p"))?.status === "completed";
    });

    const state = await storage.loadWorkflow("p");
    expect(state?.result).toBe(301);
    expect(state?.steps["enrich"]?.status).toBe("completed");
    const childState = await storage.loadWorkflow("enrich-3");
    expect(childState?.status).toBe("completed");
    expect(childState?.parentWorkflowId).toBe("p");
    // The subworkflow step never went to the queue; the child's step did.
    expect(queue.getAllTasks().map((t) => [t.workflowId, t.stepName])).toEqual([
      ["p", "load"],
      ["enrich-3", "lookup"],
      ["p", "after"],
    ]);

    await worker.stop();
  });

  it("a stub workflow's child step fails with an error naming the missing definition", async () => {
    const stub = buildStubWorkflow(parent.dag, parent.name);
    const step = stub._definition.steps.find((s) => s.name === "enrich")!;
    expect(step.kind).toBe("child");
    const storage = new InMemoryWorkflowStorage();
    const exit = await step
      .execute({
        input: { n: 1 },
        results: { load: 1 },
        workflowId: "p",
        storage,
        attemptRef: { current: 1 },
        metadataRef: { current: undefined },
      })
      .runExit();
    expect(exit).toMatchObject({ _tag: "Failure", cause: { _tag: "Die" } });
    const cause = (exit as unknown as { cause: { defect: unknown } }).cause;
    expect(String(cause.defect)).toContain(
      "runs a child workflow, which only the real definition knows",
    );
  });

  it("a child that waits on a signal without a deadline wakes its parked parent when it completes", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new InMemoryWorkflowStorage({ clock });
    const queue = new InMemoryStepQueue({ clock });
    const runner = new DistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      stepPollIntervalMs: 10,
      clock,
    });
    const gated = workflow<{ id: number }>({ name: "gated-child" })
      .waitForSignal<number>("wait", { signalName: "go" })
      .step("scale", ({ prev }) => succeed((prev as number) * 10))
      .build();
    const gatedParent = workflow<{ n: number }>({ name: "gated-parent" })
      .step("load", ({ input }) => succeed(input.n))
      .subworkflow("enrich", gated, {
        input: (prev) => ({ id: prev }),
        workflowId: (prev) => `gated-${prev}`,
      })
      .step("after", ({ prev }) => succeed((prev as number) + 1))
      .build();
    const registry = new MapStepRegistry();
    registry.register("load", (ctx) => succeed((ctx.input as { n: number }).n));
    registry.register("scale", (ctx) => succeed((ctx.deps["wait"] as number) * 10));
    registry.register("after", (ctx) => succeed((ctx.deps["enrich"] as number) + 1));
    const worker = createWorker({ storage, stepQueue: queue, registry, pollIntervalMs: 10, clock });
    void worker.start();

    await runner.submit({ workflow: gatedParent, workflowId: "gp", input: { n: 3 } });
    await waitFor(async () => {
      if (clock.pendingCount() > 0) clock.advance(10);
      return (await storage.loadWorkflow("gp"))?.status === "suspended";
    });
    const parked = await storage.loadWorkflow("gp");
    expect(parked?.steps["enrich"]?.status).toBe("waiting_for_signal");
    expect(parked?.steps["enrich"]?.signalTimeoutAt).toBeUndefined();

    const definitions = new Map<string, unknown>([
      [gated.name, gated],
      [gatedParent.name, gatedParent],
    ]);
    const resolveWorkflow = (name: string) => definitions.get(name) as never;
    const sleepScanner = createSleepScanner({
      storage,
      runner,
      scanIntervalMs: 1_000,
      resolveWorkflow,
      clock,
    });
    const signalScanner = createSignalScanner({
      storage,
      runner,
      scanIntervalMs: 1_000,
      resolveWorkflow,
      clock,
    });
    void sleepScanner.start();
    void signalScanner.start();

    await storage.deliverSignal("gated-3", "go", 7);
    await waitFor(async () => {
      if (clock.pendingCount() > 0) clock.advance(10);
      return (await storage.loadWorkflow("gp"))?.status === "completed";
    });
    expect((await storage.loadWorkflow("gated-3"))?.status).toBe("completed");
    expect((await storage.loadWorkflow("gp"))?.result).toBe(71);

    await sleepScanner.stop();
    await signalScanner.stop();
    await worker.stop();
  });
});
