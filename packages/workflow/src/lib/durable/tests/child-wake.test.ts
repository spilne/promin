// ---------------------------------------------------------------------------
// A child workflow that ends wakes the parent parked on it.
//
// The child here waits on a signal without a deadline, so the parent has no
// wake time of its own: only the child's end can wake it. Delivering the
// child's signal lets the signal scanner finish the child, whose runner then
// wakes the parent, and the scanners resume the parent to completion with
// no manual resume.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner, type WorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { createSignalScanner } from "../../distributed/signal-scanner.ts";
import { createSleepScanner } from "../../distributed/sleep-scanner.ts";
import { childEndedSignalName } from "../child-wake.ts";
import type { Workflow } from "../workflow-types.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

/** Both scanners over `storage`, resolving `definitions` by name. */
function startScanners(params: {
  storage: InMemoryWorkflowStorage;
  runner: WorkflowRunner;
  clock: FakeWallClock;
  definitions: readonly Workflow<never, unknown>[];
}): { stop: () => Promise<void> } {
  const { storage, runner, clock } = params;
  const byName = new Map(params.definitions.map((d) => [d.name, d]));
  const resolveWorkflow = (name: string) => byName.get(name) as Workflow<unknown, unknown>;
  const sleep = createSleepScanner({
    storage,
    runner,
    scanIntervalMs: 1_000,
    resolveWorkflow,
    clock,
  });
  const signal = createSignalScanner({
    storage,
    runner,
    scanIntervalMs: 1_000,
    resolveWorkflow,
    clock,
  });
  void sleep.start();
  void signal.start();
  return {
    stop: async () => {
      await sleep.stop();
      await signal.stop();
    },
  };
}

/** Advance scan intervals until run `workflowId` reaches `status`. */
async function driveUntil(params: {
  storage: InMemoryWorkflowStorage;
  clock: FakeWallClock;
  workflowId: string;
  status: string;
}): Promise<void> {
  const { storage, clock, workflowId, status } = params;
  await waitFor(async () => {
    if (clock.pendingCount() > 0) clock.advance(1_000);
    return (await storage.loadWorkflow(workflowId))?.status === status;
  });
}

const approval = workflow<{ v: number }>({ name: "approval" })
  .journaled("await", function* (ctx, input) {
    const approved = yield* ctx.signal<boolean>("approve");
    return approved ? input.v * 2 : 0;
  })
  .build();

describe("a child that ends wakes its parked parent", () => {
  it("journaled ctx.child: the parent resumes and journals the child's result", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    let afterRuns = 0;
    const parent = workflow<{ v: number }>({ name: "approval-parent" })
      .journaled("run", function* (ctx, input) {
        const doubled = yield* ctx.child(approval, { input: { v: input.v }, workflowId: "kid" });
        yield* ctx.activity("after", async () => ++afterRuns);
        return doubled + 1;
      })
      .build();
    const runner = createWorkflowRunner({ storage, clock });

    await runner
      .run({ workflow: parent, workflowId: "par", input: { v: 20 } })
      .catch(() => undefined);
    const parked = await storage.loadWorkflow("par");
    expect(parked?.status).toBe("suspended");
    expect(parked?.steps.run?.status).toBe("waiting_for_signal");
    expect(parked?.steps.run?.signalName).toBe(
      childEndedSignalName({ childWorkflowId: "kid", run: 1 }),
    );
    // Nothing to time out on: only the child's end wakes the parent.
    expect(parked?.steps.run?.signalTimeoutAt).toBeUndefined();

    const scanners = startScanners({
      storage,
      runner,
      clock,
      definitions: [approval, parent] as never,
    });
    await storage.deliverSignal({ workflowId: "kid", signalName: "approve", payload: true });
    await driveUntil({ storage, clock, workflowId: "par", status: "completed" });

    expect((await storage.loadWorkflow("kid"))?.status).toBe("completed");
    expect((await storage.loadWorkflow("par"))?.result).toBe(41);
    const journal = await storage.loadJournal({ workflowId: "par", stepName: "run" });
    expect(journal[0]!.exit).toEqual({ tag: "Success", value: 40 });
    expect(afterRuns).toBe(1);
    await scanners.stop();
  });

  it(".subworkflow(): the parent parks on the child and completes once the child does", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const child = workflow<{ id: number }>({ name: "gated-child" })
      .waitForSignal<number>("wait", { signalName: "go" })
      .step("after", ({ prev }) => succeed((prev as number) * 10))
      .build();
    const parent = workflow<{ n: number }>({ name: "gated-parent" })
      .step("load", ({ input }) => succeed(input.n))
      .subworkflow("enrich", child, {
        input: (prev) => ({ id: prev }),
        workflowId: (prev) => `enrich-${prev}`,
      })
      .step("done", ({ prev }) => succeed((prev as number) + 1))
      .build();
    const runner = createWorkflowRunner({ storage, clock });

    await runner.run({ workflow: parent, workflowId: "p", input: { n: 3 } }).catch(() => undefined);
    const parked = await storage.loadWorkflow("p");
    expect(parked?.status).toBe("suspended");
    expect(parked?.steps.enrich?.status).toBe("waiting_for_signal");
    expect(parked?.steps.enrich?.signalName).toBe(
      childEndedSignalName({ childWorkflowId: "enrich-3", run: 1 }),
    );

    const scanners = startScanners({
      storage,
      runner,
      clock,
      definitions: [child, parent] as never,
    });
    await storage.deliverSignal({ workflowId: "enrich-3", signalName: "go", payload: 7 });
    await driveUntil({ storage, clock, workflowId: "p", status: "completed" });

    expect((await storage.loadWorkflow("enrich-3"))?.status).toBe("completed");
    const done = await storage.loadWorkflow("p");
    expect(done?.steps.enrich?.status).toBe("completed");
    expect(done?.result).toBe(71);
    await scanners.stop();
  });

  it("a cancelled child wakes the parent, whose ctx.child then fails", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const parent = workflow<{ v: number }>({ name: "approval-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child(approval, { input: { v: input.v }, workflowId: "kid" });
      })
      .build();
    const runner = createWorkflowRunner({ storage, clock });

    await runner
      .run({ workflow: parent, workflowId: "par", input: { v: 1 } })
      .catch(() => undefined);
    expect((await storage.loadWorkflow("par"))?.status).toBe("suspended");

    const scanners = startScanners({
      storage,
      runner,
      clock,
      definitions: [approval, parent] as never,
    });
    await runner.handle("kid").cancel();
    await driveUntil({ storage, clock, workflowId: "par", status: "failed" });

    const failed = await storage.loadWorkflow("par");
    expect(failed?.steps.run?.status).toBe("failed");
    expect(failed?.error).toContain("cancel");
    await scanners.stop();
  });

  it("does not wake a parent that is not parked on the child", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const napper = workflow<{ n: number }>({ name: "napper" })
      .step("load", ({ input }) => succeed(input.n))
      .sleep("nap", 60_000)
      .build();
    const quick = workflow<{ n: number }>({ name: "quick" })
      .step("only", ({ input }) => succeed(input.n))
      .build();
    const runner = createWorkflowRunner({ storage, clock });

    await runner
      .run({ workflow: napper, workflowId: "nap-1", input: { n: 1 } })
      .catch(() => undefined);
    const before = await storage.loadWorkflow("nap-1");
    expect(before?.steps.nap?.status).toBe("sleeping");

    // A child of "nap-1" that its parent never waited on ends.
    await storage.createWorkflow({
      workflowId: "stray",
      workflowName: "quick",
      input: { n: 2 },
      parentWorkflowId: "nap-1",
    });
    expect(await runner.run({ workflow: quick, workflowId: "stray", input: { n: 2 } })).toBe(2);

    const scanners = startScanners({
      storage,
      runner,
      clock,
      definitions: [napper, quick] as never,
    });
    clock.advance(5_000);
    for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));

    const after = await storage.loadWorkflow("nap-1");
    expect(after?.status).toBe("suspended");
    expect(after?.steps.nap?.status).toBe("sleeping");
    expect(after?.steps.nap?.wakeAt).toEqual(before?.steps.nap?.wakeAt);
    await scanners.stop();
  });
});
