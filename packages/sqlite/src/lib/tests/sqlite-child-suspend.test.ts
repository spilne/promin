// A journaled ctx.child whose child workflow suspends, on the SQLite store:
// the parent's child entry stays pending, the parent parks on the child
// until the child wakes, and the next parent run resumes the child. A child
// that ends wakes its parked parent through the scanners.

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import {
  FakeWallClock,
  childEndedSignalName,
  createSignalScanner,
  createSleepScanner,
  createWorkflowRunner,
  workflow,
} from "@promin/workflow";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

/** Yield to the event loop until `predicate` holds (bounded). */
async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  for (let i = 0; i < 2_000; i++) {
    if (await predicate()) return;
    await new Promise<void>((r) => setImmediate(r));
  }
  expect(await predicate()).toBe(true);
}

describe("SqliteWorkflowStorage — ctx.child with a suspending child", () => {
  it("resumes the parent with the child's result after the child wakes", async () => {
    const storage = SqliteWorkflowStorage.make({ db: new Database(":memory:") });
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    let childRuns = 0;

    const napper = workflow<{ v: number }>({ name: "napper" })
      .journaled("nap", function* (ctx, input) {
        yield* ctx.activity("before-nap", async () => ++childRuns);
        yield* ctx.sleep(60_000);
        return input.v * 2;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "napper-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child(napper, { input: { v: input.v }, workflowId: "kid-sql" });
      })
      .build();

    const runner = createWorkflowRunner({ storage, clock });
    await runner
      .run({ workflow: parent, workflowId: "par-sql", input: { v: 4 } })
      .catch(() => undefined);

    const [entry] = await storage.loadJournal("par-sql", "run");
    expect(entry!.stepType).toBe("child");
    expect(entry!.phase).toBe("pending");
    const parentState = await storage.loadWorkflow("par-sql");
    expect(parentState?.status).toBe("suspended");
    expect(parentState?.steps.run?.signalTimeoutAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    clock.advance(60_000);
    expect(await runner.run({ workflow: parent, workflowId: "par-sql", input: { v: 4 } })).toBe(8);
    expect(childRuns).toBe(1);
    expect((await storage.loadJournal("par-sql", "run"))[0]!.exit).toEqual({
      tag: "Success",
      value: 8,
    });
  });

  it("wakes the parent when a child waiting on a signal without a deadline completes", async () => {
    const storage = SqliteWorkflowStorage.make({ db: new Database(":memory:") });
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");

    const approval = workflow<{ v: number }>({ name: "approval" })
      .journaled("await", function* (ctx, input) {
        const approved = yield* ctx.signal<boolean>("approve");
        return approved ? input.v * 2 : 0;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "approval-parent" })
      .journaled("run", function* (ctx, input) {
        const doubled = yield* ctx.child(approval, { input: { v: input.v }, workflowId: "kid" });
        return doubled + 1;
      })
      .build();
    const definitions: Record<string, unknown> = { approval, "approval-parent": parent };
    const resolveWorkflow = (name: string) => definitions[name] as never;

    const runner = createWorkflowRunner({ storage, clock });
    await runner
      .run({ workflow: parent, workflowId: "par", input: { v: 20 } })
      .catch(() => undefined);
    const parked = await storage.loadWorkflow("par");
    expect(parked?.status).toBe("suspended");
    expect(parked?.steps.run?.signalName).toBe(
      childEndedSignalName({ childWorkflowId: "kid", run: 1 }),
    );
    expect(parked?.steps.run?.signalTimeoutAt).toBeUndefined();

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

    await storage.deliverSignal("kid", "approve", true);
    await waitFor(async () => {
      if (clock.pendingCount() > 0) clock.advance(1_000);
      return (await storage.loadWorkflow("par"))?.status === "completed";
    });
    expect((await storage.loadWorkflow("kid"))?.status).toBe("completed");
    expect((await storage.loadWorkflow("par"))?.result).toBe(41);
    expect((await storage.loadJournal("par", "run"))[0]!.exit).toEqual({
      tag: "Success",
      value: 40,
    });

    await sleepScanner.stop();
    await signalScanner.stop();
  });
});
