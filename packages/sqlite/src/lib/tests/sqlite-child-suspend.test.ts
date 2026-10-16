// A journaled ctx.child whose child workflow suspends, on the SQLite store:
// the parent's child entry stays pending, the parent sleeps until the child
// wakes, and the next parent run resumes the child.

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock, createWorkflowRunner, workflow } from "@promin/workflow";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

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
    expect(parentState?.steps.run?.wakeAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    clock.advance(60_000);
    expect(await runner.run({ workflow: parent, workflowId: "par-sql", input: { v: 4 } })).toBe(8);
    expect(childRuns).toBe(1);
    expect((await storage.loadJournal("par-sql", "run"))[0]!.exit).toEqual({
      tag: "Success",
      value: 8,
    });
  });
});
