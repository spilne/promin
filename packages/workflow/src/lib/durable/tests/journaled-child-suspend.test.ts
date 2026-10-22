// ---------------------------------------------------------------------------
// ctx.child whose child workflow suspends.
//
// The child's suspension is not an outcome: the parent's child entry stays
// pending (never a Failure), the parent step parks on the child until the
// child's own wake time, and the next parent run resumes the child and
// returns its result. (A child that ends wakes the parent itself: see
// child-wake.test.ts.)
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import { childEndedSignalName } from "../child-wake.ts";

describe("ctx.child — child suspends", () => {
  it("keeps the child entry pending, suspends the parent until the child wakes, then resumes", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    let childNaps = 0;
    let after = 0;

    const napper = workflow<{ v: number }>({ name: "napper" })
      .journaled("nap", function* (ctx, input) {
        yield* ctx.activity("before-nap", async () => ++childNaps);
        yield* ctx.sleep(60_000);
        return input.v * 2;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "napper-parent" })
      .journaled("run", function* (ctx, input) {
        const doubled = yield* ctx.child(napper, { input: { v: input.v }, workflowId: "kid-1" });
        const next = yield* ctx.activity("after", async () => ++after);
        return { doubled, next };
      })
      .build();

    const runner = createWorkflowRunner({ storage, clock });
    // The first run parks; the stored state below is what matters.
    await runner
      .run({ workflow: parent, workflowId: "par-s", input: { v: 21 } })
      .catch(() => undefined);

    const journal = await storage.loadJournal("par-s", "run");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.stepType).toBe("child");
    expect(journal[0]!.phase).toBe("pending");
    expect(journal[0]!.exit).toBeUndefined();

    const parentState = await storage.loadWorkflow("par-s");
    expect(parentState?.status).toBe("suspended");
    // Parked on the child: a wait that times out at the child's wake time
    // and ends when the child does.
    expect(parentState?.steps.run?.status).toBe("waiting_for_signal");
    expect(parentState?.steps.run?.signalName).toBe(
      childEndedSignalName({ childWorkflowId: "kid-1", run: 1 }),
    );
    expect(parentState?.steps.run?.signalTimeoutAt?.toISOString()).toBe("2026-01-01T00:01:00.000Z");

    clock.advance(60_000);
    const result = await runner.run({ workflow: parent, workflowId: "par-s", input: { v: 21 } });
    expect(result).toEqual({ doubled: 42, next: 1 });
    expect(childNaps).toBe(1);
    expect((await storage.loadWorkflow("kid-1"))?.status).toBe("completed");
    const done = await storage.loadJournal("par-s", "run");
    expect(done[0]!.exit).toEqual({ tag: "Success", value: 42 });
  });
});
