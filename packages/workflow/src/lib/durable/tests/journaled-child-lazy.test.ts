// ---------------------------------------------------------------------------
// ctx.child with a lazily loaded child definition (`LazyChildWorkflow`).
//
// The loader runs only when the child has to run: on the first run and
// when re-attaching to a child left in flight. A journaled outcome replays
// without loading anything.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { Workflow } from "../workflow-types.ts";
import { isCancelledRun } from "../workflow-state.ts";
import { ChildWorkflowGoneError } from "../durable-pipeline-error.ts";
import { CHILD_LOAD_RETRY_MS } from "../journaled-ctx-child.ts";

describe("ctx.child — lazy definition", () => {
  it("loads the child only when it runs, never on a replay of its outcome", async () => {
    const storage = new InMemoryWorkflowStorage();
    let loads = 0;
    let childRuns = 0;
    const doubler = workflow<{ v: number }>({ name: "lazy-doubler" })
      .journaled("double", function* (ctx, input) {
        yield* ctx.activity("count", async () => ++childRuns);
        return input.v * 2;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "lazy-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child<number>(
          {
            name: "doubler",
            load: async () => {
              loads++;
              return doubler as Workflow<unknown, number>;
            },
          },
          { input: { v: input.v }, workflowId: "lazy-kid-1" },
        );
      })
      .build();

    const runner = createWorkflowRunner({ storage });
    expect(await runner.run({ workflow: parent, workflowId: "lazy-1", input: { v: 4 } })).toBe(8);
    expect(loads).toBe(1);
    expect((await storage.loadWorkflow("lazy-kid-1"))?.parentWorkflowId).toBe("lazy-1");
    const journal = await storage.loadJournal({ workflowId: "lazy-1", stepName: "run" });
    expect(journal[0]!.stepType).toBe("child");
    expect(journal[0]!.activityName).toBe("doubler");

    // Replay: the outcome is journaled, nothing is loaded or run again.
    expect(await runner.run({ workflow: parent, workflowId: "lazy-1", input: { v: 4 } })).toBe(8);
    expect(loads).toBe(1);
    expect(childRuns).toBe(1);
  });

  it("loads again to re-attach to a child still in flight, and resumes it", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    let loads = 0;
    let before = 0;
    const napper = workflow<{ v: number }>({ name: "lazy-napper" })
      .journaled("nap", function* (ctx, input) {
        yield* ctx.activity("before", async () => ++before);
        yield* ctx.sleep(60_000);
        return input.v + 1;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "lazy-napper-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child<number>(
          {
            name: "napper",
            load: async () => {
              loads++;
              return napper as Workflow<unknown, number>;
            },
          },
          { input: { v: input.v }, workflowId: "lazy-nap-kid" },
        );
      })
      .build();

    const runner = createWorkflowRunner({ storage, clock });
    await runner
      .run({ workflow: parent, workflowId: "lazy-nap", input: { v: 1 } })
      .catch(() => undefined);
    expect((await storage.loadWorkflow("lazy-nap"))?.status).toBe("suspended");
    expect(loads).toBe(1);

    clock.advance(60_000);
    expect(await runner.run({ workflow: parent, workflowId: "lazy-nap", input: { v: 1 } })).toBe(2);
    expect(loads).toBe(2);
    expect(before).toBe(1);
  });

  it("cancels the child's run when its definition can no longer be loaded on re-attach", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    let loadable = true;
    const napper = workflow<{ v: number }>({ name: "gone-napper" })
      .journaled("nap", function* (ctx, input) {
        yield* ctx.sleep(60_000);
        return input.v;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "gone-parent" })
      .journaled("run", function* (ctx, input) {
        try {
          return yield* ctx.child<number>(
            {
              name: "napper",
              load: async () => {
                if (!loadable) throw new ChildWorkflowGoneError({ message: "definition gone" });
                return napper as Workflow<unknown, number>;
              },
            },
            { input: { v: input.v }, workflowId: "gone-kid" },
          );
        } catch (err) {
          if ((err as { _tag?: string })._tag === "WorkflowSuspendedError") throw err;
          return `failed: ${(err as Error).message}`;
        }
      })
      .build();

    const runner = createWorkflowRunner({ storage, clock });
    await runner
      .run({ workflow: parent, workflowId: "gone-par", input: { v: 1 } })
      .catch(() => undefined);
    expect((await storage.loadWorkflow("gone-kid"))?.status).toBe("suspended");

    loadable = false;
    clock.advance(60_000);
    expect(await runner.run({ workflow: parent, workflowId: "gone-par", input: { v: 1 } })).toBe(
      "failed: definition gone",
    );
    expect(isCancelledRun((await storage.loadWorkflow("gone-kid"))!)).toBe(true);
  });

  it("leaves the child alone on a transient load error and re-attaches on a later run", async () => {
    const storage = new InMemoryWorkflowStorage();
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    let failing = false;
    let before = 0;
    const napper = workflow<{ v: number }>({ name: "flaky-napper" })
      .journaled("nap", function* (ctx, input) {
        yield* ctx.activity("before", async () => ++before);
        yield* ctx.sleep(60_000);
        return input.v * 10;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "flaky-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child<number>(
          {
            name: "napper",
            load: async () => {
              if (failing) throw new Error("ECONNRESET");
              return napper as Workflow<unknown, number>;
            },
          },
          { input: { v: input.v }, workflowId: "flaky-kid" },
        );
      })
      .build();

    const runner = createWorkflowRunner({ storage, clock });
    await runner
      .run({ workflow: parent, workflowId: "flaky-par", input: { v: 2 } })
      .catch(() => undefined);

    // The child is due; the store is down when the parent comes back for it.
    failing = true;
    clock.advance(60_000);
    await expect(
      runner.run({ workflow: parent, workflowId: "flaky-par", input: { v: 2 } }),
    ).rejects.toThrow("ECONNRESET");
    expect((await storage.loadWorkflow("flaky-kid"))?.status).toBe("suspended");
    expect((await storage.loadWorkflow("flaky-par"))?.status).toBe("suspended");
    const journal = await storage.loadJournal({ workflowId: "flaky-par", stepName: "run" });
    expect(journal[0]!.phase).toBe("pending");

    failing = false;
    clock.advance(CHILD_LOAD_RETRY_MS);
    expect(await runner.run({ workflow: parent, workflowId: "flaky-par", input: { v: 2 } })).toBe(
      20,
    );
    expect(before).toBe(1);
  });

  it("stores a child under its parent's namespace", async () => {
    const storage = new InMemoryWorkflowStorage();
    const kid = workflow<{ v: number }>({ name: "ns-kid" })
      .journaled("k", function* (_ctx, input) {
        return input.v;
      })
      .build();
    const parent = workflow<{ v: number }>({ name: "ns-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child<number>(kid as Workflow<unknown, number>, {
          input: { v: input.v },
          workflowId: "ns-kid-1",
        });
      })
      .build();

    await createWorkflowRunner({ storage }).run({
      workflow: parent,
      workflowId: "ns-par",
      input: { v: 1 },
      namespace: "acme",
    });

    expect((await storage.loadWorkflow("ns-kid-1"))?.namespace).toBe("acme");
  });
});
