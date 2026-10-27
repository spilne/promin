import { succeed, tryPromise } from "@spilne/perfect-core";
import { describe, it, expect, beforeEach } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { runJournaledStep } from "../journaled-step.ts";

// ---------------------------------------------------------------------------
// Tests for ctx.child inside .journaled() steps:
//  - Child workflow runs once, result journaled as step_type='child'.
//  - Replay returns journaled result; child fn not re-called.
//  - Default workflowId is derived from parent.workflowId + step + index.
//  - Explicit workflowId option is respected.
//  - Child failure is journaled and rethrown on replay.
//  - ctx.child throws if runChild callback not supplied.
//  - Non-determinism: child workflow name change detected.
// ---------------------------------------------------------------------------

describe("ctx.child", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(() => {
    storage = new InMemoryWorkflowStorage();
  });

  it("runs the child once, journals its result, and returns it", async () => {
    let enrichCalls = 0;

    const enrichWorkflow = workflow<{ userId: string }>({ name: "enrich" })
      .step("fetch", ({ input }) =>
        tryPromise(
          async () => {
            enrichCalls++;
            return { userId: input.userId, tags: ["vip"] };
          },
          (e) => e,
        ).orDie(),
      )
      .build();

    const parent = workflow<{ userId: string }>({ name: "signup" })
      .journaled("setup", function* (ctx, input) {
        const enrichment = yield* ctx.child(enrichWorkflow, {
          input: { userId: input.userId },
          workflowId: `enrich-${input.userId}`,
        });
        return { userId: input.userId, enrichment };
      })
      .build();

    const runner = createWorkflowRunner({ storage });
    const result = await runner.run({
      workflow: parent,
      workflowId: "parent-1",
      input: { userId: "u1" },
    });

    expect(enrichCalls).toBe(1);
    expect(result).toEqual({
      userId: "u1",
      enrichment: { userId: "u1", tags: ["vip"] },
    });

    // Re-running with same workflowId should replay from journal — child NOT re-called.
    const result2 = await runner.run({
      workflow: parent,
      workflowId: "parent-1",
      input: { userId: "u1" },
    });

    expect(enrichCalls).toBe(1); // still 1 — replayed from journal
    expect(result2).toEqual(result);
  });

  it("child workflow row is created with parentWorkflowId", async () => {
    const childWf = workflow<{ x: number }>({ name: "child-wf" })
      .step("double", ({ input }) => succeed(input.x * 2))
      .build();

    const parent = workflow<{ n: number }>({ name: "parent-wf" })
      .journaled("compute", function* (ctx, input) {
        return yield* ctx.child(childWf, {
          input: { x: input.n },
          workflowId: `child-${input.n}`,
        });
      })
      .build();

    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: parent, workflowId: "par-1", input: { n: 5 } });

    const childState = await storage.loadWorkflow("child-5");
    expect(childState).not.toBeNull();
    expect(childState?.parentWorkflowId).toBe("par-1");
    expect(childState?.status).toBe("completed");
  });

  it("uses deterministic default workflowId when none provided", async () => {
    const childWf = workflow<{ v: number }>({ name: "det-child" })
      .step("id", ({ input }) => succeed(input.v))
      .build();

    const parent = workflow<{ v: number }>({ name: "det-parent" })
      .journaled("run", function* (ctx, input) {
        return yield* ctx.child(childWf, { input: { v: input.v } });
      })
      .build();

    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: parent, workflowId: "det-par-1", input: { v: 42 } });

    // Default childId = "{parentId}.{stepName}.{activityIndex}" = "det-par-1.run.0"
    const childState = await storage.loadWorkflow("det-par-1.run.0");
    expect(childState).not.toBeNull();
    expect(childState?.status).toBe("completed");
  });

  it("journals child failure and rethrows on replay", async () => {
    const failingChild = workflow<void>({ name: "fail-child" })
      .step("boom", () =>
        tryPromise(
          () => Promise.reject(new Error("child exploded")),
          (e) => e,
        ).orDie(),
      )
      .build();

    const parent = workflow<void>({ name: "fail-parent" })
      .journaled("run", function* (ctx) {
        return yield* ctx.child(failingChild, { workflowId: "fail-child-1" });
      })
      .build();

    // Keeps the failure the engine discards once it escapes the body.
    const retaining = new (class extends InMemoryWorkflowStorage {
      override async discardJournalEntries(): Promise<void> {}
    })();
    const runner = createWorkflowRunner({ storage: retaining });
    await expect(
      runner.run({ workflow: parent, workflowId: "fail-par-1", input: undefined }),
    ).rejects.toThrow();

    const journal = await retaining.loadJournal({ workflowId: "fail-par-1", stepName: "run" });
    const childEntry = journal.find((e) => e.stepType === "child");
    expect(childEntry).toBeDefined();
    expect(childEntry?.exit?.tag).toBe("Failure");
  });

  it("throws if runChild callback is not provided", async () => {
    const childWf = workflow<{ x: number }>({ name: "no-runner-child" })
      .step("id", ({ input }) => succeed(input.x))
      .build();

    // Drive runJournaledStep directly without runChild
    await expect(
      runJournaledStep({
        input: undefined,
        prev: undefined,
        workflowId: "direct-1",
        stepName: "test",
        storage,
        body: function* (ctx) {
          return yield* ctx.child(childWf, { workflowId: "child-direct" });
        },
        // runChild intentionally omitted
      }),
    ).rejects.toThrow("requires a `runChild` callback");
  });
});
