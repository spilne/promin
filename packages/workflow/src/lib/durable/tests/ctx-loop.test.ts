import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("ctx.dowhile / ctx.dountil (inside journaled body)", () => {
  it("iterates while the condition stays true, each iter a journal entry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ start: number }>({ name: "jw-loop" })
      .journaled("poll", function* (ctx) {
        return yield* ctx.dowhile(
          "tick",
          (iter) => iter,
          (result) => result < 3,
        );
      })
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "jw-loop-1",
      input: { start: 0 },
    });

    // iter 0 → 0 < 3 continue, 1 → continue, 2 → continue, 3 → exit
    expect(result).toBe(3);

    const journal = await storage.loadJournal("jw-loop-1", "poll");
    const iterEntries = journal.filter((e) => e.activityName.startsWith("tick-iter-"));
    expect(iterEntries.length).toBe(4); // iter-0..iter-3
  });

  it("dountil inverts the predicate vs dowhile", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "jw-until" })
      .journaled("poll", function* (ctx) {
        return yield* ctx.dountil(
          "probe",
          (iter) => iter,
          (result) => result >= 2,
        );
      })
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "jw-until-1", input: 0 });
    // iter 0 → 0 >= 2 false, iter 1 → 1 false, iter 2 → 2 true → exit
    expect(result).toBe(2);
  });

  it("body runs at least once when condition is already false", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    let runs = 0;
    const wf = workflow<number>({ name: "jw-once" })
      .journaled("once", function* (ctx) {
        return yield* ctx.dowhile(
          "t",
          () => {
            runs++;
            return "x";
          },
          () => false,
        );
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "jw-once-1", input: 0 });
    expect(runs).toBe(1);
  });

  it("max-iteration guard trips with LoopLimitExceededError", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "jw-cap" })
      .journaled("runaway", function* (ctx) {
        return yield* ctx.dowhile(
          "t",
          () => 1,
          () => true,
          { maxIterations: 4 },
        );
      })
      .build();

    const result = await runner.runSafe({
      workflow: wf,
      workflowId: "jw-cap-1",
      input: 0,
    });
    expect(result.data).toBeNull();
    const state = await storage.loadWorkflow("jw-cap-1");
    expect(state?.error).toMatch(/exceeded 4 iterations/);
  });

  it("loops are composable with other ctx primitives (activity then loop)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ seed: number }>({ name: "jw-compose" })
      .journaled("compose", function* (ctx) {
        const seed = yield* ctx.activity("prepare", () => ctx.input.seed * 10);
        const final = yield* ctx.dowhile(
          "bump",
          (iter) => seed + iter,
          (result) => result < seed + 3,
        );
        return final;
      })
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "jw-compose-1",
      input: { seed: 5 },
    });
    // seed=50; iter 0→50 continue, 1→51 continue, 2→52 continue, 3→53 exit
    expect(result).toBe(53);
  });

  it("falls through to next-step consumption via prev", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "jw-chain" })
      .journaled("loop", function* (ctx) {
        return yield* ctx.dountil(
          "probe",
          (iter) => iter,
          (n) => n >= 2,
        );
      })
      .step("format", ({ prev }) => Pipeline.succeed(`done:${prev}`))
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "jw-chain-1", input: 0 });
    expect(result).toBe("done:2");
  });
});
