import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { LoopLimitExceededError } from "../durable-pipeline-error.ts";

describe("dowhile / dountil", () => {
  describe("dowhile", () => {
    it("iterates while the condition stays true", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<{ start: number }>({ name: "w-count" })
        .step("load", ({ input }) => Pipeline.succeed(input.start))
        .dowhile(
          "inc",
          (ctx, iter) => (ctx.prev as number) + iter + 1,
          (n) => n < 6,
        )
        .build();

      const result = await runner.run({
        workflow: wf,
        workflowId: "w-1",
        input: { start: 0 },
      });

      // iter 0: 0 + 0 + 1 = 1 → continue
      // iter 1: 0 + 1 + 1 = 2 → continue
      // iter 2: 0 + 2 + 1 = 3 → continue
      // iter 3: 0 + 3 + 1 = 4 → continue
      // iter 4: 0 + 4 + 1 = 5 → continue
      // iter 5: 0 + 5 + 1 = 6 → exit (condition returns false)
      expect(result).toBe(6);
    });

    it("body runs at least once even when condition is already false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      let runs = 0;
      const wf = workflow<number>({ name: "w-once" })
        .dowhile(
          "body",
          () => {
            runs++;
            return "only-once";
          },
          () => false,
        )
        .build();

      const result = await runner.run({ workflow: wf, workflowId: "w-once-1", input: 0 });
      expect(result).toBe("only-once");
      expect(runs).toBe(1);
    });

    it("exits immediately after first iteration when condition is false", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      let runs = 0;
      const wf = workflow<number>({ name: "w-one" })
        .dowhile(
          "body",
          (ctx, iter) => {
            runs++;
            return iter;
          },
          (result) => result < 0,
        )
        .build();

      await runner.run({ workflow: wf, workflowId: "w-one-1", input: 0 });
      expect(runs).toBe(1);
    });
  });

  describe("dountil", () => {
    it("iterates until the condition becomes true", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<{ target: number }>({ name: "u-target" })
        .dountil(
          "climb",
          (ctx, iter) => iter + 1,
          (n, _iter) => n >= ctx_target,
        )
        .build();

      const ctx_target = 3;
      const result = await runner.run({
        workflow: wf,
        workflowId: "u-1",
        input: { target: 3 },
      });

      // iter 0: 1 → 1 >= 3 is false → continue
      // iter 1: 2 → 2 >= 3 is false → continue
      // iter 2: 3 → 3 >= 3 is true → exit
      expect(result).toBe(3);
    });

    it("inverse polarity of dowhile with same predicate", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "u-invert" })
        .dountil(
          "body",
          (_ctx, iter) => iter,
          (result) => result >= 2,
        )
        .build();

      const result = await runner.run({
        workflow: wf,
        workflowId: "u-invert-1",
        input: 0,
      });
      // iter 0: 0 → 0 >= 2 false → continue
      // iter 1: 1 → 1 >= 2 false → continue
      // iter 2: 2 → 2 >= 2 true → exit
      expect(result).toBe(2);
    });
  });

  describe("max iteration guard", () => {
    it("throws LoopLimitExceededError when body never converges", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "w-runaway" })
        .dowhile(
          "loopy",
          () => 1,
          () => true, // never converges
          { maxIterations: 3 },
        )
        .build();

      const result = await runner.runSafe({
        workflow: wf,
        workflowId: "w-runaway-1",
        input: 0,
      });

      expect(result.data).toBeNull();
      expect(result.error).toBeDefined();
      // The error surfaces wrapped in a StepError; the underlying cause is
      // LoopLimitExceededError. Verify it made it into the error message.
      const state = await storage.loadWorkflow("w-runaway-1");
      expect(state?.status).toBe("failed");
      expect(state?.error).toMatch(/exceeded 3 iterations/);
    });

    it("uses default max of 100 when not specified", async () => {
      // Run a loop that would run 120 times without the cap. We expect it
      // to stop at 100.
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "w-default-cap" })
        .dountil(
          "stops",
          (_ctx, iter) => iter,
          (result) => result >= 200, // never reaches — 100 is the cap
        )
        .build();

      const result = await runner.runSafe({
        workflow: wf,
        workflowId: "w-cap-1",
        input: 0,
      });

      expect(result.data).toBeNull();
      const state = await storage.loadWorkflow("w-cap-1");
      expect(state?.error).toMatch(/exceeded 100 iterations/);
    });

    it("rejects maxIterations < 1 at build time", () => {
      expect(() =>
        workflow<number>({ name: "w-bad" })
          .dowhile(
            "x",
            () => 1,
            () => false,
            { maxIterations: 0 },
          )
          .build(),
      ).toThrow(/maxIterations/);
    });
  });

  describe("chaining", () => {
    it("passes the final iteration result to the next step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "w-chain" })
        .dowhile(
          "count",
          (_ctx, iter) => iter,
          (result) => result < 3,
        )
        .step("format", ({ prev }) => Pipeline.succeed(`iterations-done:${prev}`))
        .build();

      const result = await runner.run({
        workflow: wf,
        workflowId: "w-chain-1",
        input: 0,
      });
      expect(result).toBe("iterations-done:3");
    });

    it("can follow another step", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<{ n: number }>({ name: "w-after" })
        .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
        .dowhile(
          "inc",
          // ctx.prev is the step-level prev (fixed across iterations).
          // Iterations differ via the `iter` index — we accumulate with it.
          (ctx, iter) => (ctx.prev as number) + iter + 1,
          (result) => result < 10,
        )
        .build();

      const result = await runner.run({
        workflow: wf,
        workflowId: "w-after-1",
        input: { n: 3 },
      });
      // start=6, iter0=7, iter1=8, iter2=9, iter3=10 → exit at 10
      expect(result).toBe(10);
    });
  });

  describe("per-iteration step rows", () => {
    it("saves a step row per iteration (named <name>.iter.<n>)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "w-iter-rows" })
        .dowhile(
          "loop",
          (_ctx, iter) => iter,
          (result) => result < 2,
        )
        .build();

      await runner.run({ workflow: wf, workflowId: "w-iter-rows-1", input: 0 });

      const state = await storage.loadWorkflow("w-iter-rows-1");
      expect(state?.status).toBe("completed");
      expect(state?.steps["loop"]?.status).toBe("completed");
      expect(state?.steps["loop.iter.0"]?.status).toBe("completed");
      expect(state?.steps["loop.iter.1"]?.status).toBe("completed");
      expect(state?.steps["loop.iter.2"]?.status).toBe("completed");
      // loop exits at iter 2 since condition (result < 2) becomes false
      // after the iter-2 body returns 2.
      expect(state?.steps["loop.iter.3"]).toBeUndefined();
    });

    it("resumes from the first missing iter row on restart", async () => {
      // Simulate a crash mid-loop by pre-seeding completed iter rows in
      // storage before running the workflow. The loop should replay them
      // (body never runs for those iters) and resume from iter-2.
      const storage = new InMemoryWorkflowStorage();
      await storage.createWorkflow({
        workflowId: "w-resume-1",
        workflowName: "w-resume",
        input: 0,
      });
      const now = new Date(0);
      await storage.saveStepResult({
        workflowId: "w-resume-1",
        stepName: "loop.iter.0",
        result: 0,
        durationMs: 1,
        startedAt: now,
      });
      await storage.saveStepResult({
        workflowId: "w-resume-1",
        stepName: "loop.iter.1",
        result: 1,
        durationMs: 1,
        startedAt: now,
      });

      let bodyRuns = 0;
      const runner = createWorkflowRunner({ storage });
      const wf = workflow<number>({ name: "w-resume" })
        .dowhile(
          "loop",
          (_ctx, iter) => {
            bodyRuns++;
            return iter;
          },
          (result) => result < 3,
        )
        .build();

      const result = await runner.run({ workflow: wf, workflowId: "w-resume-1", input: 0 });

      // iters 0 and 1 were pre-seeded; the body should only run for iters
      // 2 and 3, at which point condition(3, 3) is false → exit.
      expect(bodyRuns).toBe(2);
      expect(result).toBe(3);
    });

    it("per-iteration rows carry duration and timestamps", async () => {
      const storage = new InMemoryWorkflowStorage();
      const runner = createWorkflowRunner({ storage });

      const wf = workflow<number>({ name: "w-iter-meta" })
        .dowhile(
          "loop",
          (_ctx, iter) => iter,
          (result) => result < 1,
        )
        .build();

      await runner.run({ workflow: wf, workflowId: "w-iter-meta-1", input: 0 });

      const state = await storage.loadWorkflow("w-iter-meta-1");
      const iter0 = state?.steps["loop.iter.0"];
      expect(iter0).toBeDefined();
      expect(iter0?.startedAt).toBeInstanceOf(Date);
      expect(iter0?.completedAt).toBeInstanceOf(Date);
      expect(typeof iter0?.durationMs).toBe("number");
    });
  });
});
