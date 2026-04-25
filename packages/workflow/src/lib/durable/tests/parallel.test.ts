import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "@promin/core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

class BranchFailed extends Data.TaggedError("BranchFailed")<{ readonly message: string }> {}

describe("parallel", () => {
  it("runs two branches concurrently and joins into a keyed record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ userId: string }>({ name: "par-basic" })
      .step("load", ({ input }) => Pipeline.succeed(input))
      .parallel("enrich", {
        user: ({ prev }) => Pipeline.succeed({ name: `U:${prev.userId}` }),
        perms: ({ prev }) => Pipeline.succeed({ roles: [`R:${prev.userId}`] }),
      })
      .step("combine", ({ prev }) =>
        Pipeline.succeed({ name: prev.user.name, roles: prev.perms.roles }),
      )
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "wf-par-1",
      input: { userId: "42" },
    });

    expect(result).toEqual({ name: "U:42", roles: ["R:42"] });
  });

  it("runs as the first block (no parent step)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ a: number; b: number }>({ name: "par-first" })
      .parallel("ops", {
        sum: ({ input }) => Pipeline.succeed(input.a + input.b),
        prod: ({ input }) => Pipeline.succeed(input.a * input.b),
      })
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "wf-par-first-1",
      input: { a: 3, b: 4 },
    });

    expect(result).toEqual({ sum: 7, prod: 12 });
  });

  it("persists each branch as its own scoped step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-persist" })
      .step("in", ({ input }) => Pipeline.succeed(input))
      .parallel("fork", {
        x: ({ prev }) => Pipeline.succeed(prev * 10),
        y: ({ prev }) => Pipeline.succeed(prev * 100),
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "wf-persist-1", input: 5 });

    const state = await storage.loadWorkflow("wf-persist-1");
    expect(state?.steps["fork.x"]?.status).toBe("completed");
    expect(state?.steps["fork.y"]?.status).toBe("completed");
    expect(state?.steps["fork"]?.status).toBe("completed");
    expect(state?.steps["fork"]?.stepType).toBeDefined();
  });

  it("join step result equals the branch record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<string>({ name: "par-join-result" })
      .parallel("f", {
        upper: ({ input }) => Pipeline.succeed(input.toUpperCase()),
        len: ({ input }) => Pipeline.succeed(input.length),
      })
      .build();

    await runner.run({ workflow: wf, workflowId: "wf-join-1", input: "abcd" });

    const state = await storage.loadWorkflow("wf-join-1");
    expect(state?.steps["f"]?.result).toEqual({ upper: "ABCD", len: 4 });
  });

  it("supports three or more branches", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-3" })
      .parallel("triple", {
        a: ({ input }) => Pipeline.succeed(input + 1),
        b: ({ input }) => Pipeline.succeed(input + 2),
        c: ({ input }) => Pipeline.succeed(input + 3),
      })
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-3-1", input: 10 });
    expect(result).toEqual({ a: 11, b: 12, c: 13 });
  });

  it("fails the workflow when a branch fails", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-fail" })
      .parallel("fork", {
        ok: ({ input }) => Pipeline.succeed(input),
        bad: () => Pipeline.fail(new BranchFailed({ message: "boom" })),
      })
      .build();

    const result = await runner.runSafe({
      workflow: wf,
      workflowId: "wf-fail-1",
      input: 1,
    });

    expect(result.data).toBeNull();
    expect(result.error).toBeDefined();
    const state = await storage.loadWorkflow("wf-fail-1");
    expect(state?.status).toBe("failed");
  });

  it("rejects empty branches object", () => {
    expect(() => workflow<number>({ name: "par-empty" }).parallel("empty", {}).build()).toThrow(
      /at least one branch/,
    );
  });

  it("rejects duplicate block name", () => {
    expect(() =>
      workflow<number>({ name: "par-dup" })
        .step("foo", ({ input }) => Pipeline.succeed(input))
        .parallel("foo", {
          a: ({ prev }) => Pipeline.succeed(prev),
        })
        .build(),
    ).toThrow(/Duplicate step name/);
  });

  it("rejects scoped-branch name collision with an existing step", () => {
    expect(() =>
      workflow<number>({ name: "par-col" })
        .step("fork.a", ({ input }) => Pipeline.succeed(input))
        .parallel("fork", {
          a: ({ prev }) => Pipeline.succeed(prev),
        })
        .build(),
    ).toThrow(/Duplicate step name: "fork\.a"/);
  });

  it("chains after a parallel block — downstream sees the record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-chain" })
      .parallel("split", {
        dbl: ({ input }) => Pipeline.succeed(input * 2),
        trp: ({ input }) => Pipeline.succeed(input * 3),
      })
      .step("combine", ({ prev }) => Pipeline.succeed(prev.dbl + prev.trp))
      .step("format", ({ prev }) => Pipeline.succeed(`total:${prev}`))
      .build();

    const result = await runner.run({
      workflow: wf,
      workflowId: "wf-chain-1",
      input: 10,
    });
    expect(result).toBe("total:50");
  });

  it("DAG exports branches and join node", () => {
    const wf = workflow<number>({ name: "par-dag" })
      .step("start", ({ input }) => Pipeline.succeed(input))
      .parallel("fork", {
        left: ({ prev }) => Pipeline.succeed(prev + 1),
        right: ({ prev }) => Pipeline.succeed(prev + 2),
      })
      .build();

    const dag = wf.dag;
    const stepNames = dag.steps.map((s) => s.name);
    expect(stepNames).toContain("fork.left");
    expect(stepNames).toContain("fork.right");
    expect(stepNames).toContain("fork");

    const joinNode = dag.steps.find((s) => s.name === "fork")!;
    expect(joinNode.kind).toBe("parallel");
    expect(joinNode.dependsOn).toEqual(["fork.left", "fork.right"]);

    const leftNode = dag.steps.find((s) => s.name === "fork.left")!;
    expect(leftNode.dependsOn).toEqual(["start"]);
  });

  it("runs branches concurrently (timing observable)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-concurrent" })
      .parallel("sleep", {
        a: () =>
          Pipeline.fromPromise(async () => {
            await new Promise((r) => setTimeout(r, 40));
            return "a";
          }),
        b: () =>
          Pipeline.fromPromise(async () => {
            await new Promise((r) => setTimeout(r, 40));
            return "b";
          }),
      })
      .build();

    const t0 = Date.now();
    const result = await runner.run({
      workflow: wf,
      workflowId: "wf-conc-1",
      input: 0,
    });
    const elapsed = Date.now() - t0;

    expect(result).toEqual({ a: "a", b: "b" });
    // If they ran sequentially this would be ≥ 80ms. Allow generous headroom
    // for CI noise but still gate on concurrency.
    expect(elapsed).toBeLessThan(75);
  });
});
