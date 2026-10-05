import { describe, it, expect } from "bun:test";
import { TaggedError, succeed, fail, tryPromise } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

class BranchFailed extends TaggedError("BranchFailed")<{ readonly message: string }>() {}

describe("parallel", () => {
  it("runs two branches concurrently and joins into a keyed record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ userId: string }>({ name: "par-basic" })
      .step("load", ({ input }) => succeed(input))
      .parallelSteps("enrich", {
        user: ({ prev }) => succeed({ name: `U:${prev.userId}` }),
        perms: ({ prev }) => succeed({ roles: [`R:${prev.userId}`] }),
      })
      .step("combine", ({ prev }) => succeed({ name: prev.user.name, roles: prev.perms.roles }))
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
      .parallelSteps("ops", {
        sum: ({ input }) => succeed(input.a + input.b),
        prod: ({ input }) => succeed(input.a * input.b),
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
      .step("in", ({ input }) => succeed(input))
      .parallelSteps("fork", {
        x: ({ prev }) => succeed(prev * 10),
        y: ({ prev }) => succeed(prev * 100),
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
      .parallelSteps("f", {
        upper: ({ input }) => succeed(input.toUpperCase()),
        len: ({ input }) => succeed(input.length),
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
      .parallelSteps("triple", {
        a: ({ input }) => succeed(input + 1),
        b: ({ input }) => succeed(input + 2),
        c: ({ input }) => succeed(input + 3),
      })
      .build();

    const result = await runner.run({ workflow: wf, workflowId: "wf-3-1", input: 10 });
    expect(result).toEqual({ a: 11, b: 12, c: 13 });
  });

  it("fails the workflow when a branch fails", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-fail" })
      .parallelSteps("fork", {
        ok: ({ input }) => succeed(input),
        bad: () => fail(new BranchFailed({ message: "boom" })),
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
    expect(() =>
      workflow<number>({ name: "par-empty" }).parallelSteps("empty", {}).build(),
    ).toThrow(/at least one branch/);
  });

  it("rejects duplicate block name", () => {
    expect(() =>
      workflow<number>({ name: "par-dup" })
        .step("foo", ({ input }) => succeed(input))
        .parallelSteps("foo", {
          a: ({ prev }) => succeed(prev),
        })
        .build(),
    ).toThrow(/Duplicate step name/);
  });

  it("rejects scoped-branch name collision with an existing step", () => {
    expect(() =>
      workflow<number>({ name: "par-col" })
        .step("fork.a", ({ input }) => succeed(input))
        .parallelSteps("fork", {
          a: ({ prev }) => succeed(prev),
        })
        .build(),
    ).toThrow(/Duplicate step name: "fork\.a"/);
  });

  it("chains after a parallel block — downstream sees the record", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "par-chain" })
      .parallelSteps("split", {
        dbl: ({ input }) => succeed(input * 2),
        trp: ({ input }) => succeed(input * 3),
      })
      .step("combine", ({ prev }) => succeed(prev.dbl + prev.trp))
      .step("format", ({ prev }) => succeed(`total:${prev}`))
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
      .step("start", ({ input }) => succeed(input))
      .parallelSteps("fork", {
        left: ({ prev }) => succeed(prev + 1),
        right: ({ prev }) => succeed(prev + 2),
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

  it("runs branches concurrently", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const started: string[] = [];

    // Each branch takes 40ms of clock time.
    const branch = (name: string) => () =>
      tryPromise(
        async () => {
          started.push(name);
          await new Promise<void>((r) => clock.setTimeout(r, 40));
          return name;
        },
        (e) => e,
      ).orDie();
    const wf = workflow<number>({ name: "par-concurrent" })
      .parallelSteps("sleep", { a: branch("a"), b: branch("b") })
      .build();

    const t0 = clock.currentTimeMs();
    let result: unknown;
    const running = runner
      .run({ workflow: wf, workflowId: "wf-conc-1", input: 0 })
      .then((r) => (result = r));

    // Both branches are in flight before any clock time has passed; run
    // one after the other, "b" could only start 40ms later.
    for (let i = 0; i < 2_000 && started.length < 2; i++) {
      await new Promise<void>((r) => setImmediate(r));
    }
    expect(started.sort()).toEqual(["a", "b"]);
    expect(clock.currentTimeMs()).toBe(t0);

    // One 40ms advance finishes both.
    clock.advance(40);
    await running;
    expect(result).toEqual({ a: "a", b: "b" });
    expect(clock.currentTimeMs() - t0).toBe(40);
  });
});
