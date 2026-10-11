import { describe, expect, it } from "bun:test";
import { TaggedError, fail, succeed } from "@spilne/perfect-core";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { bindWorkflow, createWorkflowApp } from "../workflow-app.ts";
import { workflow } from "../workflow-builder.ts";
import { step } from "../workflow-fragment.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

class Declined extends TaggedError("Declined")<{ readonly message: string }>() {}

const suspendedTag = (error: unknown): string | undefined =>
  (error as { _tag?: string } | null)?._tag;

describe("step bodies", () => {
  it("accepts Promise-returning functions in step()", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<{ n: number }>({ name: "unified-step" })
      .step("double", async ({ input }) => input.n * 2)
      .step("plus-one", async ({ prev }) => prev + 1)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "unified-step-1", input: { n: 20 } }),
    ).resolves.toBe(41);
  });

  it("accepts plain values and Effs in step()", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<{ n: number }>({ name: "plain-step" })
      .step("double", ({ input }) => input.n * 2)
      .step("eff", ({ prev }) => succeed(`${prev}!`))
      .step("dag", { dependsOn: ["double", "eff"] }, ({ deps }) => ({ ...deps }))
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "plain-step-1", input: { n: 2 } }),
    ).resolves.toEqual({ double: 4, eff: "4!" });
  });

  it("keeps an Eff's typed failure typed (retry applies), but a rejection is a defect", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let effAttempts = 0;
    let promiseAttempts = 0;
    const retry = { maxRetries: 2, baseDelayMs: 0 };

    const typed = workflow<number>({ name: "typed-failure" })
      .step(
        "charge",
        () => {
          effAttempts++;
          return fail(new Declined({ message: "no" }));
        },
        { retry },
      )
      .build();
    const { error } = await runner.runSafe({ workflow: typed, workflowId: "typed-1", input: 0 });
    expect(error).toBeInstanceOf(Declined);
    expect(effAttempts).toBe(3);

    const defect = workflow<number>({ name: "defect" })
      .step(
        "charge",
        async () => {
          promiseAttempts++;
          throw new Error("boom");
        },
        { retry },
      )
      .build();
    const result = await runner.runSafe({ workflow: defect, workflowId: "defect-1", input: 0 });
    expect(result.data).toBeNull();
    expect(promiseAttempts).toBe(1);
    expect((await storage.loadWorkflow("defect-1"))?.steps["charge"]?.error).toContain("boom");
  });

  it("accepts Promise and plain bodies in mapOver, dowhile and dountil", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<number[]>({ name: "bodies" })
      .step("items", ({ input }) => input)
      .mapOver("doubled", { array: "items" }, async (n) => n * 2)
      .dowhile(
        "count",
        (_ctx, iter) => iter,
        (n) => n < 2,
      )
      .dountil(
        "until",
        async ({ prev }, iter) => prev + iter,
        (n) => n >= 3,
      )
      .step("out", { dependsOn: ["doubled", "until"] }, ({ deps }) => deps)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "bodies-1", input: [1, 2] }),
    ).resolves.toEqual({ doubled: [2, 4], until: 3 });
  });
});

describe("parallel", () => {
  it("forks the head into Promise-returning branches", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<{ n: number }>({ name: "parallel-fork" })
      .step("start", async ({ input }) => input.n)
      .parallel("fanout", {
        left: async ({ prev }) => prev + 1,
        right: ({ prev }) => prev + 2,
        eff: ({ prev }) => succeed(prev + 3),
      })
      .step("join", async ({ prev }) => prev.left + prev.right + prev.eff)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "parallel-fork-1", input: { n: 10 } }),
    ).resolves.toBe(36);
    expect(wf.dag.steps.find((s) => s.name === "fanout.left")?.dependsOn).toEqual(["start"]);
  });

  it("forks from explicit dependencies", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<{ n: number }>({ name: "parallel-dag" })
      .step("a", ({ input }) => input.n)
      .step("b", { dependsOn: [] }, () => "b")
      .parallel(
        "fanout",
        { dependsOn: ["a", "b"] },
        { inc: async ({ prev }) => prev + 1, same: ({ prev }) => prev },
      )
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "parallel-dag-1", input: { n: 1 } }),
    ).resolves.toEqual({ inc: 2, same: 1 });
    expect(wf.dag.steps.find((s) => s.name === "fanout.inc")?.dependsOn).toEqual(["a", "b"]);
  });
});

describe("dependsOn forms", () => {
  it("runs a DAG branch on its first dependency's value", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<string>({ name: "branch-dag" })
      .step("text", ({ input }) => input)
      .step("other", { dependsOn: [] }, () => 0)
      .branch("route", {
        dependsOn: ["text", "other"],
        condition: (text) => text.length > 3,
        ifTrue: async ({ prev }) => `long:${prev}`,
        ifFalse: ({ prev }) => `short:${prev}`,
      })
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "branch-dag-1", input: "hi" }),
    ).resolves.toBe("short:hi");
    expect(wf.dag.steps.find((s) => s.name === "route")?.dependsOn).toEqual(["text", "other"]);
  });

  it("sleeps after explicit dependencies and passes the first one's value through", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const wf = workflow<number>({ name: "sleep-dag" })
      .step("a", ({ input }) => input + 1)
      .step("b", { dependsOn: [] }, () => "b")
      .sleep("pause", 0, { dependsOn: ["a", "b"] })
      .step("after", ({ prev }) => prev * 10)
      .build();

    const first = await runner.runSafe({ workflow: wf, workflowId: "sleep-dag-1", input: 1 });
    expect(suspendedTag(first.error)).toBe("WorkflowSuspendedError");
    await expect(runner.run({ workflow: wf, workflowId: "sleep-dag-1", input: 1 })).resolves.toBe(
      20,
    );
  });
});

describe("approval", () => {
  it("waits for <name>-approved by default", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ requestId: string }>({ name: "approval-default" })
      .step("load", async ({ input }) => input.requestId)
      .approval("manager")
      .step("finish", ({ prev }) => prev.approved)
      .build();

    const first = await runner.runSafe({
      workflow: wf,
      workflowId: "approval-default-1",
      input: { requestId: "r1" },
    });
    expect(suspendedTag(first.error)).toBe("WorkflowSuspendedError");
    await storage.deliverSignal({
      workflowId: "approval-default-1",
      signalName: "manager-approved",
      payload: { approved: false },
    });

    await expect(
      runner.run({ workflow: wf, workflowId: "approval-default-1", input: { requestId: "r1" } }),
    ).resolves.toBe(false);
  });

  it("takes a custom signal name, payload type and dependsOn", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const wf = workflow<{ requestId: string }>({ name: "approval-custom" })
      .step("load", async ({ input }) => input.requestId)
      .step("audit", { dependsOn: [] }, () => "audited")
      .approval<{ by: string }>("sign-off", {
        signalName: "signed",
        dependsOn: ["load", "audit"],
      })
      .step("finish", ({ prev }) => prev.by)
      .build();

    expect(wf.dag.steps.find((s) => s.name === "sign-off")?.dependsOn).toEqual(["load", "audit"]);
    await runner.runSafe({
      workflow: wf,
      workflowId: "approval-custom-1",
      input: { requestId: "r1" },
    });
    await storage.deliverSignal({
      workflowId: "approval-custom-1",
      signalName: "signed",
      payload: { by: "ada" },
    });

    await expect(
      runner.run({ workflow: wf, workflowId: "approval-custom-1", input: { requestId: "r1" } }),
    ).resolves.toBe("ada");
  });
});

describe("fragments", () => {
  it("applies a reusable fragment", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const normalize = step("trim", ({ input }: { input: { name: string } }) =>
      input.name.trim(),
    ).andThen("upper", async ({ prev }) => prev.toUpperCase());

    const wf = workflow<{ name: string }>({ name: "fragment-use" })
      .use(normalize)
      .step("greet", { dependsOn: ["trim", "upper"] }, ({ deps }) => `${deps.trim}/${deps.upper}`)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "fragment-use-1", input: { name: " ada " } }),
    ).resolves.toBe("ada/ADA");
  });

  it("applies fragment parallel blocks after the current head", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const enrich = step<unknown, number, never, number>("inc", ({ prev }) => prev + 1).parallel(
      "split",
      { half: ({ prev }) => prev / 2, twice: async ({ prev }) => prev * 2 },
    );

    const wf = workflow<number>({ name: "fragment-parallel" })
      .step("start", ({ input }) => input)
      .use(enrich)
      .step("sum", ({ prev }) => prev.half + prev.twice)
      .build();

    await expect(
      runner.run({ workflow: wf, workflowId: "fragment-parallel-1", input: 3 }),
    ).resolves.toBe(10);
    expect(wf.dag.steps.find((s) => s.name === "inc")?.dependsOn).toEqual(["start"]);
  });

  it("is immutable: extending a fragment leaves the original unchanged", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const base = step("one", () => 1);
    base.andThen("two", ({ prev }) => prev + 1);

    const wf = workflow<null>({ name: "fragment-immutable" }).use(base).build();
    expect(wf.dag.steps.map((s) => s.name)).toEqual(["one"]);
    await expect(
      runner.run({ workflow: wf, workflowId: "fragment-immutable-1", input: null }),
    ).resolves.toBe(1);
  });

  it("rejects a fragment whose step names clash with the workflow's", () => {
    const frag = step("load", () => 1);
    const wf = workflow<null>({ name: "fragment-clash" }).step("load", () => 0);
    expect(() => wf.use(frag)).toThrow('Duplicate step name: "load"');
  });
});

describe("bound workflows", () => {
  it("runs workflows through createWorkflowApp and bindWorkflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ n: number }>({ name: "bound-app" })
      .step("triple", ({ input }) => succeed(input.n * 3))
      .build();

    const app = createWorkflowApp({ storage });
    const bound = app.workflow(wf);
    expect(bound.workflow).toBe(wf);
    expect(bound.runner).toBe(app.runner);

    await expect(bound.run({ workflowId: "bound-app-1", input: { n: 5 } })).resolves.toBe(15);
    await expect(bound.runSafe({ workflowId: "bound-app-2", input: { n: 1 } })).resolves.toEqual({
      data: 3,
      error: null,
    });
    const handle = await bound.start({ workflowId: "bound-app-3", input: { n: 2 } });
    await expect(handle.result()).resolves.toBe(6);
    await expect(bound.handle("bound-app-1").result()).resolves.toBe(15);

    const viaFn = bindWorkflow({ runner: app.runner, workflow: wf });
    await expect(viaFn.run({ workflowId: "bound-app-4", input: { n: 6 } })).resolves.toBe(18);
  });

  it("reuses a given runner", async () => {
    const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });
    const app = createWorkflowApp({ runner });
    expect(app.runner).toBe(runner);
  });

  it("resumes a bound workflow from a step", async () => {
    const storage = new InMemoryWorkflowStorage();
    let calls = 0;
    const wf = workflow<number>({ name: "bound-resume" })
      .step("a", ({ input }) => input)
      .step("b", ({ prev }) => {
        calls++;
        return prev + calls;
      })
      .build();
    const bound = createWorkflowApp({ storage }).workflow(wf);

    await expect(bound.run({ workflowId: "bound-resume-1", input: 10 })).resolves.toBe(11);
    await expect(bound.resume({ workflowId: "bound-resume-1", fromStep: "b" })).resolves.toBe(12);
  });
});
