// ---------------------------------------------------------------------------
// A `.journaled()` body sees the version / patches of the definition it was
// built into, regardless of where `.version()` sits in the builder chain.
// Builder methods share StepDefinition objects between builders, so the
// runner hands the driving definition's config to the step at run time.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe(".journaled() — definition config comes from the driving definition", () => {
  it(".version() after .journaled() is visible as ctx.workflowVersion", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let seen: unknown = "unset";
    const wf = workflow<number>({ name: "jv-after" })
      .journaled("j", function* (ctx) {
        seen = ctx.workflowVersion;
        return 1;
      })
      .version("2")
      .build();

    await runner.run({ workflow: wf, workflowId: "jv-1", input: 1 });

    expect(seen).toBe("2");
    expect((await storage.loadWorkflow("jv-1"))?.version).toBe("2");
  });

  it("patches stay visible when .version() follows .journaled()", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let patched: boolean | undefined;
    const wf = workflow<number>({ name: "jp-after", patches: ["fix-1"] })
      .journaled("j", function* (ctx) {
        patched = ctx.patched("fix-1");
        return 1;
      })
      .version("2")
      .build();

    await runner.run({ workflow: wf, workflowId: "jp-1", input: 1 });
    expect(patched).toBe(true);
  });

  it("two versions built from one shared prefix each see their own version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const seen: unknown[] = [];
    const base = workflow<number>({ name: "jv-shared" }).journaled("j", function* (ctx) {
      seen.push(ctx.workflowVersion);
      return 1;
    });
    const v1 = base.version("1").build();
    const v2 = base.version("2").build();

    await runner.run({ workflow: v1, workflowId: "jvs-1", input: 1 });
    await runner.run({ workflow: v2, workflowId: "jvs-2", input: 1 });
    expect(seen).toEqual(["1", "2"]);
  });

  it(".map() after .journaled() keeps the bound version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let seen: unknown = "unset";
    const wf = workflow<number>({ name: "jv-map" })
      .journaled("j", function* (ctx) {
        seen = ctx.workflowVersion;
        return 20;
      })
      .map((n) => n + 1)
      .version("7")
      .build();

    const out = await runner.run({ workflow: wf, workflowId: "jvm-1", input: 1 });
    expect(out).toBe(21);
    expect(seen).toBe("7");
  });

  it("the ephemeral .execute() path binds the version too", async () => {
    let seen: unknown = "unset";
    const out = await workflow<number>({ name: "jv-exec" })
      .journaled("j", function* (ctx) {
        seen = ctx.workflowVersion;
        return 5;
      })
      .version("3")
      .execute(1);
    expect(out).toBe(5);
    expect(seen).toBe("3");
  });
});
