// ---------------------------------------------------------------------------
// `.subworkflow()` child creation — the child row carries the child's version
// (so the runner's version check passes), creation is create-if-absent (a
// re-invocation resumes the existing row instead of failing), and the child
// runs on the parent runner's clock.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

describe(".subworkflow() — versioned child", () => {
  it("runs a versioned child and stores the child's version on its row", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const child = workflow<number>({ name: "child-v", version: "3" })
      .step("inc", ({ input }) => succeed(input + 1))
      .build();
    const parent = workflow<number>({ name: "parent-v" })
      .subworkflow("sub", child, { input: (p) => p, workflowId: () => "child-v-1" })
      .build();

    const r = await runner.runSafe({ workflow: parent, workflowId: "parent-v-1", input: 1 });

    expect(r.error).toBeNull();
    expect(r.data).toBe(2);
    const childState = await storage.loadWorkflow("child-v-1");
    expect(childState?.version).toBe("3");
    expect(childState?.parentWorkflowId).toBe("parent-v-1");
    expect(childState?.status).toBe("completed");
  });

  it("resumes a pre-existing child row instead of failing (idempotent create)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    let childRuns = 0;
    const child = workflow<number>({ name: "child-i", version: "2" })
      .step("inc", ({ input }) => {
        childRuns++;
        return succeed(input + 1);
      })
      .build();
    const parent = workflow<number>({ name: "parent-i" })
      .subworkflow("sub", child, { input: (p) => p, workflowId: () => "child-i-1" })
      .build();

    // Simulate a crash between creating the child row and running it.
    await storage.createWorkflow({
      workflowId: "child-i-1",
      workflowName: "child-i",
      input: 10,
      parentWorkflowId: "parent-i-1",
      version: "2",
    });

    const first = await runner.run({ workflow: parent, workflowId: "parent-i-1", input: 10 });
    expect(first).toBe(11);
    expect(childRuns).toBe(1);

    // A second parent pointing at the same child id gets the completed
    // child's result without re-running it or rewriting the row.
    const second = await runner.run({ workflow: parent, workflowId: "parent-i-2", input: 10 });
    expect(second).toBe(11);
    expect(childRuns).toBe(1);
    const childState = await storage.loadWorkflow("child-i-1");
    expect(childState?.parentWorkflowId).toBe("parent-i-1");
    expect(childState?.version).toBe("2");
    const children = await storage.listWorkflows({ parentId: "parent-i-1" });
    expect(children.map((c) => c.workflowId)).toEqual(["child-i-1"]);
  });

  it("runs the child on the parent runner's clock", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00.000Z");
    const storage = new InMemoryWorkflowStorage({ clock });
    const runner = createWorkflowRunner({ storage, clock });
    const child = workflow<number>({ name: "child-c" })
      .dowhile(
        "spin",
        (_ctx, iter) => {
          clock.advance(100);
          return iter + 1;
        },
        (n) => n < 1,
      )
      .build();
    const parent = workflow<number>({ name: "parent-c" })
      .subworkflow("sub", child, { input: (p) => p, workflowId: () => "child-c-1" })
      .build();

    await runner.run({ workflow: parent, workflowId: "parent-c-1", input: 0 });

    const childState = await storage.loadWorkflow("child-c-1");
    expect(childState?.steps["spin.iter.0"]?.startedAt?.toISOString()).toBe(
      "2026-01-01T00:00:00.000Z",
    );
    expect(childState?.steps["spin.iter.0"]?.durationMs).toBe(100);
  });
});
