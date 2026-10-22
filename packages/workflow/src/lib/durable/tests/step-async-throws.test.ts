import { describe, it, expect } from "bun:test";
import { tryPromise } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

// Reproducer for promin-4ace: a workflow whose `.stepAsync()` body throws
// used to leave the workflow row stuck in `status = "pending"` with no
// step rows and no error recorded. `.stepAsync()` lifts its body with
// defect semantics (a rejection is a defect, not a typed failure), and
// settling the step batch without catching defects let the failure escape
// past the `saveStepFailure` path. Fixed by catching defects there too.

describe("stepAsync throwing body — promin-4ace reproducer", () => {
  it("lands the workflow in status=failed when stepAsync throws", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<{ input: number }>({ name: "throwy" })
      .stepAsync("boom", async () => {
        throw new Error("boom");
      })
      .build();

    const result = await runner.runSafe({
      workflow: wf,
      workflowId: "wf-throwy-1",
      input: { input: 1 },
    });

    expect(result.data).toBeNull();
    expect(result.error).toBeDefined();

    const state = await storage.loadWorkflow("wf-throwy-1");
    expect(state?.status).toBe("failed");
    // The step row must record the failure with its error message so
    // observability (dashboards, alerts) can surface it.
    expect(state?.steps["boom"]?.status).toBe("failed");
    expect(state?.steps["boom"]?.error).toContain("boom");
  });

  it("handles user code that lifts a rejecting promise as a defect", async () => {
    // Any user step that lifts a throwing body with `.orDie()` would hit
    // the same defect-escape path. The runner-level defect handling
    // covers this case too.
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "raw-from-promise" })
      .step("boom", () =>
        tryPromise(
          async () => {
            throw new Error("raw rejection");
          },
          (e) => e,
        ).orDie(),
      )
      .build();

    const result = await runner.runSafe({
      workflow: wf,
      workflowId: "wf-raw-1",
      input: 0,
    });

    expect(result.data).toBeNull();
    const state = await storage.loadWorkflow("wf-raw-1");
    expect(state?.status).toBe("failed");
    expect(state?.steps["boom"]?.status).toBe("failed");
    expect(state?.steps["boom"]?.error).toContain("raw rejection");
  });

  it("sync throw inside stepAsync also fails the workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow<number>({ name: "sync-throw" })
      .stepAsync("boom", (() => {
        throw new Error("sync");
      }) as () => Promise<number>)
      .build();

    const result = await runner.runSafe({
      workflow: wf,
      workflowId: "wf-sync-1",
      input: 0,
    });

    expect(result.data).toBeNull();
    const state = await storage.loadWorkflow("wf-sync-1");
    expect(state?.status).toBe("failed");
  });
});
