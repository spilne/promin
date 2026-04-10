import { describe, it, expect } from "bun:test";
import { Pipeline } from "../pipeline.ts";
import { workflow, WorkflowVersionMismatchError } from "./index.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";

describe("workflow versioning", () => {
  it("stores no version when not set", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ x: number }>({ name: "test", storage }).step("add", ({ input }) =>
      Pipeline.succeed(input.x + 1),
    );

    await wf.run({ workflowId: "v-default", input: { x: 1 } });
    const state = await storage.loadWorkflow("v-default");
    expect(state!.version).toBeUndefined();
  });

  it("stores version from .version() builder method", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ x: number }>({ name: "test", storage })
      .version("2")
      .step("add", ({ input }) => Pipeline.succeed(input.x + 1));

    await wf.run({ workflowId: "v-explicit", input: { x: 1 } });
    const state = await storage.loadWorkflow("v-explicit");
    expect(state!.version).toBe("2");
  });

  it("stores version from workflow() params", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ x: number }>({ name: "test", storage, version: "3" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );

    await wf.run({ workflowId: "v-params", input: { x: 1 } });
    const state = await storage.loadWorkflow("v-params");
    expect(state!.version).toBe("3");
  });

  it("resumes successfully when version matches", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ x: number }>({ name: "test", storage, version: "2" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );

    const result1 = await wf.run({ workflowId: "v-match", input: { x: 1 } });
    expect(result1).toBe(2);

    // Re-run same version — should succeed (idempotent)
    const result2 = await wf.run({ workflowId: "v-match", input: { x: 1 } });
    expect(result2).toBe(2);
  });

  it("skips version check when builder has no version", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Create workflow with explicit version
    const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );
    await v1.run({ workflowId: "v-skip", input: { x: 1 } });

    // Resume with no version set — should succeed (no check)
    const noVersion = workflow<{ x: number }>({ name: "test", storage }).step("add", ({ input }) =>
      Pipeline.succeed(input.x + 1),
    );
    const result = await noVersion.run({ workflowId: "v-skip", input: { x: 1 } });
    expect(result).toBe(2);
  });

  it("throws WorkflowVersionMismatchError on version mismatch", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Create workflow with v1
    const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );
    await v1.run({ workflowId: "v-mismatch", input: { x: 1 } });

    // Try to resume with v2 — should throw
    const v2 = workflow<{ x: number }>({ name: "test", storage, version: "2" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );

    try {
      await v2.run({ workflowId: "v-mismatch", input: { x: 1 } });
      expect(true).toBe(false); // should not reach
    } catch (e) {
      expect(e).toBeInstanceOf(WorkflowVersionMismatchError);
      const err = e as InstanceType<typeof WorkflowVersionMismatchError>;
      expect(err.workflowId).toBe("v-mismatch");
      expect(err.expected).toBe("2");
      expect(err.actual).toBe("1");
    }
  });

  it("throws when resuming unversioned workflow with versioned code", async () => {
    const storage = new InMemoryWorkflowStorage();

    // Create workflow without version
    const noVer = workflow<{ x: number }>({ name: "test", storage }).step("add", ({ input }) =>
      Pipeline.succeed(input.x + 1),
    );
    await noVer.run({ workflowId: "v-upgrade", input: { x: 1 } });

    // Try to resume with explicit version — should throw
    const v2 = workflow<{ x: number }>({ name: "test", storage, version: "2" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );

    try {
      await v2.run({ workflowId: "v-upgrade", input: { x: 1 } });
      expect(true).toBe(false);
    } catch (e) {
      expect(e).toBeInstanceOf(WorkflowVersionMismatchError);
      const err = e as InstanceType<typeof WorkflowVersionMismatchError>;
      expect(err.actual).toBe("(none)");
      expect(err.expected).toBe("2");
    }
  });

  it("new workflow always creates with builder version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const wf = workflow<{ x: number }>({ name: "test", storage, version: "5" }).step(
      "add",
      ({ input }) => Pipeline.succeed(input.x + 1),
    );

    await wf.run({ workflowId: "v-new", input: { x: 1 } });
    const state = await storage.loadWorkflow("v-new");
    expect(state!.version).toBe("5");
  });
});
