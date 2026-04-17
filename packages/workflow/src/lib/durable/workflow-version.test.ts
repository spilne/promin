import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
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

  // ---------------------------------------------------------------------------
  // onVersionMismatch: "drain" + previousVersions
  // ---------------------------------------------------------------------------

  describe("drain policy", () => {
    it("delegates resume of older-version workflow to matching previousVersion", async () => {
      const storage = new InMemoryWorkflowStorage();

      // v1 — logic produces input * 10
      const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" })
        .step("compute", ({ input }) => Pipeline.succeed(input.x * 10))
        .build();

      // Create a v1 workflow, don't run it to completion (we'll pretend it's in flight).
      await v1.run({ workflowId: "drain-1", input: { x: 3 } });
      // Actually v1 ran to completion. For drain test, what matters is that the
      // stored version is "1" while we run v2's definition against it.

      // v2 — different logic (input * 100) + drain policy + v1 in previousVersions
      const v2 = workflow<{ x: number }>({
        name: "test",
        storage,
        version: "2",
        onVersionMismatch: "drain",
        previousVersions: [v1],
      }).step("compute", ({ input }) => Pipeline.succeed(input.x * 100));

      // Running v2 against the v1 workflow should delegate to v1's def.
      // With idempotency absent, re-run loads the completed state; but the
      // real check is that no VersionMismatchError is thrown.
      const result = await v2.run({ workflowId: "drain-1", input: { x: 3 } });
      // v1's logic produced 30 on the first run, and the workflow already
      // completed, so re-running just returns the cached result.
      expect(result).toBe(30);
    });

    it("throws VersionMismatchError when drain can't find matching previousVersion", async () => {
      const storage = new InMemoryWorkflowStorage();

      const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" })
        .step("compute", ({ input }) => Pipeline.succeed(input.x * 10))
        .build();
      await v1.run({ workflowId: "drain-missing", input: { x: 1 } });

      // v3 with drain policy but only v2 in previousVersions (not v1).
      const v2 = workflow<{ x: number }>({ name: "test", storage, version: "2" })
        .step("compute", ({ input }) => Pipeline.succeed(input.x * 100))
        .build();

      const v3 = workflow<{ x: number }>({
        name: "test",
        storage,
        version: "3",
        onVersionMismatch: "drain",
        previousVersions: [v2], // missing v1
      }).step("compute", ({ input }) => Pipeline.succeed(input.x * 1000));

      try {
        await v3.run({ workflowId: "drain-missing", input: { x: 1 } });
        expect(true).toBe(false);
      } catch (e) {
        expect(e).toBeInstanceOf(WorkflowVersionMismatchError);
        expect((e as InstanceType<typeof WorkflowVersionMismatchError>).actual).toBe("1");
        expect((e as InstanceType<typeof WorkflowVersionMismatchError>).expected).toBe("3");
      }
    });

    it("throws at workflow() construction when drain is set without previousVersions", () => {
      const storage = new InMemoryWorkflowStorage();
      expect(() =>
        workflow<{ x: number }>({
          name: "test",
          storage,
          version: "2",
          onVersionMismatch: "drain",
          // previousVersions omitted — should throw
        }),
      ).toThrow(/requires previousVersions/);
    });

    it("throws at construction when previousVersions entries lack `version`", () => {
      const storage = new InMemoryWorkflowStorage();
      const unversioned = workflow<{ x: number }>({ name: "test", storage })
        .step("x", ({ input }) => Pipeline.succeed(input))
        .build();

      expect(() =>
        workflow<{ x: number }>({
          name: "test",
          storage,
          version: "2",
          onVersionMismatch: "drain",
          previousVersions: [unversioned],
        }),
      ).toThrow(/must have a `version` field set/);
    });

    it("strict policy (default) still throws on mismatch even with previousVersions set", async () => {
      const storage = new InMemoryWorkflowStorage();
      const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" })
        .step("compute", ({ input }) => Pipeline.succeed(input))
        .build();
      await v1.run({ workflowId: "strict-with-prev", input: { x: 1 } });

      // previousVersions present but no drain policy — should still throw on mismatch.
      const v2 = workflow<{ x: number }>({
        name: "test",
        storage,
        version: "2",
        previousVersions: [v1], // present but ignored without "drain"
      }).step("compute", ({ input }) => Pipeline.succeed(input));

      await expect(v2.run({ workflowId: "strict-with-prev", input: { x: 1 } })).rejects.toThrow(
        WorkflowVersionMismatchError,
      );
    });

    it("fresh workflow uses current definition regardless of drain", async () => {
      const storage = new InMemoryWorkflowStorage();
      const v1 = workflow<{ x: number }>({ name: "test", storage, version: "1" })
        .step("c", ({ input }) => Pipeline.succeed(input.x * 10))
        .build();

      const v2 = workflow<{ x: number }>({
        name: "test",
        storage,
        version: "2",
        onVersionMismatch: "drain",
        previousVersions: [v1],
      }).step("c", ({ input }) => Pipeline.succeed(input.x * 100));

      // Brand new workflowId — drain should NOT kick in.
      const result = await v2.run({ workflowId: "fresh-drain", input: { x: 5 } });
      expect(result).toBe(500); // v2's logic

      const state = await storage.loadWorkflow("fresh-drain");
      expect(state!.version).toBe("2");
    });
  });
});
