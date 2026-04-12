import { describe, it, expect } from "bun:test";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { WorkflowVersionRegistry } from "./workflow-version-registry.ts";

describe("WorkflowVersionRegistry", () => {
  it("registers and resolves versioned workflows", () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    const v1 = workflow({ name: "order", storage, version: "1" })
      .stepAsync("validate", async () => "v1-result")
      .build();
    const v2 = workflow({ name: "order", storage, version: "2" })
      .stepAsync("verify", async () => "v2-result")
      .build();

    registry.register(v1);
    registry.register(v2);

    expect(registry.versions("order")).toEqual(["1", "2"]);
    expect(registry.latest("order")).toBe("2");
    expect(registry.resolve("order", "1")).toBe(v1);
    expect(registry.resolve("order", "2")).toBe(v2);
    expect(registry.resolve("order")).toBe(v2); // latest
  });

  it("throws when registering without version", () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    const noVersion = workflow({ name: "order", storage })
      .stepAsync("step", async () => "done")
      .build();

    expect(() => registry.register(noVersion)).toThrow("must have a version");
  });

  it("run() creates new workflow with latest version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    const v1 = workflow({ name: "order", storage, version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    const v2 = workflow({ name: "order", storage, version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    const result = await registry.run({
      workflowId: "new-1",
      input: {},
      name: "order",
    });
    expect(result).toBe("v2");

    const state = await storage.loadWorkflow("new-1");
    expect(state!.version).toBe("2");
  });

  it("run() resumes existing workflow with stored version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    // Register v1 and v2
    let v1Calls = 0;
    const v1 = workflow({ name: "order", storage, version: "1" })
      .stepAsync("step", async () => {
        v1Calls++;
        return "v1";
      })
      .build();
    const v2 = workflow({ name: "order", storage, version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    // Create workflow with v1 directly
    await v1.run({ workflowId: "old-1", input: {} });
    expect(v1Calls).toBe(1);

    // Reset for re-run test — start fresh run
    await storage.startFreshRun("old-1");

    // Registry.run should pick v1 for existing workflow
    const result = await registry.run({
      workflowId: "old-1",
      input: {},
      name: "order",
    });
    expect(result).toBe("v1"); // used v1, not v2
    expect(v1Calls).toBe(2);
  });

  it("run() throws if stored version not in registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    // Create workflow with v1
    const v1 = workflow({ name: "order", storage, version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    await v1.run({ workflowId: "old-1", input: {} });
    await storage.startFreshRun("old-1");

    // Only register v2 (v1 not registered)
    const v2 = workflow({ name: "order", storage, version: "2" })
      .stepAsync("step", async () => "v2")
      .build();
    registry.register(v2);

    await expect(registry.run({ workflowId: "old-1", input: {}, name: "order" })).rejects.toThrow(
      "version",
    );
  });

  it("names() lists registered workflows", () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    registry.register(
      workflow({ name: "order", storage, version: "1" })
        .stepAsync("s", async () => 1)
        .build(),
    );
    registry.register(
      workflow({ name: "payment", storage, version: "1" })
        .stepAsync("s", async () => 1)
        .build(),
    );

    expect(registry.names().sort()).toEqual(["order", "payment"]);
  });

  it("countByVersion reports in-flight workflows per version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = new WorkflowVersionRegistry();

    const v1 = workflow({ name: "order", storage, version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    const v2 = workflow({ name: "order", storage, version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    await v1.run({ workflowId: "v1-a", input: {} });
    await v1.run({ workflowId: "v1-b", input: {} });
    await v2.run({ workflowId: "v2-a", input: {} });

    const counts = await registry.countByVersion({
      name: "order",
      storage,
    });
    expect(counts.get("1")!.completed).toBe(2);
    expect(counts.get("2")!.completed).toBe(1);
  });
});
