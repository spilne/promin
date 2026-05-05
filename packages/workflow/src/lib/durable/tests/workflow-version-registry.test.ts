import { describe, it, expect } from "bun:test";
import { workflow } from "../durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import {
  createWorkflowVersionRegistry,
  WorkflowVersionRegistry,
} from "../workflow-version-registry.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";

describe("WorkflowVersionRegistry", () => {
  it("registers and resolves versioned workflows", () => {
    const registry = createWorkflowVersionRegistry();

    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("validate", async () => "v1-result")
      .build();
    const v2 = workflow({ name: "order", version: "2" })
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
    const registry = createWorkflowVersionRegistry();

    const noVersion = workflow({ name: "order" })
      .stepAsync("step", async () => "done")
      .build();

    expect(() => registry.register(noVersion)).toThrow("must have a version");
  });

  it("run() creates new workflow with latest version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    const v2 = workflow({ name: "order", version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({
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
    const registry = createWorkflowVersionRegistry();

    // Register v1 and v2
    let v1Calls = 0;
    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("step", async () => {
        v1Calls++;
        return "v1";
      })
      .build();
    const v2 = workflow({ name: "order", version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });

    // Create workflow with v1 directly
    await runner.run({ workflow: v1, workflowId: "old-1", input: {} });
    expect(v1Calls).toBe(1);

    // Reset for re-run test — start fresh run
    await storage.startFreshRun("old-1");

    // Registry.run should pick v1 for existing workflow
    const result = await runner.run({
      workflowId: "old-1",
      input: {},
      name: "order",
    });
    expect(result).toBe("v1"); // used v1, not v2
    expect(v1Calls).toBe(2);
  });

  it("run() throws if stored version not in registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    // Create workflow with v1
    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    const seedRunner = createWorkflowRunner({ storage });
    await seedRunner.run({ workflow: v1, workflowId: "old-1", input: {} });
    await storage.startFreshRun("old-1");

    // Only register v2 (v1 not registered)
    const v2 = workflow({ name: "order", version: "2" })
      .stepAsync("step", async () => "v2")
      .build();
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });
    await expect(runner.run({ workflowId: "old-1", input: {}, name: "order" })).rejects.toThrow(
      "version",
    );
  });

  it("names() lists registered workflows", () => {
    const registry = createWorkflowVersionRegistry();

    registry.register(
      workflow({ name: "order", version: "1" })
        .stepAsync("s", async () => 1)
        .build(),
    );
    registry.register(
      workflow({ name: "payment", version: "1" })
        .stepAsync("s", async () => 1)
        .build(),
    );

    expect(registry.names().sort()).toEqual(["order", "payment"]);
  });

  it("countByVersion reports in-flight workflows per version", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("step", async () => "v1")
      .build();
    const v2 = workflow({ name: "order", version: "2" })
      .stepAsync("step", async () => "v2")
      .build();

    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage });
    await runner.run({ workflow: v1, workflowId: "v1-a", input: {} });
    await runner.run({ workflow: v1, workflowId: "v1-b", input: {} });
    await runner.run({ workflow: v2, workflowId: "v2-a", input: {} });

    const counts = await registry.countByVersion({
      name: "order",
      storage,
    });
    expect(counts.get("1")!.completed).toBe(2);
    expect(counts.get("2")!.completed).toBe(1);
  });

  // -------------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------------

  it("multiple workflow types in same registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    const order = workflow({ name: "order", version: "1" })
      .stepAsync("s", async () => "order-result")
      .build();
    const payment = workflow({ name: "payment", version: "1" })
      .stepAsync("s", async () => "payment-result")
      .build();

    registry.register(order);
    registry.register(payment);

    const runner = createWorkflowRunner({ storage, registry });
    const r1 = await runner.run({ workflowId: "o1", name: "order", input: {} });
    const r2 = await runner.run({ workflowId: "p1", name: "payment", input: {} });

    expect(r1).toBe("order-result");
    expect(r2).toBe("payment-result");
    expect(registry.names().sort()).toEqual(["order", "payment"]);
  });

  it("re-registering same version overwrites definition", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    const v1a = workflow({ name: "order", version: "1" })
      .stepAsync("s", async () => "first")
      .build();
    const v1b = workflow({ name: "order", version: "1" })
      .stepAsync("s", async () => "replaced")
      .build();

    registry.register(v1a);
    registry.register(v1b);

    expect(registry.versions("order")).toEqual(["1"]); // still one version
    const runner = createWorkflowRunner({ storage, registry });
    const result = await runner.run({ workflowId: "r1", name: "order", input: {} });
    expect(result).toBe("replaced"); // uses the latest registration
  });

  it("resolve returns undefined for non-existent workflow name", () => {
    const registry = createWorkflowVersionRegistry();
    expect(registry.resolve("nonexistent")).toBeUndefined();
    expect(registry.resolve("nonexistent", "1")).toBeUndefined();
    expect(registry.latest("nonexistent")).toBeUndefined();
    expect(registry.versions("nonexistent")).toEqual([]);
  });

  it("run throws for non-existent workflow name", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();
    const runner = createWorkflowRunner({ storage, registry });
    await expect(runner.run({ workflowId: "x", name: "nonexistent", input: {} })).rejects.toThrow(
      "No workflow",
    );
  });

  it("latest is always the last registered version", () => {
    const registry = createWorkflowVersionRegistry();

    registry.register(
      workflow({ name: "order", version: "3" })
        .stepAsync("s", async () => 1)
        .build(),
    );
    registry.register(
      workflow({ name: "order", version: "1" })
        .stepAsync("s", async () => 1)
        .build(),
    );
    registry.register(
      workflow({ name: "order", version: "2" })
        .stepAsync("s", async () => 1)
        .build(),
    );

    // Last registered wins, regardless of version number
    expect(registry.latest("order")).toBe("2");
  });

  it("different versions can have different step structures", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    // v1: 2 steps
    const v1 = workflow({ name: "order", version: "1" })
      .stepAsync("validate", async () => "validated")
      .stepAsync("charge", async () => "v1-charged")
      .build();

    // v2: 3 steps (different names, extra step)
    const v2 = workflow({ name: "order", version: "2" })
      .stepAsync("verify", async () => "verified")
      .stepAsync("charge", async () => "v2-charged")
      .stepAsync("notify", async () => "v2-notified")
      .build();

    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });

    // Create a v1 workflow directly, then resume via registry
    await runner.run({ workflow: v1, workflowId: "v1-1", input: {} });
    await storage.startFreshRun("v1-1");
    const r1 = await runner.run({ workflowId: "v1-1", name: "order", input: {} });

    // New workflow gets v2
    const r2 = await runner.run({ workflowId: "v2-1", name: "order", input: {} });

    expect(r1).toBe("v1-charged"); // resumed with v1 definition
    expect(r2).toBe("v2-notified"); // new, used v2
  });

  it("concurrent workflows on different versions", async () => {
    const storage = new InMemoryWorkflowStorage();
    const registry = createWorkflowVersionRegistry();

    let v1Count = 0;
    let v2Count = 0;

    const v1 = workflow({ name: "job", version: "1" })
      .stepAsync("run", async () => {
        v1Count++;
        return "v1";
      })
      .build();
    const v2 = workflow({ name: "job", version: "2" })
      .stepAsync("run", async () => {
        v2Count++;
        return "v2";
      })
      .build();

    registry.register(v1);
    registry.register(v2);

    const runner = createWorkflowRunner({ storage, registry });

    // Create v1 workflows directly
    await runner.run({ workflow: v1, workflowId: "j1", input: {} });
    await runner.run({ workflow: v1, workflowId: "j2", input: {} });

    // Fresh runs to simulate resume
    await storage.startFreshRun("j1");
    await storage.startFreshRun("j2");

    // Run all concurrently — j1,j2 should use v1, j3,j4 use v2 (latest)
    const results = await Promise.all([
      runner.run({ workflowId: "j1", name: "job", input: {} }),
      runner.run({ workflowId: "j2", name: "job", input: {} }),
      runner.run({ workflowId: "j3", name: "job", input: {} }),
      runner.run({ workflowId: "j4", name: "job", input: {} }),
    ]);

    expect(results).toEqual(["v1", "v1", "v2", "v2"]);
    expect(v1Count).toBe(4); // 2 original + 2 resumed
    expect(v2Count).toBe(2);
  });

  // ---------------------------------------------------------------------------
  // Fluent scoped builder + drain detection
  // ---------------------------------------------------------------------------

  describe("WorkflowVersionRegistry.for() scoped builder", () => {
    it("returns a builder scoped to one workflow name", () => {
      const v1 = workflow({ name: "order", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();
      const v2 = workflow({ name: "order", version: "2" })
        .stepAsync("x", async () => "v2")
        .build();

      const scoped = WorkflowVersionRegistry.for("order").register(v1).register(v2);

      expect(scoped.versions()).toEqual(["1", "2"]);
      expect(scoped.latest()).toBe("2");
      expect(scoped.resolve("1")).toBe(v1);
    });

    it("rejects definitions with a mismatched name", () => {
      const wrongName = workflow({ name: "billing", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();

      const scoped = WorkflowVersionRegistry.for("order");
      expect(() => scoped.register(wrongName)).toThrow(/mismatched|name/i);
    });

    it("deregister removes a version", () => {
      const v1 = workflow({ name: "order", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();
      const v2 = workflow({ name: "order", version: "2" })
        .stepAsync("x", async () => "v2")
        .build();

      const scoped = WorkflowVersionRegistry.for("order").register(v1).register(v2);
      scoped.deregister("1");

      expect(scoped.versions()).toEqual(["2"]);
      expect(scoped.resolve("1")).toBeUndefined();
    });
  });

  describe("onDrained + autoDeregister", () => {
    it("fires onDrained when a version's in-flight count hits zero", async () => {
      const storage = new InMemoryWorkflowStorage();
      const drained: Array<[string, string]> = [];
      const registry = createWorkflowVersionRegistry({
        onDrained: (name, version) => {
          drained.push([name, version]);
        },
      });

      const v1 = workflow({ name: "order", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();
      const v2 = workflow({ name: "order", version: "2" })
        .stepAsync("x", async () => "v2")
        .build();

      registry.register(v1);
      registry.register(v2);

      // Create a v1 workflow and complete it (contributes to v1's completed counter).
      const runner = createWorkflowRunner({ storage });
      await runner.run({ workflow: v1, workflowId: "o1", input: {} });

      // Trigger the drain detection.
      await registry.countByVersion({ name: "order", storage });

      // v1 has 0 running, should fire onDrained.
      expect(drained.some(([n, v]) => n === "order" && v === "1")).toBe(true);
    });

    it("autoDeregister removes drained versions (except the latest)", async () => {
      const storage = new InMemoryWorkflowStorage();
      const registry = createWorkflowVersionRegistry({ autoDeregister: true });

      const v1 = workflow({ name: "order", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();
      const v2 = workflow({ name: "order", version: "2" })
        .stepAsync("x", async () => "v2")
        .build();

      registry.register(v1);
      registry.register(v2);

      const runner = createWorkflowRunner({ storage });
      await runner.run({ workflow: v1, workflowId: "o1", input: {} });

      // v1 drained (0 running), v2 has no workflows at all (also drained)
      await registry.countByVersion({ name: "order", storage });

      // v1 should be auto-deregistered; v2 (latest) stays even if drained.
      expect(registry.versions("order")).toContain("2");
      expect(registry.versions("order")).not.toContain("1");
    });

    it("doesn't double-fire onDrained for the same version", async () => {
      const storage = new InMemoryWorkflowStorage();
      let count = 0;
      const registry = createWorkflowVersionRegistry({
        onDrained: () => {
          count++;
        },
      });

      const v1 = workflow({ name: "job", version: "1" })
        .stepAsync("x", async () => "v1")
        .build();
      registry.register(v1);

      await registry.countByVersion({ name: "job", storage });
      await registry.countByVersion({ name: "job", storage });
      await registry.countByVersion({ name: "job", storage });

      expect(count).toBe(1);
    });
  });

  describe("runner routing — promote / findActive drives dispatch", () => {
    it("by default (no promote) routes to latest registered — pre-promote behaviour preserved", async () => {
      const storage = new InMemoryWorkflowStorage();
      const registry = createWorkflowVersionRegistry();

      let v1Calls = 0;
      let v2Calls = 0;
      const v1 = workflow<{ n: number }>({ name: "compute", version: "1" })
        .stepAsync("x", async ({ input }) => {
          v1Calls++;
          return input.n;
        })
        .build();
      const v2 = workflow<{ n: number }>({ name: "compute", version: "2" })
        .stepAsync("x", async ({ input }) => {
          v2Calls++;
          return input.n * 2;
        })
        .build();
      registry.register(v1);
      registry.register(v2); // v2 is "latest"

      const runner = createWorkflowRunner({ storage, registry });
      await runner.run({ name: "compute", workflowId: "r1", input: { n: 5 } });

      expect(v1Calls).toBe(0);
      expect(v2Calls).toBe(1);
    });

    it("when v1 is promoted, runner routes to v1 even though v2 is latest-registered", async () => {
      const storage = new InMemoryWorkflowStorage();
      const registry = createWorkflowVersionRegistry();

      let v1Calls = 0;
      let v2Calls = 0;
      const v1 = workflow<{ n: number }>({ name: "compute", version: "1" })
        .stepAsync("x", async ({ input }) => {
          v1Calls++;
          return input.n;
        })
        .build();
      const v2 = workflow<{ n: number }>({ name: "compute", version: "2" })
        .stepAsync("x", async ({ input }) => {
          v2Calls++;
          return input.n * 2;
        })
        .build();
      registry.register(v1);
      registry.register(v2);
      registry.promote("compute", "1"); // override "latest = v2"

      const runner = createWorkflowRunner({ storage, registry });
      await runner.run({ name: "compute", workflowId: "r2", input: { n: 5 } });

      expect(v1Calls).toBe(1);
      expect(v2Calls).toBe(0);
    });

    it("explicit version on .run() always wins over the active pointer", async () => {
      const storage = new InMemoryWorkflowStorage();
      const registry = createWorkflowVersionRegistry();

      let v1Calls = 0;
      let v2Calls = 0;
      const v1 = workflow<{ n: number }>({ name: "compute", version: "1" })
        .stepAsync("x", async () => {
          v1Calls++;
          return "v1";
        })
        .build();
      const v2 = workflow<{ n: number }>({ name: "compute", version: "2" })
        .stepAsync("x", async () => {
          v2Calls++;
          return "v2";
        })
        .build();
      registry.register(v1);
      registry.register(v2);
      registry.promote("compute", "1");

      const runner = createWorkflowRunner({ storage, registry });
      await runner.run({ name: "compute", version: "2", workflowId: "r3", input: { n: 5 } });

      expect(v1Calls).toBe(0);
      expect(v2Calls).toBe(1);
    });

    it("rollback shifts the routing target", async () => {
      const storage = new InMemoryWorkflowStorage();
      const registry = createWorkflowVersionRegistry();

      let v1Calls = 0;
      let v2Calls = 0;
      const v1 = workflow<{ n: number }>({ name: "compute", version: "1" })
        .stepAsync("x", async () => {
          v1Calls++;
          return "v1";
        })
        .build();
      const v2 = workflow<{ n: number }>({ name: "compute", version: "2" })
        .stepAsync("x", async () => {
          v2Calls++;
          return "v2";
        })
        .build();
      registry.register(v1);
      registry.register(v2);

      registry.promote("compute", "2");
      const runner = createWorkflowRunner({ storage, registry });
      await runner.run({ name: "compute", workflowId: "r4a", input: { n: 5 } });
      expect(v2Calls).toBe(1);

      registry.rollback({ name: "compute", toVersion: "1" });
      await runner.run({ name: "compute", workflowId: "r4b", input: { n: 5 } });
      expect(v1Calls).toBe(1);
    });
  });
});
