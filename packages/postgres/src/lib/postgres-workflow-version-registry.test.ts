import { describe, it, expect, beforeEach } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow } from "@promin/workflow";
import { PostgresWorkflowVersionRegistry } from "./postgres-workflow-version-registry.ts";
import { migrate } from "./migrate.ts";
import { postgresDescribe } from "./test-utils.ts";

// ---------------------------------------------------------------------------
// Integration tests for PostgresWorkflowVersionRegistry.
// Requires a live Postgres container via testcontainers.
// ---------------------------------------------------------------------------

postgresDescribe("PostgresWorkflowVersionRegistry", { migrate }, (pg) => {
  let registry: PostgresWorkflowVersionRegistry;

  beforeEach(async () => {
    registry = new PostgresWorkflowVersionRegistry(pg.db);
    await pg.sql`TRUNCATE wf_workflow_registry`;
  });

  const wfV1 = workflow<{ x: number }>({ name: "my-wf", version: "1" })
    .step("double", ({ input }) => Pipeline.succeed(input.x * 2))
    .build();

  const wfV2 = workflow<{ x: number }>({ name: "my-wf", version: "2" })
    .step("double", ({ input }) => Pipeline.succeed(input.x * 2))
    .step("add", ({ prev }) => Pipeline.succeed(prev + 1))
    .build();

  describe("register + resolve", () => {
    it("round-trips a workflow by (name, version)", async () => {
      await registry.register(wfV1);
      const fetched = await registry.resolve("my-wf", "1");
      expect(fetched).not.toBeUndefined();
      expect(fetched!.name).toBe("my-wf");
      expect(fetched!.version).toBe("1");
      expect(fetched!.dag.steps.map((s) => s.name)).toEqual(["double"]);
    });

    it("resolve without version returns the latest registered", async () => {
      await registry.register(wfV1);
      await registry.register(wfV2);
      const latest = await registry.resolve("my-wf");
      expect(latest!.version).toBe("2");
      expect(latest!.dag.steps).toHaveLength(2);
    });

    it("returns undefined for unknown workflow", async () => {
      const result = await registry.resolve("no-such-wf");
      expect(result).toBeUndefined();
    });

    it("returns undefined for unknown version", async () => {
      await registry.register(wfV1);
      const result = await registry.resolve("my-wf", "99");
      expect(result).toBeUndefined();
    });

    it("register is idempotent (upsert)", async () => {
      await registry.register(wfV1);
      await registry.register(wfV1); // second register should not throw
      const versions = await registry.versions("my-wf");
      expect(versions).toHaveLength(1);
    });
  });

  describe("versions + names + latest", () => {
    it("versions() returns all registered versions in order", async () => {
      await registry.register(wfV1);
      await registry.register(wfV2);
      const versions = await registry.versions("my-wf");
      expect(versions).toEqual(["1", "2"]);
    });

    it("latest() returns the most recently registered version", async () => {
      await registry.register(wfV1);
      await registry.register(wfV2);
      expect(await registry.latest("my-wf")).toBe("2");
    });

    it("latest() returns undefined for unknown name", async () => {
      expect(await registry.latest("ghost")).toBeUndefined();
    });

    it("names() returns distinct workflow names", async () => {
      const other = workflow<void>({ name: "other-wf", version: "1" })
        .step("noop", () => Pipeline.succeed(undefined))
        .build();
      await registry.register(wfV1);
      await registry.register(other);
      const names = await registry.names();
      expect(names).toContain("my-wf");
      expect(names).toContain("other-wf");
    });
  });

  describe("deregister", () => {
    it("removes the (name, version) entry", async () => {
      await registry.register(wfV1);
      await registry.register(wfV2);
      await registry.deregister("my-wf", "1");
      expect(await registry.resolve("my-wf", "1")).toBeUndefined();
      expect(await registry.resolve("my-wf", "2")).toBeDefined();
    });
  });

  describe("stub workflow", () => {
    it("reconstructed stub has correct DAG structure", async () => {
      await registry.register(wfV2);
      const stub = await registry.resolve("my-wf", "2");
      expect(stub!.dag.steps).toHaveLength(2);
      expect(stub!.dag.steps[1]!.dependsOn).toContain("double");
    });

    it("throws on version without version field", async () => {
      const noVersion = workflow<void>({ name: "no-ver" })
        .step("noop", () => Pipeline.succeed(undefined))
        .build();
      await expect(registry.register(noVersion)).rejects.toThrow("must have a version");
    });
  });
});
