import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { MapActivityRegistry } from "../activity-registry.ts";
import { compileWorkflow, WorkflowCompilationError } from "../workflow-compiler.ts";
import {
  validateWorkflowSchema,
  validateWorkflowSchemaSafe,
} from "../workflow-schema-validator.ts";
import type { WorkflowSchema } from "../workflow-schema.ts";

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const storage = new InMemoryWorkflowStorage();

const registry = new MapActivityRegistry({
  "transform.uppercase": () => (ctx) => Pipeline.succeed(String(ctx.prev).toUpperCase()),

  "transform.reverse": () => (ctx) =>
    Pipeline.succeed(String(ctx.prev).split("").reverse().join("")),

  "transform.concat": (config) => (ctx) => {
    const deps = ctx.deps as Record<string, string>;
    const separator = (config?.separator as string) ?? " ";
    return Pipeline.succeed(Object.values(deps).join(separator));
  },

  "data.split": (config) => (ctx) => {
    const separator = (config?.separator as string) ?? " ";
    return Pipeline.succeed(String(ctx.prev).split(separator));
  },

  "transform.exclaim": () => (ctx) => Pipeline.succeed(`${ctx.prev}!`),

  "transform.identity": () => (ctx) => Pipeline.succeed(ctx.prev),
});

// ---------------------------------------------------------------------------
// WorkflowSchema validation
// ---------------------------------------------------------------------------

describe("WorkflowSchema validation", () => {
  it("validates a correct schema", () => {
    const schema: WorkflowSchema = {
      version: 1,
      name: "test",
      steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
    };
    expect(() => validateWorkflowSchema(schema)).not.toThrow();
  });

  it("rejects schema with no steps", () => {
    const result = validateWorkflowSchemaSafe({
      version: 1,
      name: "empty",
      steps: [],
    });
    expect(result.success).toBe(false);
  });

  it("rejects invalid version", () => {
    const result = validateWorkflowSchemaSafe({
      version: 2,
      name: "bad-version",
      steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "x" }],
    });
    expect(result.success).toBe(false);
  });

  it("validates map step schema", () => {
    const schema: WorkflowSchema = {
      version: 1,
      name: "with-map",
      steps: [
        { type: "step", name: "split", dependsOn: [], activityRef: "data.split" },
        { type: "map", name: "process", arrayFrom: "split", activityRef: "transform.uppercase" },
      ],
    };
    expect(() => validateWorkflowSchema(schema)).not.toThrow();
  });

  it("validates schema with UI metadata", () => {
    const schema: WorkflowSchema = {
      version: 1,
      name: "with-ui",
      steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
      ui: {
        a: { x: 100, y: 200, color: "#ff0000", label: "Transform" },
      },
    };
    expect(() => validateWorkflowSchema(schema)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// compileWorkflow — linear chains
// ---------------------------------------------------------------------------

describe("compileWorkflow", () => {
  describe("linear chain", () => {
    it("compiles a single-step workflow", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "single-step",
          steps: [
            { type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" },
          ],
        },
        storage,
        registry,
      });

      const result = await definition.run({ workflowId: "compile-1", input: "hello" });
      expect(result).toBe("HELLO");
    });

    it("compiles a multi-step linear chain", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "linear-chain",
          steps: [
            { type: "step", name: "upper", dependsOn: [], activityRef: "transform.uppercase" },
            {
              type: "step",
              name: "reverse",
              dependsOn: ["upper"],
              activityRef: "transform.reverse",
            },
          ],
        },
        storage,
        registry,
      });

      const result = await definition.run({ workflowId: "compile-2", input: "hello" });
      expect(result).toBe("OLLEH");
    });
  });

  // ---------------------------------------------------------------------------
  // DAG workflows
  // ---------------------------------------------------------------------------

  describe("DAG", () => {
    it("compiles a diamond DAG", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "diamond",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
            {
              type: "step",
              name: "upper",
              dependsOn: ["start"],
              activityRef: "transform.uppercase",
            },
            {
              type: "step",
              name: "reverse",
              dependsOn: ["start"],
              activityRef: "transform.reverse",
            },
            {
              type: "step",
              name: "combine",
              dependsOn: ["upper", "reverse"],
              activityRef: "transform.concat",
              config: { separator: "-" },
            },
          ],
        },
        storage,
        registry,
      });

      const result = await definition.run({ workflowId: "compile-dag-1", input: "abc" });
      expect(result).toBe("ABC-cba");
    });
  });

  // ---------------------------------------------------------------------------
  // Config passing
  // ---------------------------------------------------------------------------

  describe("config", () => {
    it("passes config to activities", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "with-config",
          steps: [
            {
              type: "step",
              name: "split",
              dependsOn: [],
              activityRef: "data.split",
              config: { separator: "," },
            },
          ],
        },
        storage,
        registry,
      });

      const result = await definition.run({ workflowId: "compile-config-1", input: "a,b,c" });
      expect(result).toEqual(["a", "b", "c"]);
    });
  });

  // ---------------------------------------------------------------------------
  // Validation errors
  // ---------------------------------------------------------------------------

  describe("validation errors", () => {
    it("rejects unknown activity ref", () => {
      expect(() =>
        compileWorkflow({
          schema: {
            version: 1,
            name: "bad-ref",
            steps: [
              { type: "step", name: "a", dependsOn: [], activityRef: "nonexistent.activity" },
            ],
          },
          storage,
          registry,
        }),
      ).toThrow(WorkflowCompilationError);
    });

    it("rejects missing dependency", () => {
      expect(() =>
        compileWorkflow({
          schema: {
            version: 1,
            name: "bad-dep",
            steps: [
              {
                type: "step",
                name: "a",
                dependsOn: ["missing"],
                activityRef: "transform.uppercase",
              },
            ],
          },
          storage,
          registry,
        }),
      ).toThrow(WorkflowCompilationError);
    });

    it("rejects duplicate step names", () => {
      expect(() =>
        compileWorkflow({
          schema: {
            version: 1,
            name: "dup-names",
            steps: [
              { type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" },
              { type: "step", name: "a", dependsOn: [], activityRef: "transform.reverse" },
            ],
          },
          storage,
          registry,
        }),
      ).toThrow(WorkflowCompilationError);
    });

    it("error contains all issues", () => {
      try {
        compileWorkflow({
          schema: {
            version: 1,
            name: "multi-error",
            steps: [
              { type: "step", name: "a", dependsOn: ["missing"], activityRef: "nonexistent" },
            ],
          },
          storage,
          registry,
        });
        expect(true).toBe(false); // should not reach
      } catch (e) {
        const err = e as WorkflowCompilationError;
        expect(err.issues.length).toBeGreaterThanOrEqual(2);
        expect(err.issues.some((i) => i.includes("nonexistent"))).toBe(true);
        expect(err.issues.some((i) => i.includes("missing"))).toBe(true);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // WorkflowDefinition interface
  // ---------------------------------------------------------------------------

  describe("WorkflowDefinition", () => {
    it("has name and storage", () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "named-wf",
          steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
        },
        storage,
        registry,
      });

      expect(definition.name).toBe("named-wf");
      expect(definition.storage).toBe(storage);
    });

    it("runSafe returns data on success", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "safe-ok",
          steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
        },
        storage,
        registry,
      });

      const result = await definition.runSafe({ workflowId: "safe-1", input: "hi" });
      expect(result.data).toBe("HI");
      expect(result.error).toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// MapActivityRegistry
// ---------------------------------------------------------------------------

describe("MapActivityRegistry", () => {
  it("resolves registered activities", () => {
    const fn = registry.resolve("transform.uppercase");
    expect(typeof fn).toBe("function");
  });

  it("throws on unknown ref", () => {
    expect(() => registry.resolve("nope")).toThrow(/not found/);
  });

  it("has() checks existence", () => {
    expect(registry.has("transform.uppercase")).toBe(true);
    expect(registry.has("nope")).toBe(false);
  });

  it("list() returns all names", () => {
    const names = registry.list();
    expect(names).toContain("transform.uppercase");
    expect(names).toContain("data.split");
  });

  it("register() adds new activities", () => {
    const r = new MapActivityRegistry({});
    expect(r.has("test")).toBe(false);
    r.register("test", () => () => Pipeline.succeed("ok"));
    expect(r.has("test")).toBe(true);
  });
});
