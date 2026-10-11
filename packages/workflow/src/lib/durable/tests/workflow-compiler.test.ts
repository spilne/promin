import { succeed } from "@spilne/perfect-core";
import { describe, it, expect } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { MapActivityRegistry } from "../activity-registry.ts";
import { compileWorkflow, WorkflowCompilationError } from "../workflow-compiler.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import {
  validateWorkflowSchema,
  validateWorkflowSchemaSafe,
} from "../workflow-schema-validator.ts";
import type { WorkflowSchema } from "../workflow-schema.ts";

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const registry = new MapActivityRegistry({
  "transform.uppercase": () => (ctx) => succeed(String(ctx.prev).toUpperCase()),

  "transform.reverse": () => (ctx) => succeed(String(ctx.prev).split("").reverse().join("")),

  "transform.concat": (config) => (ctx) => {
    const deps = ctx.deps as Record<string, string>;
    const separator = (config?.separator as string) ?? " ";
    return succeed(Object.values(deps).join(separator));
  },

  "data.split": (config) => (ctx) => {
    const separator = (config?.separator as string) ?? " ";
    return succeed(String(ctx.prev).split(separator));
  },

  "transform.exclaim": () => (ctx) => succeed(`${ctx.prev}!`),

  "transform.identity": () => (ctx) => succeed(ctx.prev),

  "predicate.long": () => (ctx) => succeed(String(ctx.prev).length > 3),

  "transform.true": () => () => succeed("long"),

  "transform.false": () => () => succeed("short"),
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

  it("validates sleep, signal, approval, branch and parallel step schemas", () => {
    const schema: WorkflowSchema = {
      version: 1,
      name: "parity",
      steps: [
        { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
        { type: "sleep", name: "pause", dependsOn: ["start"], ms: 1 },
        {
          type: "parallel",
          name: "fanout",
          dependsOn: ["pause"],
          branches: {
            upper: { activityRef: "transform.uppercase" },
            reverse: { activityRef: "transform.reverse" },
          },
        },
        {
          type: "branch",
          name: "route",
          dependsOn: ["start"],
          conditionRef: "predicate.long",
          ifTrue: { activityRef: "transform.true" },
          ifFalse: { activityRef: "transform.false" },
        },
        { type: "signal", name: "wait", dependsOn: ["route"], signalName: "go", timeoutMs: 10 },
        { type: "approval", name: "approve", dependsOn: ["wait"], signalName: "approved" },
      ],
    };
    expect(() => validateWorkflowSchema(schema)).not.toThrow();
  });

  it("rejects a parallel branch without an activityRef", () => {
    const result = validateWorkflowSchemaSafe({
      version: 1,
      name: "bad-parallel",
      steps: [{ type: "parallel", name: "p", dependsOn: [], branches: { a: { config: {} } } }],
    });
    expect(result.success).toBe(false);
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
        registry,
      });

      const result = await runner.run({
        workflow: definition,
        workflowId: "compile-1",
        input: "hello",
      });
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
        registry,
      });

      const result = await runner.run({
        workflow: definition,
        workflowId: "compile-2",
        input: "hello",
      });
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
        registry,
      });

      const result = await runner.run({
        workflow: definition,
        workflowId: "compile-dag-1",
        input: "abc",
      });
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
        registry,
      });

      const result = await runner.run({
        workflow: definition,
        workflowId: "compile-config-1",
        input: "a,b,c",
      });
      expect(result).toEqual(["a", "b", "c"]);
    });
  });

  // ---------------------------------------------------------------------------
  // sleep / signal / approval / branch / parallel
  // ---------------------------------------------------------------------------

  describe("sleep, signal, approval, branch and parallel nodes", () => {
    it("compiles branch nodes", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "branch-schema",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
            {
              type: "branch",
              name: "route",
              dependsOn: ["start"],
              conditionRef: "predicate.long",
              ifTrue: { activityRef: "transform.true" },
              ifFalse: { activityRef: "transform.false" },
            },
          ],
        },
        registry,
      });

      await expect(
        runner.run({ workflow: definition, workflowId: "compile-branch-1", input: "hello" }),
      ).resolves.toBe("long");
      await expect(
        runner.run({ workflow: definition, workflowId: "compile-branch-2", input: "hi" }),
      ).resolves.toBe("short");
    });

    it("compiles parallel nodes", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "parallel-schema",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
            {
              type: "parallel",
              name: "fanout",
              dependsOn: ["start"],
              branches: {
                upper: { activityRef: "transform.uppercase" },
                reverse: { activityRef: "transform.reverse" },
              },
            },
          ],
        },
        registry,
      });

      await expect(
        runner.run({ workflow: definition, workflowId: "compile-parallel-1", input: "abc" }),
      ).resolves.toEqual({ upper: "ABC", reverse: "cba" });
      expect(definition.dag.steps.map((s) => s.name).sort()).toEqual([
        "fanout",
        "fanout.reverse",
        "fanout.upper",
        "start",
      ]);
    });

    it("gives a root parallel node the workflow input", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "root-parallel-schema",
          steps: [
            {
              type: "parallel",
              name: "fanout",
              dependsOn: [],
              branches: { upper: { activityRef: "transform.uppercase" } },
            },
          ],
        },
        registry,
      });

      await expect(
        runner.run({ workflow: definition, workflowId: "compile-parallel-root", input: "abc" }),
      ).resolves.toEqual({ upper: "ABC" });
    });

    it("compiles sleep nodes that pass their dependency's value through", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "sleep-schema",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.uppercase" },
            { type: "sleep", name: "pause", dependsOn: ["start"], ms: 0 },
            {
              type: "step",
              name: "finish",
              dependsOn: ["pause"],
              activityRef: "transform.exclaim",
            },
          ],
        },
        registry,
      });

      // The first run suspends on the timer; the run after it is due wakes.
      const first = await runner.runSafe({
        workflow: definition,
        workflowId: "compile-sleep-1",
        input: "zzz",
      });
      expect((first.error as { _tag?: string } | null)?._tag).toBe("WorkflowSuspendedError");
      await expect(
        runner.run({ workflow: definition, workflowId: "compile-sleep-1", input: "zzz" }),
      ).resolves.toBe("ZZZ!");
    });

    it("compiles signal nodes", async () => {
      const localStorage = new InMemoryWorkflowStorage();
      const localRunner = createWorkflowRunner({ storage: localStorage });
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "signal-schema",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
            { type: "signal", name: "wait", dependsOn: ["start"], signalName: "go" },
          ],
        },
        registry,
      });

      const first = await localRunner.runSafe({
        workflow: definition,
        workflowId: "compile-signal-1",
        input: "x",
      });
      expect((first.error as { _tag?: string } | null)?._tag).toBe("WorkflowSuspendedError");
      await localStorage.deliverSignal({
        workflowId: "compile-signal-1",
        signalName: "go",
        payload: { n: 1 },
      });
      await expect(
        localRunner.run({ workflow: definition, workflowId: "compile-signal-1", input: "x" }),
      ).resolves.toEqual({ n: 1 });
    });

    it("compiles approval nodes as waitForSignal sugar", async () => {
      const localStorage = new InMemoryWorkflowStorage();
      const localRunner = createWorkflowRunner({ storage: localStorage });
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "approval-schema",
          steps: [
            { type: "step", name: "start", dependsOn: [], activityRef: "transform.identity" },
            { type: "approval", name: "approve", dependsOn: ["start"] },
            {
              type: "step",
              name: "finish",
              dependsOn: ["approve"],
              activityRef: "transform.identity",
            },
          ],
        },
        registry,
      });

      await localRunner.runSafe({
        workflow: definition,
        workflowId: "compile-approval-1",
        input: "go",
      });
      // The default signal name is "<step>-approved".
      await localStorage.deliverSignal({
        workflowId: "compile-approval-1",
        signalName: "approve-approved",
        payload: { approved: true },
      });

      await expect(
        localRunner.run({ workflow: definition, workflowId: "compile-approval-1", input: "go" }),
      ).resolves.toEqual({ approved: true });
    });
  });

  // ---------------------------------------------------------------------------
  // Validation errors
  // ---------------------------------------------------------------------------

  describe("validation errors", () => {
    it("rejects unknown activity refs in branch and parallel nodes", () => {
      let issues: string[] = [];
      try {
        compileWorkflow({
          schema: {
            version: 1,
            name: "bad-refs",
            steps: [
              {
                type: "branch",
                name: "route",
                dependsOn: [],
                conditionRef: "missing.condition",
                ifTrue: { activityRef: "transform.true" },
                ifFalse: { activityRef: "missing.false" },
              },
              {
                type: "parallel",
                name: "fanout",
                dependsOn: ["route"],
                branches: { a: { activityRef: "missing.branch" } },
              },
            ],
          },
          registry,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(WorkflowCompilationError);
        issues = (e as WorkflowCompilationError).issues;
      }
      expect(issues).toHaveLength(3);
      expect(issues[0]).toContain('Branch step "route" condition references unknown activity');
      expect(issues[1]).toContain('Branch step "route" ifFalse references unknown activity');
      expect(issues[2]).toContain(
        'Parallel step "fanout" branch "a" references unknown activity "missing.branch"',
      );
    });

    it("rejects a parallel node without branches", () => {
      expect(() =>
        compileWorkflow({
          schema: {
            version: 1,
            name: "empty-parallel",
            steps: [{ type: "parallel", name: "fanout", dependsOn: [], branches: {} }],
          },
          registry,
        }),
      ).toThrow('Parallel step "fanout" requires at least one branch');
    });

    it("rejects a signal node depending on a missing step", () => {
      expect(() =>
        compileWorkflow({
          schema: {
            version: 1,
            name: "bad-signal-dep",
            steps: [{ type: "signal", name: "wait", dependsOn: ["nope"], signalName: "go" }],
          },
          registry,
        }),
      ).toThrow('Step "wait" depends on "nope" which does not exist');
    });

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
  // Returned Workflow shape
  // ---------------------------------------------------------------------------

  describe("compiled Workflow", () => {
    it("carries its name", () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "named-wf",
          steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
        },
        registry,
      });

      expect(definition.name).toBe("named-wf");
    });

    it("runSafe returns data on success", async () => {
      const definition = compileWorkflow({
        schema: {
          version: 1,
          name: "safe-ok",
          steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.uppercase" }],
        },
        registry,
      });

      const result = await runner.runSafe({
        workflow: definition,
        workflowId: "safe-1",
        input: "hi",
      });
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
    const fn = registry.resolve({ ref: "transform.uppercase" });
    expect(typeof fn).toBe("function");
  });

  it("throws on unknown ref", () => {
    expect(() => registry.resolve({ ref: "nope" })).toThrow(/not found/);
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
    r.register({ ref: "test", factory: () => () => succeed("ok") });
    expect(r.has("test")).toBe(true);
  });
});
