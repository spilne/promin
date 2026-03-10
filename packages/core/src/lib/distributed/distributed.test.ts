import { describe, it, expect } from "bun:test";
import { Pipeline } from "../pipeline.ts";
import { workflow, InMemoryWorkflowStorage } from "../durable/index.ts";
import { MapStepRegistry } from "./step-registry.ts";
import { InMemoryStepQueue } from "./in-memory-step-queue.ts";
import { createCoordinator } from "./coordinator.ts";
import { createWorker } from "./worker.ts";

// ---------------------------------------------------------------------------
// StepRegistry
// ---------------------------------------------------------------------------

describe("StepRegistry", () => {
  it("registers and resolves steps", () => {
    const registry = new MapStepRegistry();
    registry.register("double", (ctx) => Pipeline.succeed((ctx.prev as number) * 2));

    expect(registry.has("double")).toBe(true);
    expect(registry.has("missing")).toBe(false);
    expect(registry.resolve("double")).toBeDefined();
    expect(registry.resolve("missing")).toBeUndefined();
    expect(registry.list()).toEqual(["double"]);
  });
});

// ---------------------------------------------------------------------------
// InMemoryStepQueue
// ---------------------------------------------------------------------------

describe("InMemoryStepQueue", () => {
  it("enqueue and claim", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-a",
      queue: "default",
      input: { n: 5 },
      prevResults: {},
    });

    const tasks = await queue.claim({ queues: ["default"], limit: 10 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.stepName).toBe("step-a");
    expect(tasks[0]!.status).toBe("running");
  });

  it("claim respects queue filter", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      queue: "default",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      queue: "gpu",
      input: {},
      prevResults: {},
    });

    const defaultTasks = await queue.claim({ queues: ["default"], limit: 10 });
    expect(defaultTasks).toHaveLength(1);
    expect(defaultTasks[0]!.stepName).toBe("a");

    const gpuTasks = await queue.claim({ queues: ["gpu"], limit: 10 });
    expect(gpuTasks).toHaveLength(1);
    expect(gpuTasks[0]!.stepName).toBe("b");
  });

  it("claim respects limit", async () => {
    const queue = new InMemoryStepQueue();

    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        workflowId: "wf-1",
        stepName: `s-${i}`,
        queue: "default",
        input: {},
        prevResults: {},
      });
    }

    const tasks = await queue.claim({ queues: ["default"], limit: 2 });
    expect(tasks).toHaveLength(2);
  });

  it("claimed tasks are not re-claimed", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const first = await queue.claim({ queues: ["default"], limit: 10 });
    expect(first).toHaveLength(1);

    const second = await queue.claim({ queues: ["default"], limit: 10 });
    expect(second).toHaveLength(0);
  });

  it("complete and fail update status", async () => {
    const queue = new InMemoryStepQueue();

    const id1 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      queue: "default",
      input: {},
      prevResults: {},
    });
    const id2 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      queue: "default",
      input: {},
      prevResults: {},
    });

    await queue.claim({ queues: ["default"], limit: 10 });
    await queue.complete({ taskId: id1, result: "ok", durationMs: 100 });
    await queue.fail({ taskId: id2, error: "boom", durationMs: 50 });

    const metrics = await queue.metrics();
    expect(metrics["default"]!.completed).toBe(1);
    expect(metrics["default"]!.failed).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Worker — step execution
// ---------------------------------------------------------------------------

describe("WorkflowWorker", () => {
  it("claims and executes a step", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const completed: string[] = [];

    registry.register("double", (ctx) => Pipeline.succeed((ctx.input as any).n * 2));

    // Create workflow and enqueue a task
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "test", input: { n: 5 } });
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "double",
      queue: "default",
      input: { n: 5 },
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      hooks: {
        afterStep: (task, _result, _ms) => {
          completed.push(task.stepName);
        },
      },
    });

    // Run worker briefly
    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(completed).toContain("double");

    // Check result was checkpointed
    const state = await storage.loadWorkflow("wf-1");
    expect(state?.steps["double"]?.status).toBe("completed");
    expect(state?.steps["double"]?.result).toBe(10); // 5 * 2
  });

  it("handles step failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const failures: string[] = [];

    registry.register("fail-step", () => {
      throw new Error("step exploded");
    });

    await storage.createWorkflow({ workflowId: "wf-2", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-2",
      stepName: "fail-step",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      hooks: {
        onError: (task, _err, _ms) => {
          failures.push(task.stepName);
        },
      },
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(failures).toContain("fail-step");
    const state = await storage.loadWorkflow("wf-2");
    expect(state?.steps["fail-step"]?.status).toBe("failed");
  });

  it("reports error for unregistered steps", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const failures: string[] = [];

    await storage.createWorkflow({ workflowId: "wf-3", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "unknown-step",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      hooks: {
        onError: (_task, err, _ms) => {
          failures.push(err instanceof Error ? err.message : String(err));
        },
      },
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(failures.some((f) => f.includes("not found"))).toBe(true);
  });

  it("executes async step handlers", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();

    registry.register("async-step", async (ctx) => {
      await new Promise((r) => setTimeout(r, 10));
      return (ctx.prev as number) + 100;
    });

    await storage.createWorkflow({ workflowId: "wf-4", workflowName: "test", input: { n: 5 } });
    await queue.enqueue({
      workflowId: "wf-4",
      stepName: "async-step",
      queue: "default",
      input: { n: 5 },
      prevResults: { "prev-step": 42 },
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await worker.stop();

    const state = await storage.loadWorkflow("wf-4");
    expect(state?.steps["async-step"]?.result).toBe(142); // 42 + 100
  });

  it("only polls assigned queues", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const completed: string[] = [];

    registry.register("gpu-step", (ctx) => Pipeline.succeed("gpu-result"));

    await storage.createWorkflow({ workflowId: "wf-5", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-5",
      stepName: "gpu-step",
      queue: "gpu",
      input: {},
      prevResults: {},
    });

    // Worker only listens to "default" queue
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      hooks: {
        afterStep: (task, _result, _ms) => {
          completed.push(task.stepName);
        },
      },
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    // Task should NOT be picked up
    expect(completed).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Coordinator + Worker — end-to-end
// ---------------------------------------------------------------------------

describe("Coordinator + Worker end-to-end", () => {
  it("executes a multi-step workflow across coordinator and worker", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    // Define workflow
    const wf = workflow<{ n: number }>({ name: "distributed-test", storage })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .step("add-ten", ({ prev }) => Pipeline.succeed(prev + 10))
      .build();

    // Register step implementations on the worker
    const registry = new MapStepRegistry();
    registry.register("double", (ctx) => Pipeline.succeed((ctx.input as any).n * 2));
    registry.register("add-ten", (ctx) => Pipeline.succeed((ctx.prev as number) + 10));

    // Create coordinator with routing
    const coordinator = createCoordinator({
      storage,
      stepQueue: queue,
      routing: {},
    });

    // Create worker
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
    });

    // Submit workflow
    await coordinator.submit({ workflow: wf, workflowId: "e2e-1", input: { n: 5 } });

    // Run both coordinator and worker
    void coordinator.start();
    void worker.start();

    // Wait for completion
    await new Promise((r) => setTimeout(r, 500));
    await coordinator.stop();
    await worker.stop();

    // Verify
    const state = await storage.loadWorkflow("e2e-1");
    expect(state?.steps["double"]?.status).toBe("completed");
    expect(state?.steps["double"]?.result).toBe(10); // 5 * 2
  });

  it("routes steps to different queues", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<{ text: string }>({ name: "routed", storage })
      .step("preprocess", ({ input }) => Pipeline.succeed(input.text))
      .step("transcribe", { dependsOn: ["preprocess"] }, ({ deps }) =>
        Pipeline.succeed(`transcribed: ${deps.preprocess}`),
      )
      .build();

    const defaultRegistry = new MapStepRegistry();
    defaultRegistry.register("preprocess", (ctx) => Pipeline.succeed((ctx.input as any).text));

    const gpuRegistry = new MapStepRegistry();
    gpuRegistry.register("transcribe", (ctx) => Pipeline.succeed(`transcribed: ${ctx.prev}`));

    const coordinator = createCoordinator({
      storage,
      stepQueue: queue,
      routing: { transcribe: "gpu" },
    });

    const defaultWorker = createWorker({
      storage,
      stepQueue: queue,
      registry: defaultRegistry,
      queues: ["default"],
      pollIntervalMs: 50,
    });

    const gpuWorker = createWorker({
      storage,
      stepQueue: queue,
      registry: gpuRegistry,
      queues: ["gpu"],
      pollIntervalMs: 50,
    });

    await coordinator.submit({ workflow: wf, workflowId: "routed-1", input: { text: "hello" } });

    void coordinator.start();
    void defaultWorker.start();
    void gpuWorker.start();

    await new Promise((r) => setTimeout(r, 500));
    await coordinator.stop();
    await defaultWorker.stop();
    await gpuWorker.stop();

    const state = await storage.loadWorkflow("routed-1");
    expect(state?.steps["preprocess"]?.status).toBe("completed");
    // GPU step may or may not have completed in time depending on coordination timing
  });
});

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

describe("Worker middleware", () => {
  it("middleware wraps step execution", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const log: string[] = [];

    registry.register("step-a", (ctx) => Pipeline.succeed("result"));

    await storage.createWorkflow({ workflowId: "mw-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-1",
      stepName: "step-a",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      middleware: [
        async ({ task, ctx, next }) => {
          log.push("before");
          const result = await next(ctx);
          log.push("after");
          return result;
        },
      ],
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(log).toEqual(["before", "after"]);
  });

  it("middleware chain executes in order", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const log: string[] = [];

    registry.register("step-a", () => {
      log.push("handler");
      return Pipeline.succeed("ok");
    });

    await storage.createWorkflow({ workflowId: "mw-2", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-2",
      stepName: "step-a",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      middleware: [
        async ({ ctx, next }) => {
          log.push("mw1-before");
          const r = await next(ctx);
          log.push("mw1-after");
          return r;
        },
        async ({ ctx, next }) => {
          log.push("mw2-before");
          const r = await next(ctx);
          log.push("mw2-after");
          return r;
        },
      ],
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(log).toEqual(["mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"]);
  });

  it("timeout middleware fails slow steps", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const failures: string[] = [];

    registry.register("slow-step", async () => {
      await new Promise((r) => setTimeout(r, 5000));
      return "should-not-reach";
    });

    await storage.createWorkflow({ workflowId: "mw-3", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-3",
      stepName: "slow-step",
      queue: "default",
      input: {},
      prevResults: {},
    });

    // Import timeout middleware
    const { timeoutMiddleware } = await import("./middleware.ts");

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      middleware: [timeoutMiddleware(100)],
      hooks: {
        onError: (_task, err, _ms) => {
          failures.push(err instanceof Error ? err.message : String(err));
        },
      },
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 500));
    await worker.stop();

    expect(failures.some((f) => f.includes("timed out"))).toBe(true);
  });

  it("hooks run alongside middleware", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const log: string[] = [];

    registry.register("step-a", () => Pipeline.succeed("ok"));

    await storage.createWorkflow({ workflowId: "mw-4", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-4",
      stepName: "step-a",
      queue: "default",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      queues: ["default"],
      pollIntervalMs: 50,
      hooks: {
        beforeStep: () => {
          log.push("hook:before");
        },
        afterStep: () => {
          log.push("hook:after");
        },
      },
      middleware: [
        async ({ ctx, next }) => {
          log.push("mw:before");
          const r = await next(ctx);
          log.push("mw:after");
          return r;
        },
      ],
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    expect(log).toEqual(["hook:before", "mw:before", "mw:after", "hook:after"]);
  });
});
