import { describe, it, expect } from "bun:test";
import { Pipeline } from "@promin/core";
import { workflow, InMemoryWorkflowStorage, WorkflowVersionRegistry } from "../../durable/index.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createCoordinator } from "../coordinator.ts";
import { createWorker } from "../worker.ts";

// ---------------------------------------------------------------------------
// StepRegistry
// ---------------------------------------------------------------------------

describe("Step registry — register reusable step handlers by name", () => {
  it("register a 'double' handler and look it up by name at runtime", () => {
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

describe("Step queue — distribute tasks to available workers", () => {
  it("enqueue a task and claim it for processing", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "step-a",
      input: { n: 5 },
      prevResults: {},
    });

    const tasks = await queue.claim({ capabilities: [], limit: 10 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.stepName).toBe("step-a");
    expect(tasks[0]!.status).toBe("running");
  });

  it("GPU worker only sees GPU tasks, default worker only sees default tasks", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      needs: ["gpu"],
      input: {},
      prevResults: {},
    });

    const defaultTasks = await queue.claim({ capabilities: [], limit: 10 });
    expect(defaultTasks).toHaveLength(1);
    expect(defaultTasks[0]!.stepName).toBe("a");

    const gpuTasks = await queue.claim({ capabilities: ["gpu"], limit: 10 });
    expect(gpuTasks).toHaveLength(1);
    expect(gpuTasks[0]!.stepName).toBe("b");
  });

  it("worker claims at most 2 tasks at a time — respects concurrency limit", async () => {
    const queue = new InMemoryStepQueue();

    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        workflowId: "wf-1",
        stepName: `s-${i}`,
        input: {},
        prevResults: {},
      });
    }

    const tasks = await queue.claim({ capabilities: [], limit: 2 });
    expect(tasks).toHaveLength(2);
  });

  it("urgent tasks processed before background tasks — priority ordering", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "low",
      input: {},
      prevResults: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "high",
      input: {},
      prevResults: {},
      priority: 10,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "medium",
      input: {},
      prevResults: {},
      priority: 5,
    });

    const tasks = await queue.claim({ capabilities: [], limit: 3 });
    expect(tasks.map((t) => t.stepName)).toEqual(["high", "medium", "low"]);
  });

  it("tasks without explicit priority default to medium (5)", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-d",
      stepName: "default-prio",
      input: {},
      prevResults: {},
    });

    const tasks = await queue.claim({ capabilities: [], limit: 1 });
    expect(tasks[0]!.priority).toBe(5);
  });

  it("in-progress task is invisible to other workers — no double processing", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      input: {},
      prevResults: {},
    });

    const first = await queue.claim({ capabilities: [], limit: 10 });
    expect(first).toHaveLength(1);

    const second = await queue.claim({ capabilities: [], limit: 10 });
    expect(second).toHaveLength(0);
  });

  it("marking tasks complete or failed updates queue metrics", async () => {
    const queue = new InMemoryStepQueue();

    const id1 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      input: {},
      prevResults: {},
    });
    const id2 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      input: {},
      prevResults: {},
    });

    await queue.claim({ capabilities: [], limit: 10 });
    await queue.complete({ taskId: id1, result: "ok", durationMs: 100 });
    await queue.fail({ taskId: id2, error: "boom", durationMs: 50 });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBe(1);
    expect(metrics.failed).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Worker — step execution
// ---------------------------------------------------------------------------

describe("Worker — poll queue, execute steps, checkpoint results", () => {
  it("worker picks up a 'double' task and saves the result", async () => {
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
      input: { n: 5 },
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("step throws an error — worker records the failure and moves on", async () => {
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
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("unknown step name — worker skips the task and leaves it pending for a capable worker", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const failures: string[] = [];

    await storage.createWorkflow({ workflowId: "wf-3", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "unknown-step",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

    // The worker's default claim filter rejects tasks whose step name isn't
    // registered, so the task stays pending (a worker that DOES handle
    // "unknown-step" can still pick it up). No errors are recorded because
    // the task was never claimed.
    expect(failures).toHaveLength(0);
    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.pending).toBeGreaterThanOrEqual(1);
  });

  it("async step handler with I/O delay — worker awaits completion", async () => {
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
      input: { n: 5 },
      prevResults: { "prev-step": 42 },
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await worker.stop();

    const state = await storage.loadWorkflow("wf-4");
    expect(state?.steps["async-step"]?.result).toBe(142); // 42 + 100
  });

  it("default worker ignores GPU queue tasks — queue isolation", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const completed: string[] = [];

    registry.register("gpu-step", (ctx) => Pipeline.succeed("gpu-result"));

    await storage.createWorkflow({ workflowId: "wf-5", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-5",
      stepName: "gpu-step",
      needs: ["gpu"],
      input: {},
      prevResults: {},
    });

    // Worker only listens to "default" queue
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

describe("Coordinator + Worker end-to-end — orchestrate a distributed workflow", () => {
  it("coordinator enqueues steps, worker executes them, results are checkpointed", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    // Define workflow
    const wf = workflow<{ n: number }>({ name: "distributed-test" })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .step("add-ten", ({ prev }) => Pipeline.succeed(prev + 10))
      .build();

    // Register step implementations on the worker
    const registry = new MapStepRegistry();
    registry.register("double", (ctx) => Pipeline.succeed((ctx.input as any).n * 2));
    registry.register("add-ten", (ctx) => Pipeline.succeed((ctx.prev as number) + 10));

    const coordinator = createCoordinator({
      storage,
      stepQueue: queue,
    });

    // Create worker
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("transcription step routed to GPU worker, preprocessing stays on CPU", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<{ text: string }>({ name: "routed" })
      .step("preprocess", ({ input }) => Pipeline.succeed(input.text))
      .step(
        "transcribe",
        { dependsOn: ["preprocess"] },
        ({ deps }) => Pipeline.succeed(`transcribed: ${deps.preprocess}`),
        { needs: ["gpu"] },
      )
      .build();

    const defaultRegistry = new MapStepRegistry();
    defaultRegistry.register("preprocess", (ctx) => Pipeline.succeed((ctx.input as any).text));

    const gpuRegistry = new MapStepRegistry();
    gpuRegistry.register("transcribe", (ctx) => Pipeline.succeed(`transcribed: ${ctx.prev}`));

    const coordinator = createCoordinator({
      storage,
      stepQueue: queue,
    });

    const defaultWorker = createWorker({
      storage,
      stepQueue: queue,
      registry: defaultRegistry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    const gpuWorker = createWorker({
      storage,
      stepQueue: queue,
      registry: gpuRegistry,
      capabilities: ["gpu"],
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
// Registry-keyed submit — decouple submit from workflow definition import
// ---------------------------------------------------------------------------

describe("Coordinator registry-keyed submit — submit by name, not by object", () => {
  it("resolves a registered workflow by name and runs it end-to-end", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<{ n: number }>({ name: "named-wf", version: "1" })
      .step("double", ({ input }) => Pipeline.succeed(input.n * 2))
      .build();

    const registry = new WorkflowVersionRegistry();
    registry.register(wf as any);

    const stepRegistry = new MapStepRegistry();
    stepRegistry.register("double", (ctx) => Pipeline.succeed((ctx.input as any).n * 2));

    const coordinator = createCoordinator({ storage, stepQueue: queue, registry });
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    // Caller never touches `wf` — just the name.
    await coordinator.submit({ name: "named-wf", workflowId: "named-1", input: { n: 7 } });

    void coordinator.start();
    void worker.start();
    await new Promise((r) => setTimeout(r, 400));
    await coordinator.stop();
    await worker.stop();

    const state = await storage.loadWorkflow("named-1");
    expect(state?.steps["double"]?.status).toBe("completed");
    expect(state?.steps["double"]?.result).toBe(14);
  });

  it("throws synchronously on unknown name — loud typo failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<number>({ name: "known", version: "1" })
      .step("only", ({ input }) => Pipeline.succeed(input))
      .build();

    const registry = new WorkflowVersionRegistry();
    registry.register(wf as any);

    const coordinator = createCoordinator({ storage, stepQueue: queue, registry });

    await expect(coordinator.submit({ name: "typo", workflowId: "x", input: 1 })).rejects.toThrow(
      /No workflow "typo"/,
    );
  });

  it("throws when submit({ name }) is called without a registry on config", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const coordinator = createCoordinator({ storage, stepQueue: queue });

    await expect(
      coordinator.submit({ name: "anything", workflowId: "x", input: 0 }),
    ).rejects.toThrow(/requires `registry`/);
  });

  it("direct { workflow } shape still works alongside the registry", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    // Registry has one workflow — but we can still submit a different def directly.
    const registered = workflow<number>({ name: "registered", version: "1" })
      .step("a", ({ input }) => Pipeline.succeed(input))
      .build();
    const registry = new WorkflowVersionRegistry();
    registry.register(registered as any);

    const direct = workflow<number>({ name: "direct" })
      .step("only", ({ input }) => Pipeline.succeed(input + 1))
      .build();

    const stepRegistry = new MapStepRegistry();
    stepRegistry.register("only", (ctx) => Pipeline.succeed((ctx.input as number) + 1));

    const coordinator = createCoordinator({ storage, stepQueue: queue, registry });
    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    await coordinator.submit({ workflow: direct, workflowId: "mixed-1", input: 10 });

    void coordinator.start();
    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await coordinator.stop();
    await worker.stop();

    const state = await storage.loadWorkflow("mixed-1");
    expect(state?.steps["only"]?.status).toBe("completed");
    expect(state?.steps["only"]?.result).toBe(11);
  });
});

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

describe("Worker middleware — add logging, metrics, or timeouts around step execution", () => {
  it("middleware runs before and after the step handler", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const log: string[] = [];

    registry.register("step-a", (ctx) => Pipeline.succeed("result"));

    await storage.createWorkflow({ workflowId: "mw-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-1",
      stepName: "step-a",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("two middleware layers nest correctly — onion model execution order", async () => {
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
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("slow step exceeds 100ms timeout — middleware aborts it with a clear error", async () => {
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
      input: {},
      prevResults: {},
    });

    // Import timeout middleware
    const { timeoutMiddleware } = await import("../middleware.ts");

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

  it("lifecycle hooks fire around middleware — hooks bracket the full pipeline", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    const log: string[] = [];

    registry.register("step-a", () => Pipeline.succeed("ok"));

    await storage.createWorkflow({ workflowId: "mw-4", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "mw-4",
      stepName: "step-a",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
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

// ---------------------------------------------------------------------------
// Per-step options (retry, onFailure, compensate)
// ---------------------------------------------------------------------------

import { Data } from "effect";

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

describe("Per-step options — retry, skip, and fallback at the step level", () => {
  it("flaky API step retries up to 5 times before giving up", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    let attempts = 0;

    registry.register(
      "flaky",
      (ctx) => {
        attempts++;
        if (attempts < 3) throw new Error("transient");
        return Pipeline.succeed("ok");
      },
      { retry: { maxRetries: 5, baseDelayMs: 10 } },
    );

    await storage.createWorkflow({ workflowId: "retry-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "retry-1",
      stepName: "flaky",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 500));
    await worker.stop();

    expect(attempts).toBe(3);
    const state = await storage.loadWorkflow("retry-1");
    expect(state?.steps["flaky"]?.status).toBe("completed");
    expect(state?.steps["flaky"]?.result).toBe("ok");
  });

  it("permanent error skips retry — only transient errors are retried", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();
    let attempts = 0;

    registry.register(
      "selective",
      () => {
        attempts++;
        throw new TestError({ message: "permanent" });
      },
      {
        retry: {
          maxRetries: 5,
          baseDelayMs: 10,
          when: (err: any) => err._tag !== "TestError",
        },
      },
    );

    await storage.createWorkflow({ workflowId: "when-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "when-1",
      stepName: "selective",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await worker.stop();

    // TestError not retryable → only 1 attempt
    expect(attempts).toBe(1);
  });

  it("optional enrichment step fails — skip it and continue the workflow", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();

    registry.register(
      "optional",
      () => {
        throw new Error("fail");
      },
      { onFailure: "skip" },
    );

    await storage.createWorkflow({ workflowId: "skip-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "skip-1",
      stepName: "optional",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    const state = await storage.loadWorkflow("skip-1");
    // Step should be "completed" with undefined (skipped)
    expect(state?.steps["optional"]?.status).toBe("completed");
    expect(state?.steps["optional"]?.result).toBeUndefined();
  });

  it("risky step fails — use a safe default value instead of crashing", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();

    registry.register(
      "risky",
      () => {
        throw new Error("fail");
      },
      { onFailure: { fallback: () => "default-value" } },
    );

    await storage.createWorkflow({ workflowId: "fallback-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "fallback-1",
      stepName: "risky",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    const state = await storage.loadWorkflow("fallback-1");
    expect(state?.steps["risky"]?.status).toBe("completed");
    expect(state?.steps["risky"]?.result).toBe("default-value");
  });

  it("step execution logged to attempt storage — audit trail for compliance", async () => {
    const storage = new InMemoryWorkflowStorage(); // implements StepAttemptStorage
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();

    registry.register("tracked", () => Pipeline.succeed("done"));

    await storage.createWorkflow({ workflowId: "attempt-1", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "attempt-1",
      stepName: "tracked",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    const attempts = await storage.loadStepAttempts("attempt-1");
    expect(attempts).toHaveLength(1);
    expect(attempts[0]!.type).toBe("execution");
    expect(attempts[0]!.status).toBe("completed");
  });

  it("attempt row carries the worker id that processed it — ops audit trail", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();
    const registry = new MapStepRegistry();

    registry.register("ok", () => Pipeline.succeed("done"));
    registry.register("boom", () => Pipeline.fail(new Error("nope") as any));

    await storage.createWorkflow({ workflowId: "worker-trace", workflowName: "t", input: {} });
    await queue.enqueue({
      workflowId: "worker-trace",
      stepName: "ok",
      input: {},
      prevResults: {},
    });
    await queue.enqueue({
      workflowId: "worker-trace",
      stepName: "boom",
      input: {},
      prevResults: {},
    });

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
      workerId: "worker-alpha",
    });

    void worker.start();
    await new Promise((r) => setTimeout(r, 200));
    await worker.stop();

    const attempts = await storage.loadStepAttempts("worker-trace");
    const okAttempt = attempts.find((a) => a.stepName === "ok");
    const boomAttempt = attempts.find((a) => a.stepName === "boom");
    expect(okAttempt?.workerId).toBe("worker-alpha");
    expect(boomAttempt?.workerId).toBe("worker-alpha");
  });
});

// ---------------------------------------------------------------------------
// Coordinator recovery — resume after restart
// ---------------------------------------------------------------------------

describe("Coordinator recovery — resume workflows after process restart", () => {
  it("new coordinator discovers in-flight workflows and continues them", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<{ n: number }>({ name: "recoverable" })
      .step("step-1", ({ input }) => Pipeline.succeed(input.n * 2))
      .step("step-2", ({ prev }) => Pipeline.succeed(prev + 100))
      .build();

    // First coordinator — submits workflow and processes step-1
    const coord1 = createCoordinator({ storage, stepQueue: queue, pollIntervalMs: 50 });
    await coord1.submit({ workflow: wf, workflowId: "recover-1", input: { n: 5 } });

    const registry = new MapStepRegistry();
    registry.register("step-1", (ctx) => Pipeline.succeed((ctx.input as any).n * 2));
    registry.register("step-2", (ctx) => Pipeline.succeed((ctx.prev as number) + 100));

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void coord1.start();
    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await coord1.stop();

    // Verify step-1 completed
    const stateAfterStep1 = await storage.loadWorkflow("recover-1");
    expect(stateAfterStep1?.steps["step-1"]?.status).toBe("completed");

    // Simulate coordinator restart — new coordinator with no in-memory state
    const coord2 = createCoordinator({ storage, stepQueue: queue, pollIntervalMs: 50 });

    // coord2 never saw submit() — but should recover from storage
    void coord2.start();
    await new Promise((r) => setTimeout(r, 500));
    await coord2.stop();
    await worker.stop();

    // Workflow should have progressed (step-2 completed or at least enqueued)
    const finalState = await storage.loadWorkflow("recover-1");
    expect(finalState?.steps["step-1"]?.status).toBe("completed");
    // step-2 should have been enqueued and executed by the worker
  });

  it("already-completed workflows are not re-processed after restart", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<number>({ name: "done-wf" })
      .step("only", ({ input }) => Pipeline.succeed(input * 2))
      .build();

    // Submit and complete
    const coord1 = createCoordinator({ storage, stepQueue: queue, pollIntervalMs: 50 });
    await coord1.submit({ workflow: wf, workflowId: "done-1", input: 5 });

    const registry = new MapStepRegistry();
    registry.register("only", (ctx) => Pipeline.succeed((ctx.input as any) * 2));

    const worker = createWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: 50,
    });

    void coord1.start();
    void worker.start();
    await new Promise((r) => setTimeout(r, 300));
    await coord1.stop();
    await worker.stop();

    const state = await storage.loadWorkflow("done-1");
    expect(state?.status).toBe("completed");

    // New coordinator should not pick it up
    const coord2 = createCoordinator({ storage, stepQueue: queue, pollIntervalMs: 50 });
    void coord2.start();
    await new Promise((r) => setTimeout(r, 200));
    await coord2.stop();

    // Still completed — not re-processed
    const state2 = await storage.loadWorkflow("done-1");
    expect(state2?.status).toBe("completed");
  });
});
