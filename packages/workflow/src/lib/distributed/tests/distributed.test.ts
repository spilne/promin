// ---------------------------------------------------------------------------
// Distributed execution end to end: step registry, in-memory step queue,
// workers, middleware, per-step options and the distributed runner with
// recovery after a coordinator crash. Every loop (worker claims, the
// coordinator's sweep and step waits, heartbeats, retry backoff, middleware
// timeouts) runs on one FakeWallClock that the tests advance; nothing waits
// on real time.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { succeed } from "@spilne/perfect-core";
import {
  workflow,
  InMemoryWorkflowStorage,
  InMemoryWorkflowVersionRegistry,
} from "../../../index.ts";
import { MapStepRegistry } from "../step-registry.ts";
import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { createDistributedWorkflowRunner } from "../coordinator.ts";
import { createWorker } from "../worker.ts";
import { timeoutMiddleware } from "../middleware.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";

/** Clock step: the poll interval every worker and coordinator here uses. */
const POLL_MS = 50;

/** Let in-flight async work settle: a bounded number of macrotask turns. */
async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/**
 * Drive the system on the fake clock: let in-flight work settle, then move
 * time forward by `stepMs`, until `done()` holds. Fails once `maxMs` of
 * clock time has passed without it.
 */
async function driveUntil(params: {
  clock: FakeWallClock;
  done: () => boolean | Promise<boolean>;
  stepMs?: number;
  maxMs?: number;
}): Promise<void> {
  const { clock, done, stepMs = POLL_MS, maxMs = 10_000 } = params;
  for (let elapsed = 0; ; elapsed += stepMs) {
    await flush();
    if (await done()) return;
    if (elapsed >= maxMs) break;
    clock.advance(stepMs);
  }
  expect(await done()).toBe(true);
}

/** Run the system for `ms` of clock time (for "nothing happens" checks). */
async function runFor(params: { clock: FakeWallClock; ms: number }): Promise<void> {
  for (let elapsed = 0; elapsed < params.ms; elapsed += POLL_MS) {
    await flush();
    params.clock.advance(POLL_MS);
  }
  await flush();
}

/** A storage, queue and clock that share one fake time line. */
function setup() {
  const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
  const storage = new InMemoryWorkflowStorage({ clock });
  const queue = new InMemoryStepQueue({ clock });
  return { clock, storage, queue };
}

/**
 * Storage and queue as seen by a process that can crash: after `crash()`
 * every call hangs forever, like a coordinator that stopped mid-run.
 */
function crashable(inner: { storage: InMemoryWorkflowStorage; queue: InMemoryStepQueue }): {
  storage: InMemoryWorkflowStorage;
  queue: InMemoryStepQueue;
  crash: () => void;
} {
  let crashed = false;
  const view = <T extends object>(target: T): T =>
    new Proxy(target, {
      get(obj, prop, receiver) {
        const value = Reflect.get(obj, prop, receiver);
        if (typeof value !== "function") return value;
        return (...args: unknown[]) =>
          crashed ? new Promise<never>(() => {}) : value.apply(obj, args);
      },
    });
  return {
    storage: view(inner.storage),
    queue: view(inner.queue),
    crash: () => (crashed = true),
  };
}

// ---------------------------------------------------------------------------
// StepRegistry
// ---------------------------------------------------------------------------

describe("Step registry — register reusable step handlers by name", () => {
  it("register a 'double' handler and look it up by name at runtime", async () => {
    const registry = new MapStepRegistry();
    registry.register({ stepName: "double", handler: (ctx) => succeed((ctx.prev as number) * 2) });

    expect(registry.has("double")).toBe(true);
    expect(registry.has("missing")).toBe(false);
    expect(await registry.resolve("double")).toBeDefined();
    expect(await registry.resolve("missing")).toBeUndefined();
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
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: [], limit: 10 });
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
    });
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      needs: ["gpu"],
      input: {},
    });

    const defaultTasks = await queue.claim({ workerId: "w-1", capabilities: [], limit: 10 });
    expect(defaultTasks).toHaveLength(1);
    expect(defaultTasks[0]!.stepName).toBe("a");

    const gpuTasks = await queue.claim({ workerId: "w-1", capabilities: ["gpu"], limit: 10 });
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
      });
    }

    const tasks = await queue.claim({ workerId: "w-1", capabilities: [], limit: 2 });
    expect(tasks).toHaveLength(2);
  });

  it("urgent tasks processed before background tasks — priority ordering", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "low",
      input: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "high",
      input: {},
      priority: 10,
    });
    await queue.enqueue({
      workflowId: "wf-p",
      stepName: "medium",
      input: {},
      priority: 5,
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: [], limit: 3 });
    expect(tasks.map((t) => t.stepName)).toEqual(["high", "medium", "low"]);
  });

  it("tasks without explicit priority default to medium (5)", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-d",
      stepName: "default-prio",
      input: {},
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: [], limit: 1 });
    expect(tasks[0]!.priority).toBe(5);
  });

  it("in-progress task is invisible to other workers — no double processing", async () => {
    const queue = new InMemoryStepQueue();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      input: {},
    });

    const first = await queue.claim({ workerId: "w-1", capabilities: [], limit: 10 });
    expect(first).toHaveLength(1);

    const second = await queue.claim({ workerId: "w-1", capabilities: [], limit: 10 });
    expect(second).toHaveLength(0);
  });

  it("marking tasks complete or failed updates queue metrics", async () => {
    const { clock, queue } = setup();

    const id1 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "a",
      input: {},
    });
    const id2 = await queue.enqueue({
      workflowId: "wf-1",
      stepName: "b",
      input: {},
    });

    await queue.claim({ workerId: "w-1", capabilities: [], limit: 10 });
    await queue.complete({ taskId: id1, result: "ok", durationMs: 100 });
    await queue.fail({ taskId: id2, error: "boom", durationMs: 50 });

    const metrics = await queue.metrics({ since: new Date(clock.currentTimeMs() - 60_000) });
    expect(metrics.completed).toBe(1);
    expect(metrics.failed).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Worker — step execution
// ---------------------------------------------------------------------------

describe("Worker — poll queue, execute steps, settle the task", () => {
  it("worker picks up a 'double' task and settles it with the result", async () => {
    const { clock, storage, queue } = setup();
    const registry = new MapStepRegistry();
    const completed: string[] = [];

    registry.register({ stepName: "double", handler: (ctx) => succeed((ctx.input as any).n * 2) });

    // Create workflow and enqueue a task
    await storage.createWorkflow({ workflowId: "wf-1", workflowName: "test", input: { n: 5 } });
    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "double",
      input: { n: 5 },
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
      hooks: {
        afterStep: (task, _result, _ms) => {
          completed.push(task.stepName);
        },
      },
    });

    void worker.start();
    await driveUntil({ clock, done: () => completed.length > 0 });
    await worker.stop();

    expect(completed).toEqual(["double"]);
    const [task] = queue.getAllTasks();
    expect(task?.status).toBe("completed");
    expect(task?.result).toBe(10); // 5 * 2
    // Step rows are the coordinator's to write; the worker writes none.
    expect((await storage.loadWorkflow("wf-1"))?.steps["double"]).toBeUndefined();
  });

  it("step throws an error — worker records the failure and moves on", async () => {
    const { clock, storage, queue } = setup();
    const registry = new MapStepRegistry();
    const failures: string[] = [];

    registry.register({
      stepName: "fail-step",
      handler: () => {
        throw new Error("step exploded");
      },
    });

    await storage.createWorkflow({ workflowId: "wf-2", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-2",
      stepName: "fail-step",
      input: {},
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
      hooks: {
        onError: (task, _err, _ms) => {
          failures.push(task.stepName);
        },
      },
    });

    void worker.start();
    await driveUntil({ clock, done: () => queue.getAllTasks()[0]?.status === "failed" });
    await worker.stop();

    expect(failures).toEqual(["fail-step"]);
    expect(queue.getAllTasks()[0]?.error).toContain("step exploded");
    expect((await storage.loadWorkflow("wf-2"))?.steps["fail-step"]).toBeUndefined();
  });

  it("unknown step name — worker leaves the task pending for a capable worker", async () => {
    const { clock, storage, queue } = setup();
    const registry = new MapStepRegistry();
    const failures: string[] = [];
    registry.register({ stepName: "known-step", handler: () => succeed("x") });

    await storage.createWorkflow({ workflowId: "wf-3", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-3",
      stepName: "unknown-step",
      input: {},
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
      hooks: {
        onError: (_task, err, _ms) => {
          failures.push(err instanceof Error ? err.message : String(err));
        },
      },
    });

    void worker.start();
    // Twenty polls' worth of clock time.
    await runFor({ clock, ms: 20 * POLL_MS });
    await worker.stop();

    // The claim filters on the registry's step names, so the task is never
    // claimed: it stays pending with no delivery counted, and a worker that
    // does host "unknown-step" can still take it.
    expect(failures).toHaveLength(0);
    const [task] = queue.getAllTasks();
    expect(task?.status).toBe("pending");
    expect(task?.deliveries).toBe(0);
  });

  it("async step handler with I/O delay — worker awaits completion", async () => {
    const { clock, storage, queue } = setup();
    const registry = new MapStepRegistry();

    registry.register({
      stepName: "async-step",
      handler: async (ctx) => {
        await new Promise<void>((r) => clock.setTimeout(r, 10));
        return (ctx.prev as number) + 100;
      },
    });

    await storage.createWorkflow({ workflowId: "wf-4", workflowName: "test", input: { n: 5 } });
    await queue.enqueue({
      workflowId: "wf-4",
      stepName: "async-step",
      input: { n: 5 },
      deps: { "prev-step": 42 },
      dependsOn: ["prev-step"],
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    void worker.start();
    await driveUntil({ clock, done: () => queue.getAllTasks()[0]?.status === "completed" });
    await worker.stop();

    // `prev` is the first declared dependency's result.
    expect(queue.getAllTasks()[0]?.result).toBe(142); // 42 + 100
    // The handler's own 10ms wait ran on the clock and is part of the duration.
    expect(queue.getAllTasks()[0]?.durationMs).toBeGreaterThanOrEqual(10);
  });

  it("generalist worker ignores tasks that need a capability it lacks", async () => {
    const { clock, storage, queue } = setup();
    const registry = new MapStepRegistry();
    const completed: string[] = [];

    registry.register({ stepName: "gpu-step", handler: () => succeed("gpu-result") });

    await storage.createWorkflow({ workflowId: "wf-5", workflowName: "test", input: {} });
    await queue.enqueue({
      workflowId: "wf-5",
      stepName: "gpu-step",
      needs: ["gpu"],
      input: {},
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
      hooks: {
        afterStep: (task, _result, _ms) => {
          completed.push(task.stepName);
        },
      },
    });

    void worker.start();
    await runFor({ clock, ms: 20 * POLL_MS });
    await worker.stop();

    expect(completed).toHaveLength(0);
    expect(queue.getAllTasks()[0]?.status).toBe("pending");
  });
});

// ---------------------------------------------------------------------------
// Coordinator + Worker — end-to-end
// ---------------------------------------------------------------------------

describe("Coordinator + Worker end-to-end — orchestrate a distributed workflow", () => {
  it("coordinator enqueues steps, worker executes them, results are checkpointed", async () => {
    const { clock, storage, queue } = setup();

    const wf = workflow<{ n: number }>({ name: "distributed-test" })
      .step("double", ({ input }) => succeed(input.n * 2))
      .step("add-ten", ({ prev }) => succeed(prev + 10))
      .build();

    // Register step implementations on the worker
    const registry = new MapStepRegistry();
    registry.register({ stepName: "double", handler: (ctx) => succeed((ctx.input as any).n * 2) });
    registry.register({
      stepName: "add-ten",
      handler: (ctx) => succeed((ctx.prev as number) + 10),
    });

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    const box: { result?: unknown } = {};
    void coordinator.startLoop();
    void worker.start();
    void coordinator
      .run({ workflow: wf, workflowId: "e2e-1", input: { n: 5 } })
      .then((r) => (box.result = r));

    await driveUntil({ clock, done: () => box.result !== undefined });
    await coordinator.stopLoop();
    await worker.stop();

    expect(box.result).toBe(20); // 5 * 2 + 10
    const state = await storage.loadWorkflow("e2e-1");
    expect(state?.status).toBe("completed");
    expect(state?.steps["double"]?.result).toBe(10);
    expect(state?.steps["add-ten"]?.result).toBe(20);
    // One queue task per step, each delivered once.
    expect(queue.getAllTasks().map((t) => [t.stepName, t.status, t.deliveries])).toEqual([
      ["double", "completed", 1],
      ["add-ten", "completed", 1],
    ]);
  });

  it("transcription step routed to GPU worker, preprocessing stays on CPU", async () => {
    const { clock, storage, queue } = setup();

    const wf = workflow<{ text: string }>({ name: "routed" })
      .step("preprocess", ({ input }) => succeed(input.text))
      .step(
        "transcribe",
        { dependsOn: ["preprocess"] },
        ({ deps }) => succeed(`transcribed: ${deps.preprocess}`),
        { needs: ["gpu"] },
      )
      .build();

    // Both workers host both steps: routing comes from `needs` alone.
    const handled: Record<string, string[]> = { cpu: [], gpu: [] };
    const registryFor = (name: string) => {
      const registry = new MapStepRegistry();
      registry.register({
        stepName: "preprocess",
        handler: (ctx) => (handled[name]!.push("preprocess"), succeed((ctx.input as any).text)),
      });
      registry.register({
        stepName: "transcribe",
        handler: (ctx) => (handled[name]!.push("transcribe"), succeed(`transcribed: ${ctx.prev}`)),
      });
      return registry;
    };

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const cpuWorker = createWorker({
      stepQueue: queue,
      registry: registryFor("cpu"),
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });
    const gpuWorker = createWorker({
      stepQueue: queue,
      registry: registryFor("gpu"),
      capabilities: ["gpu"],
      pollIntervalMs: POLL_MS,
      clock,
    });

    const box: { result?: unknown } = {};
    void coordinator.startLoop();
    void cpuWorker.start();
    void gpuWorker.start();
    void coordinator
      .run({ workflow: wf, workflowId: "routed-1", input: { text: "hello" } })
      .then((r) => (box.result = r));

    await driveUntil({ clock, done: () => box.result !== undefined });
    await coordinator.stopLoop();
    await cpuWorker.stop();
    await gpuWorker.stop();

    expect(box.result).toBe("transcribed: hello");
    // The GPU step only went to the GPU worker. The unrestricted step may go
    // to either worker, so it is not asserted per worker.
    expect(handled.gpu).toContain("transcribe");
    expect(handled.cpu).not.toContain("transcribe");
  });
});

// ---------------------------------------------------------------------------
// Registry-keyed submit — decouple submit from workflow definition import
// ---------------------------------------------------------------------------

describe("Coordinator registry-keyed submit — submit by name, not by object", () => {
  it("resolves a registered workflow by name and runs it end-to-end", async () => {
    const { clock, storage, queue } = setup();

    const wf = workflow<{ n: number }>({ name: "named-wf", version: "1" })
      .step("double", ({ input }) => succeed(input.n * 2))
      .build();

    const registry = new InMemoryWorkflowVersionRegistry();
    await registry.register(wf as any);

    const stepRegistry = new MapStepRegistry();
    stepRegistry.register({
      stepName: "double",
      handler: (ctx) => succeed((ctx.input as any).n * 2),
    });

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      registry,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const worker = createWorker({
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    // Caller never touches `wf` — just the name.
    await coordinator.submit({ name: "named-wf", workflowId: "named-1", input: { n: 7 } });

    void coordinator.startLoop();
    void worker.start();
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflow("named-1"))?.status === "completed",
    });
    await coordinator.stopLoop();
    await worker.stop();

    const state = await storage.loadWorkflow("named-1");
    expect(state?.version).toBe("1");
    expect(state?.steps["double"]?.status).toBe("completed");
    expect(state?.steps["double"]?.result).toBe(14);
    expect(state?.result).toBe(14);
  });

  it("throws synchronously on unknown name — loud typo failure", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const wf = workflow<number>({ name: "known", version: "1" })
      .step("only", ({ input }) => succeed(input))
      .build();

    const registry = new InMemoryWorkflowVersionRegistry();
    await registry.register(wf as any);

    const coordinator = createDistributedWorkflowRunner({ storage, stepQueue: queue, registry });

    await expect(coordinator.submit({ name: "typo", workflowId: "x", input: 1 })).rejects.toThrow(
      /No workflow "typo"/,
    );
  });

  it("throws when submit({ name }) is called without a registry on config", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new InMemoryStepQueue();

    const coordinator = createDistributedWorkflowRunner({ storage, stepQueue: queue });

    await expect(
      coordinator.submit({ name: "anything", workflowId: "x", input: 0 }),
    ).rejects.toThrow(/requires `registry`/);
  });

  it("direct { workflow } shape still works alongside the registry", async () => {
    const { clock, storage, queue } = setup();

    // Registry has one workflow — but we can still submit a different def directly.
    const registered = workflow<number>({ name: "registered", version: "1" })
      .step("a", ({ input }) => succeed(input))
      .build();
    const registry = new InMemoryWorkflowVersionRegistry();
    await registry.register(registered as any);

    const direct = workflow<number>({ name: "direct" })
      .step("only", ({ input }) => succeed(input + 1))
      .build();

    const stepRegistry = new MapStepRegistry();
    stepRegistry.register({
      stepName: "only",
      handler: (ctx) => succeed((ctx.input as number) + 1),
    });

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      registry,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const worker = createWorker({
      stepQueue: queue,
      registry: stepRegistry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    await coordinator.submit({ workflow: direct, workflowId: "mixed-1", input: 10 });

    void coordinator.startLoop();
    void worker.start();
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflow("mixed-1"))?.status === "completed",
    });
    await coordinator.stopLoop();
    await worker.stop();

    const state = await storage.loadWorkflow("mixed-1");
    expect(state?.steps["only"]?.status).toBe("completed");
    expect(state?.steps["only"]?.result).toBe(11);
  });
});

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

type Registration = Omit<Parameters<MapStepRegistry["register"]>[0], "stepName">;
type WorkerExtras = Omit<Parameters<typeof createWorker>[0], "stepQueue" | "registry" | "clock">;

/**
 * One "step-a" task on a fresh run and a worker over it. `register` and
 * `extra` receive the shared clock, so handlers and middleware can wait on
 * it.
 */
async function singleTaskWorker(params: {
  workflowId: string;
  register: (clock: FakeWallClock) => Registration;
  extra?: (clock: FakeWallClock) => WorkerExtras;
}) {
  const { clock, storage, queue } = setup();
  const registry = new MapStepRegistry();
  registry.register({ stepName: "step-a", ...params.register(clock) });
  await storage.createWorkflow({
    workflowId: params.workflowId,
    workflowName: "test",
    input: {},
  });
  await queue.enqueue({
    workflowId: params.workflowId,
    stepName: "step-a",
    input: {},
  });
  const worker = createWorker({
    stepQueue: queue,
    registry,
    capabilities: [],
    pollIntervalMs: POLL_MS,
    clock,
    ...params.extra?.(clock),
  });
  const settled = () => {
    const status = queue.getAllTasks()[0]?.status;
    return status === "completed" || status === "failed";
  };
  return { clock, storage, queue, worker, settled };
}

describe("Worker middleware — add logging, metrics, or timeouts around step execution", () => {
  it("middleware runs before and after the step handler", async () => {
    const log: string[] = [];
    const { clock, worker, settled } = await singleTaskWorker({
      workflowId: "mw-1",
      register: () => ({ handler: () => succeed("result") }),
      extra: () => ({
        middleware: [
          async ({ ctx, next }) => {
            log.push("before");
            const result = await next(ctx);
            log.push("after");
            return result;
          },
        ],
      }),
    });

    void worker.start();
    await driveUntil({ clock, done: settled });
    await worker.stop();

    expect(log).toEqual(["before", "after"]);
  });

  it("two middleware layers nest correctly — onion model execution order", async () => {
    const log: string[] = [];
    const { clock, worker, settled } = await singleTaskWorker({
      workflowId: "mw-2",
      register: () => ({
        handler: () => {
          log.push("handler");
          return succeed("ok");
        },
      }),
      extra: () => ({
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
      }),
    });

    void worker.start();
    await driveUntil({ clock, done: settled });
    await worker.stop();

    expect(log).toEqual(["mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"]);
  });

  it("slow step exceeds 100ms timeout — middleware aborts it with a clear error", async () => {
    const failures: string[] = [];
    let startedAt: number | undefined;
    const { clock, queue, worker, settled } = await singleTaskWorker({
      workflowId: "mw-3",
      // Takes 5s of clock time — far past the middleware's 100ms.
      register: (clock) => ({
        handler: async () => {
          startedAt = clock.currentTimeMs();
          await new Promise<void>((r) => clock.setTimeout(r, 5_000));
          return "should-not-reach";
        },
      }),
      extra: (clock) => ({
        middleware: [timeoutMiddleware({ ms: 100, clock })],
        hooks: {
          onError: (_task, err, _ms) => {
            failures.push(err instanceof Error ? err.message : String(err));
          },
        },
      }),
    });

    void worker.start();
    await driveUntil({ clock, done: settled, stepMs: 10, maxMs: 1_000 });

    expect(failures).toEqual(["Step timed out after 100ms"]);
    expect(queue.getAllTasks()[0]?.status).toBe("failed");
    // Failed at the 100ms deadline, long before the handler's 5s.
    expect(startedAt).toBeDefined();
    expect(clock.currentTimeMs() - startedAt!).toBeLessThanOrEqual(110);
    expect(queue.getAllTasks()[0]?.error).toBe("Step timed out after 100ms");

    // Let the abandoned handler's 5s timer fire so stop() has nothing to wait on.
    clock.advance(5_000);
    await worker.stop();
  });

  it("lifecycle hooks fire around middleware — hooks bracket the full pipeline", async () => {
    const log: string[] = [];
    const { clock, worker, settled } = await singleTaskWorker({
      workflowId: "mw-4",
      register: () => ({ handler: () => succeed("ok") }),
      extra: () => ({
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
      }),
    });

    void worker.start();
    await driveUntil({ clock, done: settled });
    await worker.stop();

    expect(log).toEqual(["hook:before", "mw:before", "mw:after", "hook:after"]);
  });
});

// ---------------------------------------------------------------------------
// Coordinator recovery — resume after restart
// ---------------------------------------------------------------------------

describe("Coordinator recovery — resume workflows after process restart", () => {
  it("a new coordinator adopts a run its crashed predecessor left mid-flight", async () => {
    const { clock, storage, queue } = setup();
    const calls = { "step-1": 0, "step-2": 0 };

    const wf = workflow<{ n: number }>({ name: "recoverable" })
      .step("step-1", ({ input }) => succeed(input.n * 2))
      .step("step-2", ({ prev }) => succeed(prev + 100))
      .build();

    // The first coordinator drives the run through views of storage and the
    // queue that stop answering when it crashes.
    const first = crashable({ storage, queue });
    const coord1 = createDistributedWorkflowRunner({
      storage: first.storage,
      stepQueue: first.queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });

    // Only step-1 has a worker so far.
    const registry1 = new MapStepRegistry();
    registry1.register({
      stepName: "step-1",
      handler: (ctx) => (calls["step-1"]++, succeed((ctx.input as any).n * 2)),
    });
    const worker1 = createWorker({
      stepQueue: queue,
      registry: registry1,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    void coord1.startLoop();
    void worker1.start();
    void coord1.run({ workflow: wf, workflowId: "recover-1", input: { n: 5 } }).catch(() => {});

    // Drive until step-1 is checkpointed and step-2 is queued, then crash.
    await driveUntil({
      clock,
      done: () => queue.getAllTasks().some((t) => t.stepName === "step-2"),
    });
    expect((await storage.loadWorkflow("recover-1"))?.steps["step-1"]?.result).toBe(10);
    first.crash();
    await worker1.stop();

    // A worker for step-2 comes up, and a new coordinator that never saw
    // the run starts its sweep.
    const registry2 = new MapStepRegistry();
    registry2.register({
      stepName: "step-2",
      handler: (ctx) => (calls["step-2"]++, succeed((ctx.prev as number) + 100)),
    });
    const worker2 = createWorker({
      stepQueue: queue,
      registry: registry2,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });
    // Count coord2's enqueues: adopting the run re-dispatches step-2, and
    // the queue hands back the task coord1 left pending.
    let adoptedEnqueues = 0;
    const enqueue = queue.enqueue.bind(queue);
    const coord2 = createDistributedWorkflowRunner({
      storage,
      stepQueue: Object.assign(Object.create(queue) as InMemoryStepQueue, {
        enqueue: (p: Parameters<InMemoryStepQueue["enqueue"]>[0]) => (
          adoptedEnqueues++, enqueue(p)
        ),
      }),
      pollIntervalMs: 1_000,
      stepPollIntervalMs: 1_000,
      orphanGraceMs: 1_000,
      recoveryIntervalMs: 5_000,
      clock,
    });
    void coord2.startLoop();

    // The dead coordinator's run lock expires and coord2 adopts the run.
    await driveUntil({ clock, done: () => adoptedEnqueues > 0, stepMs: 1_000, maxMs: 600_000 });
    expect(queue.getAllTasks().filter((t) => t.stepName === "step-2")).toHaveLength(1);
    void worker2.start();

    // coord2 finishes it. Completed steps are not run again.
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflow("recover-1"))?.status === "completed",
      stepMs: 1_000,
      maxMs: 600_000,
    });
    expect(await coord2.waitForResult<number>("recover-1")).toBe(110);
    await coord2.stopLoop();
    await worker2.stop();

    const finalState = await storage.loadWorkflow("recover-1");
    expect(finalState?.result).toBe(110);
    expect(calls).toEqual({ "step-1": 1, "step-2": 1 });
  });

  it("a task that settles while no coordinator waits on it is adopted, not run again", async () => {
    const { clock, storage, queue } = setup();
    let calls = 0;
    const wf = workflow<number>({ name: "gap" })
      .step("only", ({ input }) => succeed(input + 1))
      .build();

    const first = crashable({ storage, queue });
    const coord1 = createDistributedWorkflowRunner({
      storage: first.storage,
      stepQueue: first.queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    void coord1.startLoop();
    void coord1.run({ workflow: wf, workflowId: "gap-1", input: 1 }).catch(() => {});
    await driveUntil({ clock, done: () => queue.getAllTasks().length === 1 });
    first.crash();

    // A worker settles the task while no coordinator is waiting on it.
    const registry = new MapStepRegistry();
    registry.register({
      stepName: "only",
      handler: (ctx) => (calls++, succeed((ctx.input as number) + calls * 10)),
    });
    const worker = createWorker({
      stepQueue: queue,
      registry,
      pollIntervalMs: POLL_MS,
      clock,
    });
    void worker.start();
    await driveUntil({ clock, done: () => queue.getAllTasks()[0]?.status === "completed" });
    // Only a coordinator writes step rows, so the outcome is not in storage.
    expect((await storage.loadWorkflow("gap-1"))?.steps["only"]).toBeUndefined();

    const coord2 = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 1_000,
      stepPollIntervalMs: 1_000,
      orphanGraceMs: 1_000,
      recoveryIntervalMs: 5_000,
      clock,
    });
    void coord2.startLoop();
    await driveUntil({
      clock,
      done: async () => (await storage.loadWorkflow("gap-1"))?.status === "completed",
      stepMs: 1_000,
      maxMs: 600_000,
    });
    await coord2.stopLoop();
    await worker.stop();

    // The adopting coordinator took the settled task's outcome: the handler
    // ran exactly once across the crash, and the row holds that outcome.
    expect(calls).toBe(1);
    const state = await storage.loadWorkflow("gap-1");
    expect(state?.result).toBe(11);
    expect(state?.steps["only"]?.result).toBe(11);
    const [task] = queue.getAllTasks();
    expect(queue.getAllTasks()).toHaveLength(1);
    expect(task?.consumedAt).toBeInstanceOf(Date);
    const attempts = await storage.loadStepAttempts({ workflowId: "gap-1" });
    expect(attempts.map((a) => [a.attempt, a.status])).toEqual([[1, "completed"]]);
  });

  it("already-completed workflows are not re-processed after restart", async () => {
    const { clock, storage, queue } = setup();
    let calls = 0;

    const wf = workflow<number>({ name: "done-wf" })
      .step("only", ({ input }) => succeed(input * 2))
      .build();

    const coord1 = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: POLL_MS,
      stepPollIntervalMs: POLL_MS,
      clock,
    });
    const registry = new MapStepRegistry();
    registry.register({
      stepName: "only",
      handler: (ctx) => (calls++, succeed((ctx.input as any) * 2)),
    });
    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: [],
      pollIntervalMs: POLL_MS,
      clock,
    });

    const box: { result?: unknown } = {};
    void coord1.startLoop();
    void worker.start();
    void coord1.run({ workflow: wf, workflowId: "done-1", input: 5 }).then((r) => (box.result = r));
    await driveUntil({ clock, done: () => box.result !== undefined });
    await coord1.stopLoop();
    expect(box.result).toBe(10);

    // A new coordinator sweeps through several recovery intervals.
    const coord2 = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 1_000,
      orphanGraceMs: 1_000,
      recoveryIntervalMs: 5_000,
      clock,
    });
    void coord2.startLoop();
    for (let i = 0; i < 30; i++) {
      await flush();
      clock.advance(1_000);
    }
    await flush();
    await coord2.stopLoop();
    await worker.stop();

    // Still completed — not re-processed, and no new task was queued.
    const state = await storage.loadWorkflow("done-1");
    expect(state?.status).toBe("completed");
    expect(calls).toBe(1);
    expect(queue.getAllTasks()).toHaveLength(1);
  });
});
