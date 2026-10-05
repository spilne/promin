import { describe, it, expect, beforeAll, afterAll, beforeEach, setDefaultTimeout } from "bun:test";
import { workflow, InMemoryWorkflowStorage } from "@promin/workflow";
import {
  MapStepRegistry,
  InMemoryWorkerRegistry,
  createDistributedWorkflowRunner,
  createWorker,
} from "@promin/workflow/distributed";
import { PostgresTestContainer } from "../test-utils.ts";
import { PgStepQueue } from "../pg-step-queue.ts";

setDefaultTimeout(60_000);

// ---------------------------------------------------------------------------
// Multi-worker scenarios over one shared Postgres: concurrent claims,
// coordinator/worker lifecycles, dead-worker recovery, priority and
// capability routing.
// ---------------------------------------------------------------------------

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await new PgStepQueue({ db: pg.db }).ensureTable();
}, 120_000);

afterAll(async () => {
  await pg.stop();
});

// Dedupe is on (workflow_id, step_name) and metric assertions count every
// row in the window, so each scenario starts from an empty queue.
beforeEach(async () => {
  await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
});

/** Retry an assertion until it passes or the timeout elapses. */
async function eventually(
  assertion: () => void | Promise<void>,
  opts?: { timeoutMs?: number; intervalMs?: number },
): Promise<void> {
  const { timeoutMs = 5_000, intervalMs = 100 } = opts ?? {};
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (e) {
      lastError = e;
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  }
  throw lastError;
}

// ---------------------------------------------------------------------------
// Claim / complete across competing queue handles
// ---------------------------------------------------------------------------

describe("PgStepQueue — competing claims", () => {
  it("enqueue and claim a step task", async () => {
    const queue = new PgStepQueue({ db: pg.db });

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "process",
      needs: ["default"],
      input: { data: "hello" },
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.stepName).toBe("process");
    expect(tasks[0]!.input).toEqual({ data: "hello" });
  });

  it("SKIP LOCKED prevents double-claim between simultaneous claimers", async () => {
    const q1 = new PgStepQueue({ db: pg.db });
    const q2 = new PgStepQueue({ db: pg.db });

    await q1.enqueue({
      workflowId: "wf-2",
      stepName: "step-a",
      needs: ["default"],
      input: {},
    });

    const [t1, t2] = await Promise.all([
      q1.claim({ workerId: "w1", capabilities: ["default"], limit: 1 }),
      q2.claim({ workerId: "w2", capabilities: ["default"], limit: 1 }),
    ]);

    expect([...t1, ...t2]).toHaveLength(1);
  });

  it("higher priority tasks are claimed first", async () => {
    const queue = new PgStepQueue({ db: pg.db });

    await queue.enqueue({
      workflowId: "wf-lo",
      stepName: "low",
      needs: ["default"],
      input: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-hi",
      stepName: "high",
      needs: ["default"],
      input: {},
      priority: 10,
    });

    const first = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
    expect(first[0]!.stepName).toBe("high");

    const second = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
    expect(second[0]!.stepName).toBe("low");
  });

  it("a completed task is never claimed again", async () => {
    const queue = new PgStepQueue({ db: pg.db });

    await queue.enqueue({
      workflowId: "wf-done",
      stepName: "s1",
      needs: ["default"],
      input: {},
    });

    const tasks = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
    await queue.complete({ taskId: tasks[0]!.id, result: { output: "done" }, durationMs: 42 });

    const next = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
    expect(next).toHaveLength(0);
  });

  it("three workers drain the queue with every task claimed exactly once", async () => {
    const coordinator = new PgStepQueue({ db: pg.db });
    for (let i = 0; i < 10; i++) {
      await coordinator.enqueue({
        workflowId: "wf-e2e",
        stepName: `step-${i}`,
        needs: ["default"],
        input: { index: i },
      });
    }

    const completed: string[] = [];
    const assignments: Record<string, string[]> = { w1: [], w2: [], w3: [] };

    async function work(name: string) {
      const q = new PgStepQueue({ db: pg.db });
      while (true) {
        const tasks = await q.claim({ workerId: name, capabilities: ["default"], limit: 1 });
        if (tasks.length === 0) break;
        const task = tasks[0]!;
        assignments[name]!.push(task.stepName);
        await q.complete({ taskId: task.id, result: { done: true }, durationMs: 1 });
        completed.push(task.stepName);
      }
    }

    await Promise.all([work("w1"), work("w2"), work("w3")]);

    expect(completed.sort()).toEqual(Array.from({ length: 10 }, (_, i) => `step-${i}`).sort());
    const active = Object.values(assignments).filter((a) => a.length > 0);
    expect(active.length).toBeGreaterThanOrEqual(1);
  });

  it("claims racing completions never re-claim a finished task", async () => {
    // Many short-lived tasks claimed and completed by several workers at
    // once maximizes the window where one worker's claim scan meets a row
    // another worker has just completed.
    const enqueuer = new PgStepQueue({ db: pg.db });
    const total = 200;
    for (let i = 0; i < total; i++) {
      await enqueuer.enqueue({
        workflowId: `wf-race-${i}`,
        stepName: "s",
        needs: ["default"],
        input: {},
      });
    }

    const claimedIds: string[] = [];
    async function work(name: string) {
      const q = new PgStepQueue({ db: pg.db });
      while (true) {
        const tasks = await q.claim({ workerId: name, capabilities: ["default"], limit: 3 });
        if (tasks.length === 0) break;
        for (const task of tasks) {
          claimedIds.push(task.id);
          await q.complete({ taskId: task.id, result: null, durationMs: 0 });
        }
      }
    }

    await Promise.all(Array.from({ length: 6 }, (_, i) => work(`w${i}`)));

    expect(claimedIds).toHaveLength(total);
    expect(new Set(claimedIds).size).toBe(total);
  });
});

// ---------------------------------------------------------------------------
// DAG workflow — coordinator dispatches, workers execute
//
// upload → [transcribe, thumbnail] → summarize → notify
// ---------------------------------------------------------------------------

describe("Distributed DAG workflow — video processing pipeline", () => {
  it("coordinator dispatches DAG steps, workers execute in correct order", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db: pg.db });

    const videoPipeline = workflow<{ videoUrl: string }>({ name: "video-processing", storage })
      .stepAsync("upload", async (ctx) => ({ url: ctx.input.videoUrl, size: 1024 }))
      .stepAsync("transcribe", { dependsOn: ["upload"] }, async (ctx) => ({
        text: `Transcript of ${ctx.deps.upload.url}`,
      }))
      .stepAsync("thumbnail", { dependsOn: ["upload"] }, async (ctx) => ({
        thumbUrl: `${ctx.deps.upload.url}/thumb.jpg`,
      }))
      .stepAsync("summarize", { dependsOn: ["transcribe", "thumbnail"] }, async (ctx) => ({
        summary: ctx.deps.transcribe.text.slice(0, 20),
        thumb: ctx.deps.thumbnail.thumbUrl,
      }))
      .stepAsync("notify", async (ctx) => ({ notified: true, summary: ctx.prev.summary }))
      .build();

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 100,
    });

    const registry = new MapStepRegistry();
    const executionOrder: string[] = [];
    registry.register({
      stepName: "upload",
      handler: async (ctx) => {
        executionOrder.push("upload");
        return { url: (ctx.input as any).videoUrl, size: 1024 };
      },
    });
    registry.register({
      stepName: "transcribe",
      handler: async (ctx) => {
        executionOrder.push("transcribe");
        return { text: `Transcript of ${(ctx.prev as any).url}` };
      },
    });
    registry.register({
      stepName: "thumbnail",
      handler: async (ctx) => {
        executionOrder.push("thumbnail");
        return { thumbUrl: `${(ctx.prev as any).url}/thumb.jpg` };
      },
    });
    registry.register({
      stepName: "summarize",
      handler: async (ctx) => {
        executionOrder.push("summarize");
        return { summary: "Summary text", thumb: (ctx.deps as any).thumbnail.thumbUrl };
      },
    });
    registry.register({
      stepName: "notify",
      handler: async (ctx) => {
        executionOrder.push("notify");
        return { notified: true, summary: (ctx.prev as any).summary };
      },
    });

    const worker = createWorker({
      stepQueue: queue,
      registry,
      capabilities: ["default"],
      concurrency: 3,
      pollIntervalMs: 100,
      workerId: "worker-1",
    });

    await coordinator.submit({
      workflow: videoPipeline,
      workflowId: "vid-001",
      input: { videoUrl: "https://cdn.example.com/video.mp4" },
    });

    void coordinator.startLoop();
    worker.start();

    const result = await coordinator.waitForResult<{ notified: boolean }>("vid-001");

    await coordinator.stopLoop();
    await worker.stop();

    expect(result.notified).toBe(true);
    expect(executionOrder[0]).toBe("upload");
    expect(executionOrder.indexOf("summarize")).toBeGreaterThan(
      Math.max(executionOrder.indexOf("transcribe"), executionOrder.indexOf("thumbnail")),
    );
    expect(executionOrder.indexOf("notify")).toBeGreaterThan(executionOrder.indexOf("summarize"));
  });
});

// ---------------------------------------------------------------------------
// Competing workers — 50 orders, 3 workers, each executed exactly once
// ---------------------------------------------------------------------------

describe("Distributed workers — competing task execution", () => {
  it("3 workers process 50 tasks with no duplicates via SKIP LOCKED", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db: pg.db });
    const processed: { worker: string; workflowId: string }[] = [];

    const orderWorkflow = workflow<{ orderId: string }>({ name: "order-fulfillment", storage })
      .stepAsync("fulfill", async (ctx) => ({ fulfilled: true, orderId: ctx.input.orderId }))
      .build();

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 50,
    });

    for (let i = 0; i < 50; i++) {
      await coordinator.submit({
        workflow: orderWorkflow,
        workflowId: `order-${i}`,
        input: { orderId: `ORD-${i}` },
      });
    }

    // Step bodies park until a second worker has started one. A worker
    // holds at most `concurrency` (5) tasks, so the other 45 stay claimable
    // and load sharing is guaranteed rather than left to poll timing — a
    // loaded machine can otherwise let the first worker drain everything.
    const workersSeen = new Set<string>();
    let openGate!: () => void;
    const gate = new Promise<void>((resolve) => (openGate = resolve));

    const workers = [1, 2, 3].map((id) => {
      const registry = new MapStepRegistry();
      registry.register({
        stepName: "fulfill",
        handler: async (stepCtx) => {
          processed.push({ worker: `worker-${id}`, workflowId: stepCtx.workflowId });
          workersSeen.add(`worker-${id}`);
          if (workersSeen.size >= 2) openGate();
          await gate;
          return { fulfilled: true };
        },
      });
      return createWorker({
        stepQueue: new PgStepQueue({ db: pg.db, workerId: `worker-${id}` }),
        registry,
        capabilities: ["default"],
        concurrency: 5,
        pollIntervalMs: 50,
        workerId: `worker-${id}`,
      });
    });

    void coordinator.startLoop();
    workers.forEach((w) => w.start());

    await eventually(
      async () => {
        const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
        expect(metrics.completed).toBe(50);
      },
      { timeoutMs: 30_000, intervalMs: 200 },
    );

    await coordinator.stopLoop();
    await Promise.all(workers.map((w) => w.stop()));

    // Each workflow's step executed exactly once.
    const workflowIds = processed.map((p) => p.workflowId);
    expect(workflowIds).toHaveLength(50);
    expect(new Set(workflowIds).size).toBe(50);

    const workerCounts = new Map<string, number>();
    for (const p of processed) {
      workerCounts.set(p.worker, (workerCounts.get(p.worker) ?? 0) + 1);
    }
    expect([...workerCounts.values()].filter((c) => c > 0).length).toBeGreaterThanOrEqual(2);
  });
});

// ---------------------------------------------------------------------------
// Dead worker — coordinator re-enqueues its stuck task for another worker
// ---------------------------------------------------------------------------

describe("Dead worker detection — task recovery", () => {
  it("coordinator re-enqueues stuck tasks from dead worker", async () => {
    const queue = new PgStepQueue({ db: pg.db });
    const workerRegistry = new InMemoryWorkerRegistry();

    await queue.enqueue({
      workflowId: "wf-recovery",
      stepName: "process",
      needs: ["default"],
      input: {},
    });

    const w1Queue = new PgStepQueue({ db: pg.db });
    const claimed = await w1Queue.claim({
      workerId: "worker-dead",
      capabilities: ["default"],
      limit: 1,
    });
    expect(claimed).toHaveLength(1);

    // The worker "crashes": its task stays running and it stops heartbeating.
    await workerRegistry.register({
      workerId: "worker-dead",
      capabilities: ["default"],
      concurrency: 1,
    });
    const deadWorker = (await workerRegistry.list()).find((w) => w.workerId === "worker-dead");
    if (deadWorker) {
      (deadWorker as any).lastHeartbeat = new Date(Date.now() - 60_000);
    }

    const dead = await workerRegistry.detectDead(5_000);
    expect(dead.length).toBe(1);

    const { requeued } = await queue.requeueStuck({ mode: "worker", workerId: "worker-dead" });
    expect(requeued).toBe(1);

    const w2Queue = new PgStepQueue({ db: pg.db });
    const reclaimed = await w2Queue.claim({
      workerId: "worker-alive",
      capabilities: ["default"],
      limit: 1,
    });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.stepName).toBe("process");

    await w2Queue.complete({ taskId: reclaimed[0]!.id, result: { done: true }, durationMs: 5 });

    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBe(1);
    expect(metrics.running).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Priority — critical orders processed first even when submitted later
// ---------------------------------------------------------------------------

describe("Priority queue — critical orders processed first", () => {
  it("higher priority tasks are claimed before lower priority", async () => {
    const queue = new PgStepQueue({ db: pg.db });

    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        workflowId: `low-${i}`,
        stepName: "process",
        needs: ["default"],
        input: { priority: "low" },
        priority: 1,
      });
    }
    for (let i = 0; i < 3; i++) {
      await queue.enqueue({
        workflowId: `high-${i}`,
        stepName: "process",
        needs: ["default"],
        input: { priority: "high" },
        priority: 10,
      });
    }

    const order: string[] = [];
    for (let i = 0; i < 8; i++) {
      const tasks = await queue.claim({ workerId: "w-1", capabilities: ["default"], limit: 1 });
      if (tasks.length > 0) {
        order.push(tasks[0]!.workflowId.startsWith("high") ? "high" : "low");
        await queue.complete({ taskId: tasks[0]!.id, result: {}, durationMs: 1 });
      }
    }

    expect(order.slice(0, 3)).toEqual(["high", "high", "high"]);
    expect(order.slice(3)).toEqual(["low", "low", "low", "low", "low"]);
  });
});

// ---------------------------------------------------------------------------
// Queue routing — GPU steps to GPU workers, CPU steps to CPU workers
// ---------------------------------------------------------------------------

describe("Queue routing — GPU vs CPU workers", () => {
  it("coordinator routes steps to specialized queues, workers claim their own", async () => {
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db: pg.db });

    const mlPipeline = workflow<{ imageUrl: string }>({ name: "ml-pipeline", storage })
      .stepAsync("preprocess", async (ctx) => ({ processed: ctx.input.imageUrl }), {
        needs: ["cpu"],
      })
      .stepAsync("inference", async () => ({ prediction: "cat", confidence: 0.95 }), {
        needs: ["gpu"],
      })
      .stepAsync("postprocess", async (ctx) => ({ result: ctx.prev }), { needs: ["cpu"] })
      .build();

    const coordinator = createDistributedWorkflowRunner({
      storage,
      stepQueue: queue,
      pollIntervalMs: 50,
    });

    const gpuProcessed: string[] = [];
    const cpuProcessed: string[] = [];

    const gpuRegistry = new MapStepRegistry();
    gpuRegistry.register({
      stepName: "inference",
      handler: async () => {
        gpuProcessed.push("inference");
        return { prediction: "cat", confidence: 0.95 };
      },
    });
    const gpuWorker = createWorker({
      stepQueue: new PgStepQueue({ db: pg.db }),
      registry: gpuRegistry,
      capabilities: ["gpu"],
      concurrency: 1,
      pollIntervalMs: 50,
      workerId: "gpu-worker",
    });

    const cpuRegistry = new MapStepRegistry();
    cpuRegistry.register({
      stepName: "preprocess",
      handler: async (stepCtx) => {
        cpuProcessed.push("preprocess");
        return { processed: (stepCtx.input as any).imageUrl };
      },
    });
    cpuRegistry.register({
      stepName: "postprocess",
      handler: async (ctx) => {
        cpuProcessed.push("postprocess");
        return { result: ctx.prev };
      },
    });
    const cpuWorker = createWorker({
      stepQueue: new PgStepQueue({ db: pg.db }),
      registry: cpuRegistry,
      capabilities: ["cpu"],
      concurrency: 2,
      pollIntervalMs: 50,
      workerId: "cpu-worker",
    });

    await coordinator.submit({
      workflow: mlPipeline,
      workflowId: "ml-001",
      input: { imageUrl: "https://images.example.com/cat.jpg" },
    });

    void coordinator.startLoop();
    gpuWorker.start();
    cpuWorker.start();

    await coordinator.waitForResult("ml-001");

    await coordinator.stopLoop();
    await gpuWorker.stop();
    await cpuWorker.stop();

    expect(gpuProcessed).toEqual(["inference"]);
    expect(cpuProcessed).toEqual(["preprocess", "postprocess"]);
  });
});
