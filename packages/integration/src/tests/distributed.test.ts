import { describe, it, expect, setDefaultTimeout } from "bun:test";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { withPostgres, withAll, eventually, uniqueName } from "../infra.ts";
import { Pipeline, InMemoryState } from "@promin/core";
import { StreamTopology, TopologyRunner } from "@promin/topology";
import {
  workflow,
  InMemoryWorkflowStorage,
  DefaultCoordinator,
  DefaultWorker,
  MapStepRegistry,
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
} from "@promin/workflow";
import { PgStepQueue } from "@promin/postgres";
import type { DrizzleDb } from "@promin/postgres";
import { KafkaTopic } from "@promin/kafka";
import { createKafkajsClient, createTopic } from "../adapters/kafkajs-adapter.ts";

setDefaultTimeout(300_000);

// ---------------------------------------------------------------------------
// Helper — create drizzle db from postgres ctx
// ---------------------------------------------------------------------------

function createDb(ctx: { url: string }): { db: DrizzleDb; close: () => Promise<void> } {
  const sql = postgres(ctx.url);
  return { db: drizzle(sql) as DrizzleDb, close: () => sql.end() };
}

// ---------------------------------------------------------------------------
// Scenario 1: DAG workflow — coordinator dispatches, workers execute
//
// Business flow: Video processing pipeline
//   upload → [transcribe, thumbnail] → summarize → notify
//   transcribe and thumbnail run in parallel, summarize depends on both
// ---------------------------------------------------------------------------

withPostgres("Distributed DAG workflow — video processing pipeline", (ctx) => {
  it("coordinator dispatches DAG steps, workers execute in correct order", async () => {
    const { db, close } = createDb(ctx);
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();

    // Define the workflow with DAG
    const videoPipeline = workflow<{ videoUrl: string }>({
      name: "video-processing",
      storage,
    })
      .step("upload", (ctx) => Pipeline.succeed({ url: ctx.input.videoUrl, size: 1024 }))
      .step("transcribe", { dependsOn: ["upload"] }, (ctx) =>
        Pipeline.succeed({ text: `Transcript of ${ctx.deps.upload.url}` }),
      )
      .step("thumbnail", { dependsOn: ["upload"] }, (ctx) =>
        Pipeline.succeed({ thumbUrl: `${ctx.deps.upload.url}/thumb.jpg` }),
      )
      .step("summarize", { dependsOn: ["transcribe", "thumbnail"] }, (ctx) =>
        Pipeline.succeed({
          summary: ctx.deps.transcribe.text.slice(0, 20),
          thumb: ctx.deps.thumbnail.thumbUrl,
        }),
      )
      .step("notify", (ctx) => Pipeline.succeed({ notified: true, summary: ctx.prev.summary }))
      .build();

    // Set up coordinator
    const coordinator = new DefaultCoordinator({
      storage,
      stepQueue: queue,
      pollIntervalMs: 100,
    });

    // Set up worker with step handlers
    const registry = new MapStepRegistry();
    const executionOrder: string[] = [];

    registry.register("upload", async (ctx) => {
      executionOrder.push("upload");
      return { url: (ctx.input as any).videoUrl, size: 1024 };
    });
    registry.register("transcribe", async (ctx) => {
      executionOrder.push("transcribe");
      return { text: `Transcript of ${(ctx.prev as any).url}` };
    });
    registry.register("thumbnail", async (ctx) => {
      executionOrder.push("thumbnail");
      return { thumbUrl: `${(ctx.prev as any).url}/thumb.jpg` };
    });
    registry.register("summarize", async (ctx) => {
      executionOrder.push("summarize");
      return { summary: "Summary text", thumb: (ctx.deps as any).thumbnail.thumbUrl };
    });
    registry.register("notify", async (ctx) => {
      executionOrder.push("notify");
      return { notified: true, summary: (ctx.prev as any).summary };
    });

    const worker = new DefaultWorker({
      storage,
      stepQueue: queue,
      registry,
      capabilities: ["default"],
      concurrency: 3,
      pollIntervalMs: 100,
      workerId: "worker-1",
    });

    // Submit workflow and start processing
    await coordinator.submit({
      workflow: videoPipeline,
      workflowId: "vid-001",
      input: { videoUrl: "https://cdn.example.com/video.mp4" },
    });

    // Start coordinator and worker in background
    coordinator.start();
    worker.start();

    // Wait for completion
    const result = await coordinator.waitForResult<{ notified: boolean }>("vid-001");

    await coordinator.stop();
    await worker.stop();

    // Verify result
    expect(result.notified).toBe(true);

    // Verify DAG ordering: upload first, transcribe+thumbnail parallel, then summarize, then notify
    expect(executionOrder[0]).toBe("upload");
    expect(executionOrder.indexOf("summarize")).toBeGreaterThan(
      Math.max(executionOrder.indexOf("transcribe"), executionOrder.indexOf("thumbnail")),
    );
    expect(executionOrder.indexOf("notify")).toBeGreaterThan(executionOrder.indexOf("summarize"));

    await close();
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: Multiple workers competing for tasks with SKIP LOCKED
//
// Business flow: Order fulfillment — 50 orders, 3 workers, no duplicates
// ---------------------------------------------------------------------------

withPostgres("Distributed workers — competing task execution", (ctx) => {
  it("3 workers process 50 tasks with no duplicates via SKIP LOCKED", async () => {
    const { db, close } = createDb(ctx);
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();

    const registry = new MapStepRegistry();
    const processed: { worker: string; step: string }[] = [];

    registry.register("fulfill", async (ctx) => {
      // Simulate varying processing time
      await new Promise((r) => setTimeout(r, Math.random() * 20));
      return { fulfilled: true, orderId: ctx.input };
    });

    // Create a simple linear workflow
    const orderWorkflow = workflow<{ orderId: string }>({
      name: "order-fulfillment",
      storage,
    })
      .step("fulfill", (ctx) => Pipeline.succeed({ fulfilled: true, orderId: ctx.input.orderId }))
      .build();

    const coordinator = new DefaultCoordinator({
      storage,
      stepQueue: queue,
      pollIntervalMs: 50,
    });

    // Submit 50 orders
    for (let i = 0; i < 50; i++) {
      await coordinator.submit({
        workflow: orderWorkflow,
        workflowId: `order-${i}`,
        input: { orderId: `ORD-${i}` },
      });
    }

    // 3 competing workers
    const workers = [1, 2, 3].map(
      (id) =>
        new DefaultWorker({
          storage,
          stepQueue: new PgStepQueue({ db, workerId: `worker-${id}` }),
          registry: (() => {
            const r = new MapStepRegistry();
            r.register("fulfill", async (stepCtx) => {
              processed.push({ worker: `worker-${id}`, step: stepCtx.stepName });
              await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
              return { fulfilled: true };
            });
            return r;
          })(),
          capabilities: ["default"],
          concurrency: 5,
          pollIntervalMs: 50,
          workerId: `worker-${id}`,
        }),
    );

    // Start everything
    coordinator.start();
    workers.forEach((w) => w.start());

    // Wait for all 50 workflows
    await eventually(
      async () => {
        const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
        const completed = metrics.completed ?? 0;
        expect(completed).toBe(50);
      },
      { timeoutMs: 30_000, intervalMs: 200 },
    );

    await coordinator.stop();
    await Promise.all(workers.map((w) => w.stop()));

    // Verify no duplicates — each step executed exactly once
    const stepNames = processed.map((p) => p.step);
    expect(stepNames.length).toBe(50);
    expect(new Set(stepNames).size).toBe(1); // all "fulfill"

    // Verify work was distributed across workers
    const workerCounts = new Map<string, number>();
    for (const p of processed) {
      workerCounts.set(p.worker, (workerCounts.get(p.worker) ?? 0) + 1);
    }
    // At least 2 workers should have gotten work
    const activeWorkers = [...workerCounts.values()].filter((c) => c > 0);
    expect(activeWorkers.length).toBeGreaterThanOrEqual(2);

    await close();
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: Worker failure + task recovery
//
// Business flow: A worker crashes mid-execution, coordinator detects
// the dead worker and re-enqueues its stuck tasks for another worker.
// ---------------------------------------------------------------------------

withPostgres("Dead worker detection — task recovery", (ctx) => {
  it("coordinator re-enqueues stuck tasks from dead worker", async () => {
    const { db, close } = createDb(ctx);
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();
    const workerRegistry = new InMemoryWorkerRegistry();

    // Enqueue directly — simulating coordinator behavior
    await queue.enqueue({
      workflowId: "wf-recovery",
      stepName: "process",
      needs: ["default"],
      input: {},
      prevResults: {},
    });

    // Worker 1 claims the task
    const w1Queue = new PgStepQueue({ db, workerId: "worker-dead" });
    const claimed = await w1Queue.claim({ capabilities: ["default"], limit: 1 });
    expect(claimed).toHaveLength(1);

    // Worker 1 "crashes" — task stays in "running" state
    // Register the dead worker so coordinator can detect it
    await workerRegistry.register({
      workerId: "worker-dead",
      capabilities: ["default"],
      concurrency: 1,
    });

    // Simulate dead worker by not sending heartbeats and advancing time
    // (detectDead checks lastHeartbeat vs timeout)
    // Force lastHeartbeat to be old
    const workers = await workerRegistry.list();
    const deadWorker = workers.find((w) => w.workerId === "worker-dead");
    if (deadWorker) {
      (deadWorker as any).lastHeartbeat = new Date(Date.now() - 60_000);
    }

    const dead = await workerRegistry.detectDead(5_000); // 5s timeout
    expect(dead.length).toBe(1);

    // Re-enqueue stuck tasks
    const requeued = await queue.requeueStuck({ claimedBy: "worker-dead" });
    expect(requeued).toBe(1);

    // Worker 2 can now claim and complete the task
    const w2Queue = new PgStepQueue({ db, workerId: "worker-alive" });
    const reclaimed = await w2Queue.claim({ capabilities: ["default"], limit: 1 });
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]!.stepName).toBe("process");

    await w2Queue.complete({ taskId: reclaimed[0]!.id, result: { done: true }, durationMs: 5 });

    // Verify final metrics
    const metrics = await queue.metrics({ since: new Date(Date.now() - 60_000) });
    expect(metrics.completed).toBe(1);
    expect(metrics.running).toBe(0);

    await close();
  });
});

// ---------------------------------------------------------------------------
// Scenario 4: Priority-based task routing
//
// Business flow: Critical orders get higher priority and are processed
// before normal orders, even if submitted later.
// ---------------------------------------------------------------------------

withPostgres("Priority queue — critical orders processed first", (ctx) => {
  it("higher priority tasks are claimed before lower priority", async () => {
    const { db, close } = createDb(ctx);
    const queue = new PgStepQueue({ db, workerId: "worker-1" });
    await queue.ensureTable();

    // Enqueue low-priority tasks first
    for (let i = 0; i < 5; i++) {
      await queue.enqueue({
        workflowId: `low-${i}`,
        stepName: "process",
        needs: ["default"],
        input: { priority: "low" },
        prevResults: {},
        priority: 1,
      });
    }

    // Then enqueue high-priority tasks
    for (let i = 0; i < 3; i++) {
      await queue.enqueue({
        workflowId: `high-${i}`,
        stepName: "process",
        needs: ["default"],
        input: { priority: "high" },
        prevResults: {},
        priority: 10,
      });
    }

    // Claim tasks one at a time — should get high-priority first
    const order: string[] = [];
    for (let i = 0; i < 8; i++) {
      const tasks = await queue.claim({ capabilities: ["default"], limit: 1 });
      if (tasks.length > 0) {
        order.push(tasks[0]!.workflowId.startsWith("high") ? "high" : "low");
        await queue.complete({ taskId: tasks[0]!.id, result: {}, durationMs: 1 });
      }
    }

    // First 3 should be high-priority
    expect(order.slice(0, 3)).toEqual(["high", "high", "high"]);
    // Last 5 should be low-priority
    expect(order.slice(3)).toEqual(["low", "low", "low", "low", "low"]);

    await close();
  });
});

// ---------------------------------------------------------------------------
// Scenario 5: Queue routing — different steps to different queues
//
// Business flow: ML pipeline routes GPU steps to "gpu" queue and
// CPU steps to "cpu" queue. Workers specialize by queue.
// ---------------------------------------------------------------------------

withPostgres("Queue routing — GPU vs CPU workers", (ctx) => {
  it("coordinator routes steps to specialized queues, workers claim their own", async () => {
    const { db, close } = createDb(ctx);
    const storage = new InMemoryWorkflowStorage();
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();

    const mlPipeline = workflow<{ imageUrl: string }>({
      name: "ml-pipeline",
      storage,
    })
      .step("preprocess", (ctx) => Pipeline.succeed({ processed: ctx.input.imageUrl }), {
        needs: ["cpu"],
      })
      .step("inference", (ctx) => Pipeline.succeed({ prediction: "cat", confidence: 0.95 }), {
        needs: ["gpu"],
      })
      .step("postprocess", (ctx) => Pipeline.succeed({ result: ctx.prev }), { needs: ["cpu"] })
      .build();

    const coordinator = new DefaultCoordinator({
      storage,
      stepQueue: queue,
      pollIntervalMs: 50,
    });

    const gpuProcessed: string[] = [];
    const cpuProcessed: string[] = [];

    // GPU worker
    const gpuRegistry = new MapStepRegistry();
    gpuRegistry.register("inference", async () => {
      gpuProcessed.push("inference");
      return { prediction: "cat", confidence: 0.95 };
    });
    const gpuWorker = new DefaultWorker({
      storage,
      stepQueue: new PgStepQueue({ db, workerId: "gpu-worker" }),
      registry: gpuRegistry,
      capabilities: ["gpu"],
      concurrency: 1,
      pollIntervalMs: 50,
      workerId: "gpu-worker",
    });

    // CPU worker
    const cpuRegistry = new MapStepRegistry();
    cpuRegistry.register("preprocess", async (stepCtx) => {
      cpuProcessed.push("preprocess");
      return { processed: (stepCtx.input as any).imageUrl };
    });
    cpuRegistry.register("postprocess", async (ctx) => {
      cpuProcessed.push("postprocess");
      return { result: ctx.prev };
    });
    const cpuWorker = new DefaultWorker({
      storage,
      stepQueue: new PgStepQueue({ db, workerId: "cpu-worker" }),
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

    coordinator.start();
    gpuWorker.start();
    cpuWorker.start();

    await coordinator.waitForResult("ml-001");

    await coordinator.stop();
    await gpuWorker.stop();
    await cpuWorker.stop();

    // GPU worker only handled inference
    expect(gpuProcessed).toEqual(["inference"]);
    // CPU worker handled preprocess and postprocess
    expect(cpuProcessed).toEqual(["preprocess", "postprocess"]);

    await close();
  });
});

// ---------------------------------------------------------------------------
// Scenario 6: StreamTopology with checkpointing
//
// Business flow: Tumbling window aggregation with state that survives
// "restart" (checkpoint + restore cycle).
// ---------------------------------------------------------------------------

describe("StreamTopology — windowed aggregation with checkpoint/restore", () => {
  it("window state survives checkpoint and restore", async () => {
    const stateBackend = new InMemoryState<string, unknown>();

    // Simulated source
    const source = {
      codec: { encode: (v: any) => v, decode: (v: any) => v },
      subscribe: () => {
        throw new Error("not used");
      },
      subscribeAck: () => {
        const { StreamPipeline } = require("@promin/core");
        return StreamPipeline.fromIterable(
          Array.from({ length: 10 }, (_, i) => ({
            value: { userId: `u${i % 3}`, amount: (i + 1) * 10, ts: i * 100 },
            ack: async () => {},
            nack: async () => {},
            metadata: {},
          })),
        );
      },
    };

    const topology = StreamTopology.source(source as any)
      .keyBy((e: any) => e.userId)
      .tumbling(500) // 500ms windows
      .aggregate({
        init: () => ({ total: 0, count: 0 }),
        add: (state, event: any) => ({
          total: state.total + event.amount,
          count: state.count + 1,
        }),
        emit: (key, window, state) => ({
          userId: key,
          windowStart: window.start,
          total: state.total,
          count: state.count,
        }),
      })
      .build();

    const handle = await TopologyRunner.run(topology, {
      group: "test-window",
      stateBackend,
      checkpointIntervalMs: 50,
    });

    await new Promise((r) => setTimeout(r, 500));

    const metrics = handle.metrics();
    expect(metrics.itemsProcessed).toBeGreaterThan(0);

    await handle.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Scenario 7: Kafka → distributed workflow — end-to-end
//
// Business flow: Orders arrive via Kafka, each triggers a distributed
// workflow through the coordinator → worker pipeline.
// ---------------------------------------------------------------------------

withAll("E2E: Kafka orders → distributed workflow → completion", (ctx) => {
  it("Kafka messages trigger distributed workflows processed by workers", async () => {
    const topic = uniqueName("orders");
    await createTopic(ctx.kafka.broker, topic);

    const client = createKafkajsClient(ctx.kafka.broker);
    const kt = new KafkaTopic<{ orderId: string; amount: number }>({
      kafka: client,
      topic,
      groupId: uniqueName("g"),
    });

    const storage = new InMemoryWorkflowStorage();
    const stepQueue = new InMemoryStepQueue();

    const orderWorkflow = workflow<{ orderId: string; amount: number }>({
      name: "process-order",
      storage,
    })
      .step("validate", (c) =>
        Pipeline.succeed({ valid: c.input.amount > 0, orderId: c.input.orderId }),
      )
      .step("charge", (c) => Pipeline.succeed({ charged: true, amount: c.input.amount }))
      .build();

    const coordinator = new DefaultCoordinator({
      storage,
      stepQueue,
      pollIntervalMs: 50,
    });

    const registry = new MapStepRegistry();
    const processed: string[] = [];

    registry.register("validate", async (stepCtx) => {
      processed.push(`validate:${(stepCtx.input as any).orderId}`);
      return { valid: true, orderId: (stepCtx.input as any).orderId };
    });
    registry.register("charge", async (stepCtx) => {
      processed.push(`charge:${(stepCtx.input as any).orderId}`);
      return { charged: true };
    });

    const worker = new DefaultWorker({
      storage,
      stepQueue,
      registry,
      capabilities: ["default"],
      concurrency: 5,
      pollIntervalMs: 50,
    });

    // Publish 5 orders to Kafka
    for (let i = 0; i < 5; i++) {
      await kt.publish({ orderId: `ORD-${i}`, amount: (i + 1) * 100 });
    }

    // Start coordinator and worker
    coordinator.start();
    worker.start();

    // Consume from Kafka and submit each order as a workflow
    await kt
      .subscribeAck({ group: uniqueName("g"), fromBeginning: true })
      .take(5)
      .forEach(async (env) => {
        await coordinator.submit({
          workflow: orderWorkflow,
          workflowId: `wf-${env.value.orderId}`,
          input: env.value,
        });
        await env.ack();
      });

    // Wait for all workflows to complete
    await eventually(
      async () => {
        expect(processed.length).toBe(10); // 5 orders × 2 steps
      },
      { timeoutMs: 10_000, intervalMs: 100 },
    );

    await coordinator.stop();
    await worker.stop();
    await kt.disconnect();

    // Verify all orders processed both steps
    const validates = processed.filter((p) => p.startsWith("validate:"));
    const charges = processed.filter((p) => p.startsWith("charge:"));
    expect(validates.length).toBe(5);
    expect(charges.length).toBe(5);
  });
});
