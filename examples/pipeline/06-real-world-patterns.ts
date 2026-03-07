/**
 * Real-world patterns — composition patterns from production use cases
 *
 * Based on PATTERNS.md — these demonstrate how primitives compose
 * into complex, resilient data pipelines.
 */

import {
  Pipeline,
  StreamPipeline,
  PipelineSemaphore,
  PipelineRef,
  PipelineQueue,
  PipelinePool,
  PipelinePubSub,
  PipelineSignal,
} from "@promin/core";

// --- Saga with compensating actions (Pipeline-level, not workflow-level) ---

async function sagaPattern() {
  const chargePayment = async (_id: string, _amount: number) => ({ paymentId: "pay_1" });
  const reserveInventory = async (_id: string) => ({ reservationId: "res_1" });
  const refundPayment = async (_id: string) => {};

  const result = await Pipeline.fromPromise(() => chargePayment("ord_1", 100))
    .flatMap((payment) =>
      Pipeline.fromPromise(() => reserveInventory("ord_1")).tapError(async () => {
        await refundPayment(payment.paymentId);
      }),
    )
    .runPromise();

  console.log("Saga result:", result);
}

// --- Rate-limited bulk processing with progress ---

async function rateLimitedBulk() {
  const limit = PipelineSemaphore.make(5);
  const progress = PipelineRef.make({ done: 0, total: 100 });

  const ids = Array.from({ length: 100 }, (_, i) => `item-${i}`);

  await StreamPipeline.fromIterable(ids)
    .parAsyncMap(20, async (id) => {
      // 20 fibers, but semaphore limits to 5 concurrent API calls
      return Pipeline.fromPromise(async () => {
        await new Promise((r) => setTimeout(r, 10));
        return { id, processed: true };
      })
        .withPermit(limit)
        .runPromise();
    })
    .tapAsync(async () => {
      await progress.updateAsync((p) => ({ ...p, done: p.done + 1 }));
    })
    .grouped(25)
    .tapAsync(async () => {
      const p = await progress.getAsync();
      console.log(`Progress: ${p.done}/${p.total}`);
    })
    .drain();
}

// --- Supervised background consumer ---

async function supervisedConsumer() {
  const queue = PipelineQueue.make<{ id: string }>(100);

  // Feed some items
  for (let i = 0; i < 5; i++) {
    await queue.offerAsync({ id: `item-${i}` });
  }

  // Consumer with restart-on-failure
  await Pipeline.fromPromise(async () => {
    await queue
      .toStream()
      .take(5)
      .tapAsync(async (item) => {
        console.log("Processing:", item.id);
      })
      .drain();
  })
    .supervised({ restart: "on-failure", maxRestarts: 3, intervalMs: 1_000 })
    .runPromise();
}

// --- Dynamic configuration with live reload ---

async function dynamicConfig() {
  const config = PipelineSignal.make({
    concurrency: 5,
    batchSize: 100,
    enableScoring: false,
  });

  // Update config at runtime
  await config.updateAsync((prev) => ({ ...prev, enableScoring: true }));

  const cfg = await config.getAsync();
  console.log("Config:", cfg);

  // Processing pipeline reads config on each batch
  await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
    .grouped(cfg.batchSize)
    .tapAsync(async (batch) => {
      const currentCfg = await config.getAsync();
      console.log(`Processing batch of ${batch.length}, scoring=${currentCfg.enableScoring}`);
    })
    .drain();
}

// --- Connection pool with lifecycle ---

async function connectionPool() {
  let created = 0;

  const pool = PipelinePool.make({
    acquire: async () => {
      created++;
      return { id: created, query: async (sql: string) => [{ count: 42 }] };
    },
    release: async (_conn) => {},
    size: 3,
  });

  // Each call acquires a connection, runs the query, auto-releases
  const result = await pool.useAsync((conn) => conn.query("SELECT count(*) FROM users"));
  console.log("Result:", result);
  console.log("Connections created:", created);
}

// --- Internal event bus with PubSub ---

async function eventBus() {
  type Event = { type: string; data: string };
  const bus = PipelinePubSub.make<Event>(100);

  // Consumer 1: count events
  const counter = PipelineRef.make(0);
  const consumer1 = bus
    .subscribe()
    .take(3)
    .tapAsync(async () => {
      await counter.updateAsync((n) => n + 1);
    })
    .drain();

  // Consumer 2: filter alerts
  const consumer2 = bus
    .subscribe()
    .filter((e) => e.type === "alert")
    .take(1)
    .collect();

  // Publish
  await bus.publishAsync({ type: "info", data: "started" });
  await bus.publishAsync({ type: "alert", data: "cpu spike" });
  await bus.publishAsync({ type: "info", data: "recovered" });

  await Promise.all([consumer1, consumer2]);
  console.log("Events counted:", await counter.getAsync());
}

// --- Fetch list → stream → parallel enrichment ---

async function fetchAndEnrich() {
  const ids = [1, 2, 3, 4, 5];

  const result = await StreamPipeline.fromPipeline(Pipeline.succeed(ids))
    .flatMap((ids) => StreamPipeline.fromIterable(ids))
    .parAsyncMap(3, async (id) => {
      const [data, stats] = await Pipeline.all(
        Pipeline.succeed({ id, name: `Item ${id}` }),
        Pipeline.succeed({ views: id * 100 }).orElse({ views: 0 }),
      ).runPromise();
      return { ...data, ...stats };
    })
    .filter((item) => item.views > 200)
    .collect();

  console.log("Enriched:", result);
}

export {
  sagaPattern,
  rateLimitedBulk,
  supervisedConsumer,
  dynamicConfig,
  connectionPool,
  eventBus,
  fetchAndEnrich,
};
