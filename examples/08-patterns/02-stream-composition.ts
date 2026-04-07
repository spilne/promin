/**
 * Stream composition patterns — fan-out, worker pools, pausable processing
 *
 * Shows advanced StreamPipeline patterns:
 * - broadcastThrough for parallel consumers
 * - Channel-based worker pools
 * - Pausable streams based on downstream health
 * - Non-blocking telemetry with observe
 */

import { StreamPipeline, PipelineQueue, PipelineChannel, PipelineRef } from "@promin/core";

// ---------------------------------------------------------------------------
// Fan-out: one stream, multiple parallel consumers
// ---------------------------------------------------------------------------

async function streamFanOut(eventStream: StreamPipeline<any, any>) {
  await eventStream
    .broadcastThrough(
      // Analytics: batch and write every 5s
      (s) => s.groupWithin(100, 5_000).tapAsync((batch) => writeBatch("analytics", batch)),
      // Alerts: filter anomalies
      (s) => s.filter((e) => e.event === "anomaly").tapAsync((e) => notify(e)),
      // Archive: write everything to storage
      (s) => s.grouped(1000).tapAsync((batch) => archive(batch)),
    )
    .drain();
}

// ---------------------------------------------------------------------------
// Channel-based worker pool with graceful shutdown
// ---------------------------------------------------------------------------

async function workerPool<T>(jobs: T[], concurrency: number, process: (job: T) => Promise<void>) {
  const channel = PipelineChannel.make<T>(500);

  // Spawn N workers consuming from the same channel
  const workers = Array.from({ length: concurrency }, (_, i) =>
    channel
      .toStream()
      .tapAsync(process)
      .onFinalize(async () => console.log(`Worker ${i} done`))
      .drain(),
  );

  // Feed jobs
  for (const job of jobs) {
    await channel.sendAsync(job);
  }
  await channel.closeAsync();

  await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// Pausable processing based on downstream health
// ---------------------------------------------------------------------------

async function pausableProcessing(
  queue: ReturnType<typeof PipelineQueue.make>,
  db: { getPoolUsage: () => Promise<number>; bulkInsert: (batch: any[]) => Promise<void> },
) {
  const paused = PipelineRef.make(false);

  // Monitor downstream — pause at 90% pool usage
  const monitor = setInterval(async () => {
    const load = await db.getPoolUsage();
    await paused.setAsync(load > 0.9);
  }, 5_000);

  await queue
    .toStream()
    .pauseWhen(paused)
    .groupWithin(100, 2_000)
    .tapAsync((batch) => db.bulkInsert(batch))
    .onFinalize(async () => clearInterval(monitor))
    .drain();
}

// ---------------------------------------------------------------------------
// Non-blocking telemetry with observe
// ---------------------------------------------------------------------------
// Main pipeline runs at full speed. Observer batches analytics in background.

async function nonBlockingTelemetry(stream: StreamPipeline<any, any>) {
  await stream
    .observe((s) => s.groupWithin(100, 5_000).tapAsync((batch) => writeBatch("telemetry", batch)))
    .parAsyncMap(10, (item) => enrichItem(item))
    .drain();
}

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

async function writeBatch(_target: string, _batch: unknown[]) {}
async function notify(_event: unknown) {}
async function archive(_batch: unknown[]) {}
async function enrichItem(_item: unknown) {
  return _item;
}

export { streamFanOut, workerPool, pausableProcessing, nonBlockingTelemetry };
