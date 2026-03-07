/**
 * Concurrency primitives — queues, channels, pubsub, refs, deferred
 *
 * Building blocks for coordinating concurrent pipelines.
 */

import {
  StreamPipeline,
  PipelineQueue,
  PipelineChannel,
  PipelinePubSub,
  PipelineRef,
  PipelineDeferred,
} from "@promin/core";

// PipelineRef — mutable reference for shared state
async function refExample() {
  const counter = PipelineRef.make(0);

  await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
    .tapAsync(async () => {
      await counter.updateAsync((n) => n + 1);
    })
    .drain();

  const count = await counter.getAsync();
  console.log("Count:", count); // 5
}

// PipelineDeferred — one-shot async value
async function deferredExample() {
  const deferred = PipelineDeferred.make<string>();

  // Consumer waits for the value
  const consumer = deferred.awaitAsync();

  // Producer completes the deferred after a delay
  setTimeout(async () => {
    await deferred.succeedAsync("hello");
  }, 100);

  const result = await consumer;
  console.log(`Got: ${result}`); // "Got: hello"
}

// PipelineQueue — bounded FIFO queue with backpressure
async function queueExample() {
  const queue = PipelineQueue.make<number>(10);

  // Producer
  for (let i = 0; i < 5; i++) {
    await queue.offerAsync(i);
  }

  // Consumer — take from queue as a stream
  const items = await queue.toStream().take(5).collect();
  console.log(items); // [0, 1, 2, 3, 4]
}

// PipelineChannel — multi-producer, multi-consumer with close semantics
async function channelExample() {
  const channel = PipelineChannel.make<string>(100);

  // Start consumer
  const consumer = channel
    .toStream()
    .map((s) => s.toUpperCase())
    .collect();

  // Produce and close
  await channel.sendAsync("hello");
  await channel.sendAsync("world");
  await channel.closeAsync();

  const results = await consumer;
  console.log(results); // ["HELLO", "WORLD"]
}

// PipelinePubSub — broadcast to multiple subscribers
async function pubsubExample() {
  const bus = PipelinePubSub.make<{ type: string; data: string }>(100);

  // Subscriber 1 — only processes "alert" events
  const alerts = bus
    .subscribe()
    .filter((e) => e.type === "alert")
    .take(1)
    .collect();

  // Subscriber 2 — processes all events
  const all = bus
    .subscribe()
    .take(3)
    .collect();

  // Publish events
  await bus.publishAsync({ type: "info", data: "started" });
  await bus.publishAsync({ type: "alert", data: "cpu high" });
  await bus.publishAsync({ type: "info", data: "recovered" });

  console.log("Alerts:", (await alerts).length); // 1
  console.log("All:", (await all).length); // 3
}

// Worker pool pattern — multiple consumers sharing a channel
async function workerPool() {
  const jobs = PipelineChannel.make<number>(100);
  const results = PipelineRef.make<number[]>([]);

  // Spawn 3 workers
  const workers = Array.from({ length: 3 }, (_, id) =>
    jobs
      .toStream()
      .tapAsync(async (job) => {
        const result = job * 10;
        await results.updateAsync((r) => [...r, result]);
      })
      .drain(),
  );

  // Feed jobs
  for (let i = 1; i <= 9; i++) {
    await jobs.sendAsync(i);
  }
  await jobs.closeAsync();

  await Promise.all(workers);

  const allResults = await results.getAsync();
  console.log("Results:", allResults.sort((a, b) => a - b));
  // [10, 20, 30, 40, 50, 60, 70, 80, 90]
}

export { refExample, deferredExample, queueExample, channelExample, pubsubExample, workerPool };
