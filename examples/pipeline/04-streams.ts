/**
 * StreamPipeline — lazy, backpressured stream processing
 *
 * StreamPipeline<T, E> wraps Effect Stream with a fluent API.
 * Supports parallel processing, batching, deduplication,
 * sliding windows, and fan-out.
 */

import { Pipeline, StreamPipeline, PipelineRef } from "@promin/core";

// Create streams from various sources
async function streamSources() {
  // From iterable
  const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).collect();
  console.log(items); // [1, 2, 3, 4, 5]

  // From a pipeline (single value → stream of 1)
  const single = await StreamPipeline.fromPipeline(Pipeline.succeed("hello")).collect();
  console.log(single); // ["hello"]

  // Tick — emit at fixed intervals
  const ticks = await StreamPipeline.tick(100).take(3).collect();
  console.log("Ticks:", ticks.length); // 3

  // Unfold — generate from seed
  const fib = await StreamPipeline.unfold(
    [0, 1] as [number, number],
    async ([a, b]) => ({ value: a, next: [b, a + b] as [number, number] }),
  )
    .take(8)
    .collect();
  console.log("Fibonacci:", fib); // [0, 1, 1, 2, 3, 5, 8, 13]
}

// Transform operations
async function transforms() {
  const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    .filter((n) => n % 2 === 0) // [2, 4, 6, 8, 10]
    .map((n) => n * 10) // [20, 40, 60, 80, 100]
    .take(3) // [20, 40, 60]
    .collect();

  console.log(result); // [20, 40, 60]
}

// Parallel async processing
async function parallelProcessing() {
  const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
    .parAsyncMap(3, async (n) => {
      await new Promise((r) => setTimeout(r, 50));
      return n * 2;
    })
    .collect();

  console.log(result); // [2, 4, 6, 8, 10] — order preserved, 3 concurrent
}

// Batching — group items by count or time window
async function batching() {
  // Fixed-size groups
  const groups = await StreamPipeline.fromIterable([1, 2, 3, 4, 5, 6, 7])
    .grouped(3)
    .collect();

  console.log(groups); // [[1,2,3], [4,5,6], [7]]

  // Sliding window
  const windows = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
    .sliding(3)
    .collect();

  console.log(windows); // [[1,2,3], [2,3,4], [3,4,5]]
}

// Deduplication
async function deduplication() {
  const result = await StreamPipeline.fromIterable([1, 1, 2, 2, 3, 1, 3])
    .dedupe()
    .collect();

  console.log(result); // [1, 2, 3, 1, 3] — consecutive dedup

  // Distinct by key
  const unique = await StreamPipeline.fromIterable([
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
    { id: 1, name: "Alice (dup)" },
  ])
    .distinctBy((item) => item.id)
    .collect();

  console.log(unique.map((u) => u.name)); // ["Alice", "Bob"]
}

// Scan — running accumulator
async function scan() {
  const runningSum = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
    .scan(0, (acc, n) => acc + n)
    .collect();

  console.log(runningSum); // [1, 3, 6, 10, 15]
}

// Reduce — fold to single value
async function reduce() {
  const sum = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).reduce(0, (acc, n) => acc + n);

  console.log(sum); // 15
}

// Merge streams
async function mergeStreams() {
  const a = StreamPipeline.fromIterable([1, 2, 3]);
  const b = StreamPipeline.fromIterable([10, 20, 30]);

  const merged = await a.merge(b).collect();
  console.log("Merged:", merged.length); // 6 (order may vary)
}

// Concat — sequential composition
async function concat() {
  const result = await StreamPipeline.fromIterable([1, 2])
    .concat(StreamPipeline.fromIterable([3, 4]))
    .collect();

  console.log(result); // [1, 2, 3, 4]
}

// forEach with side effects
async function forEach() {
  const processed: number[] = [];

  await StreamPipeline.fromIterable([1, 2, 3]).forEach((n) => {
    processed.push(n * 10);
  });

  console.log(processed); // [10, 20, 30]
}

// Rate-limited processing with buffer
async function rateLimited() {
  const ref = PipelineRef.make(0);

  await StreamPipeline.fromIterable(Array.from({ length: 20 }, (_, i) => i))
    .buffer(5) // buffer up to 5 items ahead
    .metered(50) // emit at most 1 per 50ms
    .take(10)
    .tapAsync(async () => {
      await ref.updateAsync((n) => n + 1);
    })
    .drain();

  const count = await ref.getAsync();
  console.log("Processed:", count); // 10
}

export {
  streamSources,
  transforms,
  parallelProcessing,
  batching,
  deduplication,
  scan,
  reduce,
  mergeStreams,
  concat,
  forEach,
  rateLimited,
};
