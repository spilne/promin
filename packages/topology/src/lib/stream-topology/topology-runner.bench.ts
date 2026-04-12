import { group, bench, run } from "mitata";
import { Stream, Chunk } from "effect";
import { StreamPipeline } from "@promin/core";
import { StreamTopology } from "./stream-topology.ts";
import { TopologyRunner } from "./topology-runner.ts";
import { makeArray, mapFn, filterFn } from "@promin/core";
import type { Streamable, Acknowledgeable, Sinkable } from "@promin/core";

/**
 * Benchmarks for TopologyRunner:
 * 1. Compiled output: unfused vs fused mapChunks (no runner overhead)
 * 2. Full lifecycle: source → operators → sink via TopologyRunner.run()
 */

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function arraySource<T>(items: T[]): Streamable<T> & Acknowledgeable<T> {
  return {
    subscribe: () => StreamPipeline.fromIterable(items),
    subscribeAck: () =>
      StreamPipeline.fromIterable(
        items.map((value) => ({
          value,
          offset: { partition: 0, offset: 0 },
          ack: async () => {},
          nack: async () => {},
          metadata: {},
        })),
      ),
  } as unknown as Streamable<T> & Acknowledgeable<T>;
}

/** Sink that resolves a promise when `expectedCount` items arrive or timeout. */
function countingSink<T>(
  expectedCount: number,
  timeoutMs: number = 5000,
): Sinkable<T> & { done: Promise<T[]>; items: T[] } {
  const items: T[] = [];
  let resolve: (items: T[]) => void;
  const done = new Promise<T[]>((r) => {
    resolve = r;
    setTimeout(() => r(items), timeoutMs);
  });
  return {
    items,
    done,
    publish: async (value: T) => {
      items.push(value);
      if (items.length >= expectedCount) resolve(items);
    },
  } as Sinkable<T> & { done: Promise<T[]>; items: T[] };
}

// ---------------------------------------------------------------------------
// 1. Compiled output: unfused vs fused (no runner overhead)
// ---------------------------------------------------------------------------

group("compiled output: 5 maps (100K)", () => {
  const data = makeArray(100_000);

  bench("unfused: .map().map().map().map().map()", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });

  bench("fused: single mapChunks", async () => {
    const fused = (x: unknown) => {
      let v = x as number;
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      return v;
    };
    return new StreamPipeline(
      Stream.mapChunks(Stream.fromIterable(data), Chunk.map(fused)),
    ).collect();
  });
});

group("compiled output: mixed chain (100K)", () => {
  const data = makeArray(100_000);
  const SKIP = Symbol();

  bench("unfused: .map().filter().map().filter().map()", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .collect();
  });

  bench("fused: single mapChunks", async () => {
    const fused = (value: unknown): unknown => {
      let v = mapFn(value as number);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      return v;
    };
    return new StreamPipeline(
      Stream.mapChunks(Stream.fromIterable(data), (chunk) => {
        const src = Chunk.toArray(chunk);
        const result: unknown[] = [];
        for (let i = 0; i < src.length; i++) {
          const v = fused(src[i]);
          if (v !== SKIP) result.push(v);
        }
        return Chunk.unsafeFromArray(result);
      }),
    ).collect();
  });
});

// ---------------------------------------------------------------------------
// 2. Full TopologyRunner lifecycle
// ---------------------------------------------------------------------------

group("TopologyRunner e2e: map chain (10K)", () => {
  const data = makeArray(10_000);

  bench("source → 5x map → sink", async () => {
    const source = arraySource(data);
    const sink = countingSink<number>(data.length);

    const topology = StreamTopology.source(source)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: `bench-map-${Date.now()}` });
    await sink.done;
    await handle.shutdown();
  });
});

group("TopologyRunner e2e: mixed chain (10K)", () => {
  const data = makeArray(10_000);
  // Predict output count: apply map+filter chain to data
  const expectedCount = data
    .map(mapFn)
    .filter(filterFn)
    .map(mapFn)
    .filter(filterFn)
    .map(mapFn).length;

  bench("source → map.filter.map.filter.map → sink", async () => {
    const source = arraySource(data);
    const sink = countingSink<number>(expectedCount);

    const topology = StreamTopology.source(source)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: `bench-mixed-${Date.now()}` });
    await sink.done;
    await handle.shutdown();
  });
});

group("TopologyRunner e2e: keyed window aggregate (1K events)", () => {
  // 1K events across 10 keys, tumbling 1-second windows.
  // Windows 0-8 emit when the next window's first event arrives (9 windows × 10 keys = 90 results).
  // The last window (window 9) only emits on shutdown, so we expect ~90 results before timeout.
  const data = Array.from({ length: 1_000 }, (_, i) => ({
    userId: `user-${i % 10}`,
    amount: i * 10,
    ts: 1000 + Math.floor(i / 100) * 1000,
  }));

  bench("source → keyBy → tumbling(1s) → sum → sink", async () => {
    const source = arraySource(data);
    const sink = countingSink(90, 2000);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.userId)
      .tumbling(1000)
      .sum((e) => e.amount)
      .to(sink as Sinkable<any>);

    const handle = await TopologyRunner.run(topology, { group: `bench-window-${Date.now()}` });
    await sink.done;
    await handle.shutdown();
  });
});

await run();
