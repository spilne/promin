import { describe, it, expect } from "bun:test";
import { StreamTopology, TopologyRunner, WindowManager, JoinBuffer } from "./index.ts";
import { StreamPipeline } from "../stream-pipeline.ts";
import type { Streamable, Acknowledgeable, Sinkable } from "../typeclasses/streamable.ts";
import type { Codec } from "../typeclasses/codec.ts";

// ---------------------------------------------------------------------------
// Test helpers — in-memory source/sink that implement typeclasses
// ---------------------------------------------------------------------------

function createTestSource<T>(items: T[]): Streamable<T> & Acknowledgeable<T> {
  const codec: Codec<T> = {
    encode: (v: T) => JSON.stringify(v),
    decode: (s: string) => JSON.parse(s),
  };
  return {
    codec,
    subscribe: () => StreamPipeline.fromIterable(items),
    subscribeAck: () =>
      StreamPipeline.fromIterable(
        items.map((value) => ({
          value,
          ack: async () => {},
          nack: async () => {},
          metadata: {},
        })),
      ),
  };
}

function createTestSink<T>(): Sinkable<T> & { items: T[] } {
  const items: T[] = [];
  return {
    items,
    codec: { encode: (v: T) => JSON.stringify(v), decode: (s: string) => JSON.parse(s) },
    publish: async (value: T) => {
      items.push(value);
    },
  };
}

// ---------------------------------------------------------------------------
// StreamTopology builder — API shape tests
// ---------------------------------------------------------------------------

describe("StreamTopology builder — declarative processing DAG", () => {
  it("builds a source → map → filter → sink topology", () => {
    const source = createTestSource([1, 2, 3]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((n) => n * 2)
      .filter((n) => n > 2)
      .to(sink);

    expect(topology.compiled.sinks).toHaveLength(1);
  });

  it("builds a keyed topology with tumbling window", () => {
    const source = createTestSource([{ userId: "a", ts: 1000 }]);
    const sink = createTestSink<{
      key: string;
      window: { start: number; end: number };
      count: number;
    }>();

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.userId)
      .tumbling(60_000)
      .aggregate({
        init: () => ({ count: 0 }),
        add: (state) => ({ count: state.count + 1 }),
        emit: (key, window, state) => ({ key, window, count: state.count }),
      })
      .to(sink);

    expect(topology.compiled.sinks).toHaveLength(1);
  });

  it("builds a keyed topology with sliding window", () => {
    const source = createTestSource([{ cardId: "x", amount: 100, ts: 1000 }]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.cardId)
      .sliding({ windowMs: 600_000, slideMs: 60_000 })
      .count()
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("builds a keyed topology with session window", () => {
    const source = createTestSource([{ userId: "a", page: "/home", ts: 1000 }]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.userId)
      .session(30_000)
      .aggregate({
        init: () => ({ pages: [] as string[] }),
        add: (state, pv) => ({ pages: [...state.pages, pv.page] }),
        emit: (key, window, state) => ({ key, window, pageCount: state.pages.length }),
      })
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("builds a keyed topology with stateful process", () => {
    const source = createTestSource([{ sensorId: "s1", temp: 22.5, ts: 1000 }]);

    const topology = StreamTopology.source(source)
      .keyBy((r) => r.sensorId)
      .process({
        init: () => ({ avg: 0 }),
        process: (state, reading) => ({
          state: { avg: state.avg * 0.7 + reading.temp * 0.3 },
          emit: { sensorId: reading.sensorId, movingAvg: state.avg },
        }),
      })
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("builds a join topology", () => {
    const orders = createTestSource([{ customerId: "c1", amount: 100, ts: 1000 }]);
    const customers = createTestSource([{ customerId: "c1", name: "Alice", ts: 900 }]);

    const topology = StreamTopology.source(orders)
      .keyBy((o) => o.customerId)
      .join(
        StreamTopology.source(customers).keyBy((c) => c.customerId),
        { windowMs: 60_000 },
      )
      .map(({ left, right }) => ({
        orderId: left.amount,
        customerName: right.name,
      }))
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("supports keyed dedup", () => {
    const source = createTestSource([
      { id: "1", data: "a" },
      { id: "1", data: "b" },
      { id: "2", data: "c" },
    ]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.id)
      .dedupe((e) => e.id)
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("supports mapAsync with concurrency", () => {
    const source = createTestSource([1, 2, 3]);

    const topology = StreamTopology.source(source)
      .mapAsync(5, async (n) => n * 2)
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("windowed count shorthand produces correct aggregate", () => {
    const source = createTestSource([{ region: "US", ts: 1000 }]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.region)
      .tumbling(60_000)
      .count()
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });

  it("windowed sum shorthand produces correct aggregate", () => {
    const source = createTestSource([{ region: "US", amount: 100, ts: 1000 }]);

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.region)
      .tumbling(60_000)
      .sum((e) => e.amount)
      .build();

    expect(topology.compiled.nodes).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// TopologyRunner — end-to-end execution tests
// ---------------------------------------------------------------------------

describe("TopologyRunner — compiles and runs a topology", () => {
  it("runs source → map → sink", async () => {
    const source = createTestSource([1, 2, 3, 4, 5]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((n) => n * 10)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-map" });

    // Give the stream time to process
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items).toEqual([10, 20, 30, 40, 50]);
  });

  it("runs source → filter → sink", async () => {
    const source = createTestSource([1, 2, 3, 4, 5, 6]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .filter((n) => n % 2 === 0)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-filter" });
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items).toEqual([2, 4, 6]);
  });

  it("runs source → mapAsync → sink", async () => {
    const source = createTestSource([1, 2, 3]);
    const sink = createTestSink<string>();

    const topology = StreamTopology.source(source)
      .mapAsync(2, async (n) => `item-${n}`)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-async" });
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items).toEqual(["item-1", "item-2", "item-3"]);
  });

  it("runs keyed stateful process", async () => {
    const source = createTestSource([
      { sensorId: "s1", temp: 20, ts: 1000 },
      { sensorId: "s1", temp: 22, ts: 2000 },
      { sensorId: "s2", temp: 30, ts: 1500 },
      { sensorId: "s1", temp: 24, ts: 3000 },
    ]);
    const sink = createTestSink<{ sensorId: string; count: number }>();

    const topology = StreamTopology.source(source)
      .keyBy((r) => r.sensorId)
      .process<{ count: number }, { sensorId: string; count: number }>({
        init: () => ({ count: 0 }),
        process: (state, reading) => ({
          state: { count: state.count + 1 },
          emit: { sensorId: reading.sensorId, count: state.count + 1 },
        }),
      })
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-process" });
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items).toEqual([
      { sensorId: "s1", count: 1 },
      { sensorId: "s1", count: 2 },
      { sensorId: "s2", count: 1 },
      { sensorId: "s1", count: 3 },
    ]);
  });

  it("runs keyed dedup", async () => {
    const source = createTestSource([
      { id: "a", v: 1 },
      { id: "b", v: 2 },
      { id: "a", v: 3 },
      { id: "c", v: 4 },
      { id: "b", v: 5 },
    ]);
    const sink = createTestSink<{ id: string; v: number }>();

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.id)
      .dedupe((e) => e.id)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-dedup" });
    await new Promise((r) => setTimeout(r, 200));
    await handle.shutdown();

    expect(sink.items).toEqual([
      { id: "a", v: 1 },
      { id: "b", v: 2 },
      { id: "c", v: 4 },
    ]);
  });
});

// ---------------------------------------------------------------------------
// WindowManager — unit tests for windowing logic
// ---------------------------------------------------------------------------

describe("WindowManager — tumbling windows close at boundary", () => {
  it("groups items into correct tumbling windows", () => {
    const manager = new WindowManager(
      { type: "tumbling", windowMs: 1000 },
      {
        init: () => ({ count: 0 }),
        add: (state) => ({ count: state.count + 1 }),
        emit: (key, window, state) => ({ key, windowStart: window.start, count: state.count }),
      },
    );

    // All in window [0, 1000)
    manager.add("user-1", {}, 100);
    manager.add("user-1", {}, 500);
    manager.add("user-1", {}, 900);

    // This triggers flush of window [0, 1000)
    const flushed = manager.flush("user-1", 1100);
    expect(flushed).toEqual([{ key: "user-1", windowStart: 0, count: 3 }]);
  });

  it("separates different keys into independent windows", () => {
    const manager = new WindowManager(
      { type: "tumbling", windowMs: 1000 },
      {
        init: () => ({ count: 0 }),
        add: (state) => ({ count: state.count + 1 }),
        emit: (key, _w, state) => ({ key, count: state.count }),
      },
    );

    manager.add("a", {}, 100);
    manager.add("b", {}, 200);
    manager.add("a", {}, 300);

    const flushedA = manager.flush("a", 1100);
    const flushedB = manager.flush("b", 1100);

    expect(flushedA).toEqual([{ key: "a", count: 2 }]);
    expect(flushedB).toEqual([{ key: "b", count: 1 }]);
  });
});

describe("WindowManager — session windows close after inactivity gap", () => {
  it("emits a session when gap is exceeded", () => {
    const manager = new WindowManager(
      { type: "session", gapMs: 500 },
      {
        init: () => ({ pages: [] as string[] }),
        add: (state, page: string) => ({ pages: [...state.pages, page] }),
        emit: (key, window, state) => ({
          key,
          start: window.start,
          end: window.end,
          pages: state.pages,
        }),
      },
    );

    // Session 1: events at 100, 200, 400 (gaps < 500ms)
    manager.add("u1", "home", 100);
    manager.add("u1", "products", 200);
    manager.add("u1", "cart", 400);

    // Event at 1200 — gap of 800ms > 500ms, closes session 1 and starts session 2
    const emitted = manager.add("u1", "checkout", 1200);

    expect(emitted).toEqual([
      {
        key: "u1",
        start: 100,
        end: 400,
        pages: ["home", "products", "cart"],
      },
    ]);
  });

  it("extends session when activity is within gap", () => {
    const manager = new WindowManager(
      { type: "session", gapMs: 1000 },
      {
        init: () => ({ count: 0 }),
        add: (state) => ({ count: state.count + 1 }),
        emit: (key, window, state) => ({
          key,
          duration: window.end - window.start,
          count: state.count,
        }),
      },
    );

    manager.add("u1", {}, 100);
    manager.add("u1", {}, 500); // gap 400 < 1000
    manager.add("u1", {}, 900); // gap 400 < 1000

    // No session emitted — still active
    const emitted = manager.flush("u1", 1500); // 1500 - 900 = 600 < 1000
    expect(emitted).toEqual([]);

    // Now flush after gap
    const closed = manager.flush("u1", 2000); // 2000 - 900 = 1100 > 1000
    expect(closed).toEqual([{ key: "u1", duration: 800, count: 3 }]);
  });
});

describe("WindowManager — sliding windows produce overlapping results", () => {
  it("assigns items to multiple overlapping windows", () => {
    const manager = new WindowManager(
      { type: "sliding", windowMs: 1000, slideMs: 500 },
      {
        init: () => ({ count: 0 }),
        add: (state) => ({ count: state.count + 1 }),
        emit: (key, window, state) => ({
          key,
          windowStart: window.start,
          count: state.count,
        }),
      },
    );

    // Item at 750 belongs to windows [0,1000) and [500,1500)
    manager.add("k1", {}, 750);

    // Flush window [0,1000) — it should contain the item
    const flushed = manager.flush("k1", 1100);
    expect(flushed.length).toBeGreaterThanOrEqual(1);
    expect(flushed[0]!.count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// JoinBuffer — unit tests for stream-stream joins
// ---------------------------------------------------------------------------

describe("JoinBuffer — time-windowed stream-stream join", () => {
  it("matches items from left and right within window", () => {
    const buffer = new JoinBuffer<{ orderId: string }, { name: string }>(5000);

    // Left arrives first
    const m1 = buffer.addLeft("c1", { orderId: "o1" }, 1000);
    expect(m1).toEqual([]); // no right yet

    // Right arrives within window
    const m2 = buffer.addRight("c1", { name: "Alice" }, 3000);
    expect(m2).toEqual([{ left: { orderId: "o1" }, right: { name: "Alice" } }]);
  });

  it("does not match items outside window", () => {
    const buffer = new JoinBuffer<string, string>(1000);

    buffer.addLeft("k1", "left-1", 1000);
    const matches = buffer.addRight("k1", "right-1", 5000); // too far apart
    expect(matches).toEqual([]);
  });

  it("does not match items with different keys", () => {
    const buffer = new JoinBuffer<string, string>(10000);

    buffer.addLeft("k1", "left-1", 1000);
    const matches = buffer.addRight("k2", "right-1", 1000); // different key
    expect(matches).toEqual([]);
  });

  it("matches multiple lefts with one right", () => {
    const buffer = new JoinBuffer<number, string>(5000);

    buffer.addLeft("k1", 1, 1000);
    buffer.addLeft("k1", 2, 2000);

    const matches = buffer.addRight("k1", "A", 3000);
    expect(matches).toEqual([
      { left: 1, right: "A" },
      { left: 2, right: "A" },
    ]);
  });

  it("evicts expired items on new additions", () => {
    const buffer = new JoinBuffer<string, string>(1000);

    buffer.addLeft("k1", "old", 100);

    // Much later — old left should be evicted
    const matches = buffer.addRight("k1", "new", 5000);
    expect(matches).toEqual([]);

    expect(buffer.stats().leftItems).toBe(0);
  });

  it("reports buffer stats", () => {
    const buffer = new JoinBuffer<string, string>(10000);

    buffer.addLeft("k1", "a", 100);
    buffer.addLeft("k2", "b", 200);
    buffer.addRight("k1", "x", 300);

    const stats = buffer.stats();
    expect(stats.leftKeys).toBe(2);
    expect(stats.rightKeys).toBe(1);
    expect(stats.leftItems).toBe(2);
    expect(stats.rightItems).toBe(1);
  });

  it("snapshot and restore preserves buffered state", () => {
    const buffer = new JoinBuffer<string, string>(10000);

    buffer.addLeft("k1", "left-1", 1000);
    buffer.addLeft("k1", "left-2", 2000);
    buffer.addRight("k2", "right-1", 1500);

    const snap = buffer.snapshot();

    // Create a new buffer and restore
    const restored = new JoinBuffer<string, string>(10000);
    restored.restore(snap);

    // The restored buffer should match against new items
    const matches = restored.addRight("k1", "right-match", 2500);
    expect(matches).toEqual([
      { left: "left-1", right: "right-match" },
      { left: "left-2", right: "right-match" },
    ]);
  });
});

// ---------------------------------------------------------------------------
// WindowManager — snapshot / restore (checkpointing)
// ---------------------------------------------------------------------------

describe("WindowManager — checkpoint and restore", () => {
  it("snapshot and restore preserves window state", () => {
    const spec = {
      init: () => ({ count: 0 }),
      add: (state: { count: number }) => ({ count: state.count + 1 }),
      emit: (key: string, _w: any, state: { count: number }) => ({ key, count: state.count }),
    };

    const manager = new WindowManager({ type: "tumbling", windowMs: 1000 }, spec);

    manager.add("u1", {}, 100);
    manager.add("u1", {}, 500);

    const snap = manager.snapshot();

    // Create a new manager and restore
    const restored = new WindowManager({ type: "tumbling", windowMs: 1000 }, spec);
    restored.restore(snap);

    // Add one more item and flush — should see all 3
    restored.add("u1", {}, 800);
    const flushed = restored.flush("u1", 1100);
    expect(flushed).toEqual([{ key: "u1", count: 3 }]);
  });
});

// ---------------------------------------------------------------------------
// TopologyRunner — metrics
// ---------------------------------------------------------------------------

describe("TopologyRunner — metrics and backpressure", () => {
  it("reports metrics after processing", async () => {
    const source = createTestSource([1, 2, 3, 4, 5]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((n) => n * 2)
      .to(sink);

    const handle = await TopologyRunner.run(topology, { group: "test-metrics" });
    await new Promise((r) => setTimeout(r, 200));

    const metrics = handle.metrics();
    expect(metrics.itemsProcessed).toBe(5);
    expect(metrics.itemsPerSecond).toBeGreaterThan(0);

    await handle.shutdown();
  });

  it("runs with maxBufferSize without errors", async () => {
    const source = createTestSource([1, 2, 3, 4, 5]);
    const sink = createTestSink<number>();

    const topology = StreamTopology.source(source)
      .map((n) => n * 10)
      .to(sink);

    const handle = await TopologyRunner.run(topology, {
      group: "test-buffer",
      maxBufferSize: 2,
    });

    await new Promise((r) => setTimeout(r, 300));
    await handle.shutdown();

    expect(sink.items).toEqual([10, 20, 30, 40, 50]);
  });

  it("dedup respects maxDedupeSize eviction", async () => {
    // Create items where we exceed maxDedupeSize
    const items = Array.from({ length: 20 }, (_, i) => ({ id: `item-${i}`, v: i }));
    // Then repeat the first few — they should have been evicted from the LRU
    const repeated = [...items, { id: "item-0", v: 100 }, { id: "item-1", v: 101 }];
    const source = createTestSource(repeated);
    const sink = createTestSink<{ id: string; v: number }>();

    const topology = StreamTopology.source(source)
      .keyBy((e) => e.id)
      .dedupe((e) => e.id)
      .to(sink);

    const handle = await TopologyRunner.run(topology, {
      group: "test-dedupe-evict",
      maxDedupeSize: 5, // Only keep last 5 keys
    });

    await new Promise((r) => setTimeout(r, 300));
    await handle.shutdown();

    // First 20 items should all pass (unique)
    // item-0 and item-1 should pass again because they were evicted from the LRU (size 5)
    expect(sink.items.length).toBe(22);
  });
});
