import { describe, it, expect } from "bun:test";
import { StreamPipeline } from "./stream-pipeline.ts";
import { IterableSource, fromIterable } from "./adapters/memory/iterable-source.ts";
import { MemoryStream } from "./adapters/memory/memory-stream.ts";
import { InMemoryState } from "./adapters/memory/in-memory-state.ts";
import type { Acknowledgeable, Envelope } from "./typeclasses/streamable.ts";
import { JsonCodec } from "./typeclasses/codec.ts";
import { Stream } from "effect";

// ---------------------------------------------------------------------------
// StreamPipeline.fromSource — Streamable
// ---------------------------------------------------------------------------

describe("StreamPipeline.fromSource", () => {
  it("works with IterableSource (Streamable)", async () => {
    const source = fromIterable([1, 2, 3]);
    const result = await StreamPipeline.fromSource(source).collect();
    expect(result).toEqual([1, 2, 3]);
  });

  it("works with IterableSource (Partitionable) selecting partitions", async () => {
    const source = new IterableSource([10, 20, 30, 40, 50, 60], undefined, 3);
    const result = await StreamPipeline.fromSource(source, { partitions: [1] }).collect();
    // Partition 1 gets items at index 1, 4 → values 20, 50
    expect(result).toEqual([20, 50]);
  });

  it("works with MemoryStream", async () => {
    const ms = await MemoryStream.create<number>();
    const collected: number[] = [];

    const collectPromise = StreamPipeline.fromSource(ms)
      .take(2)
      .forEach((n) => {
        collected.push(n);
      });

    await ms.publish(10);
    await ms.publish(20);

    await collectPromise;
    expect(collected).toEqual([10, 20]);
  });

  it("works with MemoryStream selecting partitions", async () => {
    const ms = await MemoryStream.create<string>({ partitions: 2 });
    const collected: string[] = [];

    const collectPromise = StreamPipeline.fromSource(ms, { partitions: [0] })
      .take(1)
      .forEach((v) => {
        collected.push(v);
      });

    // Publish to partition 0 (default when no key)
    await ms.publish("hello");

    await collectPromise;
    expect(collected).toEqual(["hello"]);
  });
});

// ---------------------------------------------------------------------------
// StreamPipeline.fromAck — Acknowledgeable
// ---------------------------------------------------------------------------

describe("StreamPipeline.fromAck", () => {
  it("creates stream with ack/nack envelopes", async () => {
    const acked: string[] = [];
    const items = [
      { id: "1", data: "a" },
      { id: "2", data: "b" },
    ];

    // Create a mock Acknowledgeable source
    const source: Acknowledgeable<{ id: string; data: string }> = {
      subscribe: () => StreamPipeline.fromIterable(items),
      subscribeAck: () => {
        const envelopes: Envelope<{ id: string; data: string }>[] = items.map((item) => ({
          value: item,
          ack: async () => {
            acked.push(item.id);
          },
          nack: async () => {},
          metadata: {},
        }));
        return StreamPipeline.from(Stream.fromIterable(envelopes));
      },
      codec: JsonCodec as any,
    };

    const result: string[] = [];
    await StreamPipeline.fromAck(source).forEach(async (envelope) => {
      result.push(envelope.value.data);
      await envelope.ack();
    });

    expect(result).toEqual(["a", "b"]);
    expect(acked).toEqual(["1", "2"]);
  });
});

// ---------------------------------------------------------------------------
// .through() — stream transformer
// ---------------------------------------------------------------------------

describe("StreamPipeline.through", () => {
  it("applies a stream transformer", async () => {
    const doubler = (stream: StreamPipeline<number, never>) => stream.map((n) => n * 2);

    const result = await StreamPipeline.fromIterable([1, 2, 3]).through(doubler).collect();

    expect(result).toEqual([2, 4, 6]);
  });

  it("chains multiple transformers", async () => {
    const doubler = (s: StreamPipeline<number, never>) => s.map((n) => n * 2);
    const stringer = (s: StreamPipeline<number, never>) => s.map((n) => `val:${n}`);

    const result = await StreamPipeline.fromIterable([1, 2, 3])
      .through(doubler)
      .through(stringer)
      .collect();

    expect(result).toEqual(["val:2", "val:4", "val:6"]);
  });
});

// ---------------------------------------------------------------------------
// .to() — Sinkable
// ---------------------------------------------------------------------------

describe("StreamPipeline.to", () => {
  it("publishes each item to a sink", async () => {
    const published: number[] = [];
    const sink = {
      publish: async (value: number) => {
        published.push(value);
      },
      codec: JsonCodec as any,
    };

    await StreamPipeline.fromIterable([1, 2, 3]).to(sink);
    expect(published).toEqual([1, 2, 3]);
  });

  it("publishes with key to a keyed sink", async () => {
    const published: { value: { id: string; data: string }; key: string }[] = [];
    const sink = {
      publish: async (value: { id: string; data: string }, params?: { key: string }) => {
        published.push({ value, key: params?.key ?? "" });
      },
      codec: JsonCodec as any,
    };

    await StreamPipeline.fromIterable([
      { id: "1", data: "a" },
      { id: "2", data: "b" },
    ]).to(sink, { key: (item) => item.id });

    expect(published).toEqual([
      { value: { id: "1", data: "a" }, key: "1" },
      { value: { id: "2", data: "b" }, key: "2" },
    ]);
  });

  it("publishes to MemoryStream", async () => {
    const ms = await MemoryStream.create<number>();
    const collected: number[] = [];

    const collectPromise = ms
      .subscribe()
      .take(3)
      .forEach((n) => {
        collected.push(n);
      });

    await StreamPipeline.fromIterable([10, 20, 30]).to(ms);

    await collectPromise;
    expect(collected).toEqual([10, 20, 30]);
  });
});

// ---------------------------------------------------------------------------
// .statefulMap() — keyed state
// ---------------------------------------------------------------------------

describe("StreamPipeline.statefulMap", () => {
  it("maintains state across items", async () => {
    const state = new InMemoryState<string, number>();

    const result = await StreamPipeline.fromIterable([
      { userId: "alice", action: "view" },
      { userId: "bob", action: "view" },
      { userId: "alice", action: "click" },
      { userId: "alice", action: "view" },
    ])
      .statefulMap({
        stateBackend: state,
        keyBy: (event) => event.userId,
        process: async (event, st) => {
          const count = (await st.get(event.userId)) ?? 0;
          await st.put(event.userId, count + 1);
          return { userId: event.userId, actionCount: count + 1 };
        },
      })
      .collect();

    expect(result).toEqual([
      { userId: "alice", actionCount: 1 },
      { userId: "bob", actionCount: 1 },
      { userId: "alice", actionCount: 2 },
      { userId: "alice", actionCount: 3 },
    ]);

    // State should reflect final counts
    expect(await state.get("alice")).toBe(3);
    expect(await state.get("bob")).toBe(1);
  });

  it("works with empty stream", async () => {
    const state = new InMemoryState<string, number>();
    const result = await StreamPipeline.fromIterable([])
      .statefulMap({
        stateBackend: state,
        keyBy: () => "key",
        process: async () => "nope",
      })
      .collect();
    expect(result).toEqual([]);
  });
});
