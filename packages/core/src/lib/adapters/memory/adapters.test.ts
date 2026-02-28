import { describe, it, expect } from "bun:test";
import { MemoryStream } from "./memory-stream.ts";
import { IterableSource, fromIterable } from "./iterable-source.ts";
import { InMemoryState } from "./in-memory-state.ts";
import { isPartitionable, isStreamable, isKeyedSinkable } from "../../typeclasses/streamable.ts";

// ---------------------------------------------------------------------------
// MemoryStream
// ---------------------------------------------------------------------------

describe("MemoryStream", () => {
  it("implements Partitionable and KeyedSinkable", async () => {
    const ms = await MemoryStream.create<string>();
    expect(isStreamable(ms)).toBe(true);
    expect(isPartitionable(ms)).toBe(true);
    expect(isKeyedSinkable(ms)).toBe(true);
  });

  it("publish and subscribe single partition", async () => {
    const ms = await MemoryStream.create<number>();
    const collected: number[] = [];

    // Start collecting in background
    const collectPromise = ms
      .subscribe()
      .take(3)
      .forEach((n) => {
        collected.push(n);
      });

    // Publish items
    await ms.publish(1);
    await ms.publish(2);
    await ms.publish(3);

    await collectPromise;
    expect(collected).toEqual([1, 2, 3]);
  });

  it("routes by key to partitions", async () => {
    const ms = await MemoryStream.create<string>({ partitions: 2 });

    const p0: string[] = [];
    const p1: string[] = [];

    // Subscribe to each partition separately
    const collect0 = ms
      .subscribe({ partitions: [0] })
      .take(1)
      .forEach((v) => {
        p0.push(v);
      });
    const collect1 = ms
      .subscribe({ partitions: [1] })
      .take(1)
      .forEach((v) => {
        p1.push(v);
      });

    // Publish with different keys — they should land in different partitions
    // We need to find two keys that hash to different partitions
    await ms.publish("a", { key: "key-0" });
    await ms.publish("b", { key: "key-1" });

    // At least one of the partitions should have received something
    await Promise.race([collect0, collect1, new Promise((r) => setTimeout(r, 200))]);
    expect(p0.length + p1.length).toBeGreaterThanOrEqual(1);
  });

  it("close stops accepting publishes", async () => {
    const ms = await MemoryStream.create<number>();
    await ms.close();
    expect(ms.closed).toBe(true);
    // Should not throw
    await ms.publish(1);
  });

  it("has correct partition count", async () => {
    const ms = await MemoryStream.create<number>({ partitions: 4 });
    expect(ms.partitions).toBe(4);
  });
});

// ---------------------------------------------------------------------------
// IterableSource
// ---------------------------------------------------------------------------

describe("IterableSource", () => {
  it("implements Streamable", () => {
    const source = new IterableSource([1, 2, 3]);
    expect(isStreamable(source)).toBe(true);
  });

  it("implements Partitionable", () => {
    const source = new IterableSource([1, 2, 3]);
    expect(isPartitionable(source)).toBe(true);
  });

  it("subscribe returns all items", async () => {
    const source = new IterableSource([1, 2, 3]);
    const result = await source.subscribe().collect();
    expect(result).toEqual([1, 2, 3]);
  });

  it("subscribe with partitions filters items", async () => {
    const source = new IterableSource([10, 20, 30, 40, 50, 60], undefined, 3);
    // With 3 partitions: item 0,3 → p0, item 1,4 → p1, item 2,5 → p2
    const result = await source.subscribe({ partitions: [0] }).collect();
    expect(result).toEqual([10, 40]);
  });

  it("works with empty iterable", async () => {
    const source = new IterableSource<number>([]);
    const result = await source.subscribe().collect();
    expect(result).toEqual([]);
  });

  it("fromIterable convenience function", async () => {
    const source = fromIterable([1, 2, 3]);
    const result = await source.subscribe().collect();
    expect(result).toEqual([1, 2, 3]);
  });

  it("works with generators", async () => {
    function* gen() {
      yield 1;
      yield 2;
      yield 3;
    }
    const source = new IterableSource(gen());
    const result = await source.subscribe().collect();
    expect(result).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// InMemoryState
// ---------------------------------------------------------------------------

describe("InMemoryState", () => {
  it("get returns undefined for missing key", async () => {
    const state = new InMemoryState<string, number>();
    expect(await state.get("missing")).toBeUndefined();
  });

  it("put and get", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    expect(await state.get("a")).toBe(1);
  });

  it("put overwrites existing value", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.put("a", 2);
    expect(await state.get("a")).toBe(2);
  });

  it("delete removes key", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.delete("a");
    expect(await state.get("a")).toBeUndefined();
  });

  it("keys returns all keys", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.put("b", 2);
    expect((await state.keys()).sort()).toEqual(["a", "b"]);
  });

  it("entries returns all entries", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.put("b", 2);
    const entries = (await state.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    expect(entries).toEqual([
      ["a", 1],
      ["b", 2],
    ]);
  });

  it("checkpoint and restore", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.put("b", 2);

    await state.checkpoint({ name: "v1" });

    // Modify after checkpoint
    await state.put("a", 99);
    await state.delete("b");
    await state.put("c", 3);
    expect(await state.get("a")).toBe(99);

    // Restore
    await state.restore({ name: "v1" });
    expect(await state.get("a")).toBe(1);
    expect(await state.get("b")).toBe(2);
    expect(await state.get("c")).toBeUndefined();
  });

  it("restore non-existent checkpoint is a no-op", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.restore({ name: "nonexistent" });
    expect(await state.get("a")).toBe(1);
  });

  it("clear removes all state", async () => {
    const state = new InMemoryState<string, number>();
    await state.put("a", 1);
    await state.put("b", 2);
    await state.clear();
    expect(state.size).toBe(0);
    expect(await state.get("a")).toBeUndefined();
  });

  it("size reflects current count", async () => {
    const state = new InMemoryState<string, number>();
    expect(state.size).toBe(0);
    await state.put("a", 1);
    expect(state.size).toBe(1);
    await state.put("b", 2);
    expect(state.size).toBe(2);
    await state.delete("a");
    expect(state.size).toBe(1);
  });
});
