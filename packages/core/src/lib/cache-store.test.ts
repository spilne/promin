import { describe, it, expect } from "bun:test";
import { Pipeline } from "./pipeline.ts";
import { MemoryCache, layered } from "./cache-store.ts";

// ---------------------------------------------------------------------------
// MemoryCache — in-memory key-value with TTL and LRU
// ---------------------------------------------------------------------------

describe("MemoryCache — fast in-process key-value store", () => {
  it("stores and retrieves a value by key", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000 });

    await cache.set("x", 42);
    expect(await cache.get("x")).toBe(42);
  });

  it("returns undefined on cache miss", async () => {
    const cache = new MemoryCache<string, string>({ ttlMs: 60_000 });
    expect(await cache.get("missing")).toBeUndefined();
  });

  it("expired entries return undefined", async () => {
    const cache = new MemoryCache<string, string>({ ttlMs: 10 });

    await cache.set("key", "value");
    expect(await cache.get("key")).toBe("value");

    await new Promise((r) => setTimeout(r, 20));
    expect(await cache.get("key")).toBeUndefined();
  });

  it("LRU eviction — oldest entry removed when full", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000, maxSize: 3 });

    await cache.set("a", 1);
    await cache.set("b", 2);
    await cache.set("c", 3);
    await cache.set("d", 4); // evicts "a"

    expect(await cache.get("a")).toBeUndefined();
    expect(await cache.get("b")).toBe(2);
    expect(await cache.get("d")).toBe(4);
  });

  it("accessing a key refreshes its LRU position", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000, maxSize: 3 });

    await cache.set("a", 1);
    await cache.set("b", 2);
    await cache.set("c", 3);

    await cache.get("a"); // refresh "a" — now "b" is oldest

    await cache.set("d", 4); // evicts "b" (oldest)

    expect(await cache.get("a")).toBe(1); // still there
    expect(await cache.get("b")).toBeUndefined(); // evicted
  });

  it("delete removes a specific key", async () => {
    const cache = new MemoryCache<string, string>({ ttlMs: 60_000 });

    await cache.set("key", "value");
    await cache.delete("key");
    expect(await cache.get("key")).toBeUndefined();
  });

  it("clear removes all entries", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000 });

    await cache.set("a", 1);
    await cache.set("b", 2);
    await cache.clear();

    expect(await cache.size()).toBe(0);
  });

  it("has checks existence without side effects", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000 });

    await cache.set("exists", 1);
    expect(await cache.has("exists")).toBe(true);
    expect(await cache.has("missing")).toBe(false);
  });

  it("custom TTL per entry overrides default", async () => {
    const cache = new MemoryCache<string, string>({ ttlMs: 60_000 });

    await cache.set("short", "value", 10); // 10ms TTL
    await cache.set("long", "value", 60_000);

    await new Promise((r) => setTimeout(r, 20));

    expect(await cache.get("short")).toBeUndefined();
    expect(await cache.get("long")).toBe("value");
  });
});

// ---------------------------------------------------------------------------
// LayeredCache — L1/L2/L3 cascading cache
// ---------------------------------------------------------------------------

describe("LayeredCache — read cascades down, write fills back up", () => {
  it("hit L1 — returns immediately without checking L2", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 });

    await l1.set("key", "from-L1");
    await l2.set("key", "from-L2");

    const cache = layered(l1, l2);
    expect(await cache.get("key")).toBe("from-L1");
  });

  it("miss L1, hit L2 — value written back to L1", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 });

    await l2.set("key", "from-L2");

    const cache = layered(l1, l2);
    expect(await cache.get("key")).toBe("from-L2");

    // L1 should now have the value (write-back)
    expect(await l1.get("key")).toBe("from-L2");
  });

  it("miss all layers — returns undefined", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 });

    const cache = layered(l1, l2);
    expect(await cache.get("missing")).toBeUndefined();
  });

  it("set writes to ALL layers", async () => {
    const l1 = new MemoryCache<string, number>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, number>({ ttlMs: 60_000 });
    const l3 = new MemoryCache<string, number>({ ttlMs: 60_000 });

    const cache = layered(l1, l2, l3);
    await cache.set("key", 42);

    expect(await l1.get("key")).toBe(42);
    expect(await l2.get("key")).toBe(42);
    expect(await l3.get("key")).toBe(42);
  });

  it("delete removes from ALL layers", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 });

    const cache = layered(l1, l2);
    await cache.set("key", "value");
    await cache.delete("key");

    expect(await l1.get("key")).toBeUndefined();
    expect(await l2.get("key")).toBeUndefined();
  });

  it("getOrCompute — miss all, compute, write to all layers", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 });
    let computeCalls = 0;

    const cache = layered(l1, l2);
    const value = await cache.getOrCompute("key", async () => {
      computeCalls++;
      return "computed";
    });

    expect(value).toBe("computed");
    expect(computeCalls).toBe(1);

    // Both layers now have the value
    expect(await l1.get("key")).toBe("computed");
    expect(await l2.get("key")).toBe("computed");

    // Second call — no compute, served from L1
    const again = await cache.getOrCompute("key", async () => {
      computeCalls++;
      return "should-not-reach";
    });

    expect(again).toBe("computed");
    expect(computeCalls).toBe(1);
  });

  it("L1 expires but L2 still fresh — refills L1 from L2", async () => {
    const l1 = new MemoryCache<string, string>({ ttlMs: 10 }); // short TTL
    const l2 = new MemoryCache<string, string>({ ttlMs: 60_000 }); // long TTL

    const cache = layered(l1, l2);
    await cache.set("key", "value");

    // Wait for L1 to expire
    await new Promise((r) => setTimeout(r, 20));

    expect(await l1.get("key")).toBeUndefined(); // expired in L1
    expect(await cache.get("key")).toBe("value"); // hit L2, backfill L1
    expect(await l1.get("key")).toBe("value"); // L1 refilled
  });

  it("three layers — L1 miss, L2 miss, L3 hit — backfills L1 and L2", async () => {
    const l1 = new MemoryCache<string, number>({ ttlMs: 60_000 });
    const l2 = new MemoryCache<string, number>({ ttlMs: 60_000 });
    const l3 = new MemoryCache<string, number>({ ttlMs: 60_000 });

    await l3.set("deep", 99);

    const cache = layered(l1, l2, l3);
    expect(await cache.get("deep")).toBe(99);

    // Backfilled to L1 and L2
    expect(await l1.get("deep")).toBe(99);
    expect(await l2.get("deep")).toBe(99);
  });
});

// ---------------------------------------------------------------------------
// Pipeline.cachedBy — keyed caching on pipelines
// ---------------------------------------------------------------------------

describe("Pipeline.cachedBy — cache pipeline results by key", () => {
  it("first call computes, second call returns cached", async () => {
    const cache = new MemoryCache<string, number>({ ttlMs: 60_000 });
    let computeCalls = 0;

    const compute = (id: string) =>
      Pipeline.fromPromise(async () => {
        computeCalls++;
        return Number(id) * 10;
      }).cachedBy(cache, id);

    expect(await compute("5").runPromise()).toBe(50);
    expect(await compute("5").runPromise()).toBe(50); // cached
    expect(computeCalls).toBe(1);
  });

  it("different keys compute independently", async () => {
    const cache = new MemoryCache<string, string>({ ttlMs: 60_000 });
    let calls = 0;

    const getUser = (id: string) =>
      Pipeline.fromPromise(async () => {
        calls++;
        return `user-${id}`;
      }).cachedBy(cache, id);

    expect(await getUser("1").runPromise()).toBe("user-1");
    expect(await getUser("2").runPromise()).toBe("user-2");
    expect(await getUser("1").runPromise()).toBe("user-1"); // cached
    expect(calls).toBe(2); // only 2 computes, not 3
  });

  it("works with layered cache", async () => {
    const l1 = new MemoryCache<string, number>({ ttlMs: 10 });
    const l2 = new MemoryCache<string, number>({ ttlMs: 60_000 });
    const cache = layered(l1, l2);
    let calls = 0;

    const compute = (key: string) =>
      Pipeline.fromPromise(async () => {
        calls++;
        return 42;
      }).cachedBy(cache, key);

    await compute("x").runPromise(); // computes, writes to L1+L2
    expect(calls).toBe(1);

    await new Promise((r) => setTimeout(r, 20)); // L1 expires

    await compute("x").runPromise(); // L1 miss, L2 hit
    expect(calls).toBe(1); // no recompute
  });
});
