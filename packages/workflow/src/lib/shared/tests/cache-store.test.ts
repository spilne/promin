import { describe, it, expect } from "bun:test";
import { MemoryCache } from "../cache-store.ts";

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
