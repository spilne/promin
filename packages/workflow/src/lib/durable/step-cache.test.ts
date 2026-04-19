// ---------------------------------------------------------------------------
// Step-level result cache tests — .step({ cache }) skip-if-recent semantics.
//
// Covers:
//   * HIT path: cached value returned, body not run, step still recorded as
//     completed
//   * MISS path: body runs, result cached for subsequent runs
//   * TTL: re-running after expiry re-executes the body
//   * Failure: failed bodies do NOT poison the cache
//   * Cross-workflowId sharing: two runs with the same key share the hit
//   * Cache backend errors: storage hiccups fall through to a miss
//   * Namespace: different workflow names keyed on same input don't collide
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { MemoryCache, type CacheStore } from "@promin/core";
import { workflow } from "./durable-pipeline.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { createWorkflowRunner } from "./workflow-runner.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function counter() {
  const box = { value: 0 };
  return {
    box,
    bump() {
      box.value++;
    },
  };
}

/** A cache whose get()/set() can be made to throw on demand, to prove cache
 *  failures never leak into the workflow. */
class FaultyCache implements CacheStore<string, unknown> {
  readonly inner = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
  readonly failures: { get?: Error; set?: Error } = {};

  async get(key: string): Promise<unknown | undefined> {
    if (this.failures.get) throw this.failures.get;
    return this.inner.get(key);
  }
  async set(key: string, value: unknown, ttlMs?: number): Promise<void> {
    if (this.failures.set) throw this.failures.set;
    return this.inner.set(key, value, ttlMs);
  }
  async delete(key: string): Promise<void> {
    return this.inner.delete(key);
  }
  async has(key: string): Promise<boolean> {
    return this.inner.has(key);
  }
  async clear(): Promise<void> {
    return this.inner.clear();
  }
  async size(): Promise<number> {
    return this.inner.size();
  }
}

// ---------------------------------------------------------------------------
// Hit / miss / TTL
// ---------------------------------------------------------------------------

describe("step cache — hit / miss / TTL", () => {
  it("second run within TTL returns cached value without invoking the body", async () => {
    const cache = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "hit" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return ["v1", "v2", "v3"];
        },
        {
          cache: {
            key: (ctx) => `discover:${(ctx.input as any).accountId}`,
            ttlMs: 60_000,
            store: cache,
          },
        },
      )
      .build();

    const r1 = await runner.run({
      workflow: wf,
      workflowId: "run-1",
      input: { accountId: "alice" },
    });
    const r2 = await runner.run({
      workflow: wf,
      workflowId: "run-2",
      input: { accountId: "alice" },
    });

    expect(r1).toEqual(["v1", "v2", "v3"]);
    expect(r2).toEqual(["v1", "v2", "v3"]);
    expect(c.box.value).toBe(1); // body ran exactly once
  });

  it("different workflowIds with the same cache key share the entry", async () => {
    const cache = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "share" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return 42;
        },
        {
          cache: {
            key: () => "k",
            ttlMs: 60_000,
            store: cache,
          },
        },
      )
      .build();

    await runner.run({ workflow: wf, workflowId: "alpha", input: {} });
    await runner.run({ workflow: wf, workflowId: "beta", input: {} });
    expect(c.box.value).toBe(1);
  });

  it("expired entry triggers re-execution", async () => {
    const cache = new MemoryCache<string, unknown>({ ttlMs: 1 });
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "ttl" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return c.box.value;
        },
        {
          cache: {
            key: () => "k",
            ttlMs: 1, // 1ms — effectively "immediately stale"
            store: cache,
          },
        },
      )
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "w1", input: {} });
    await new Promise((r) => setTimeout(r, 20));
    const r2 = await runner.run({ workflow: wf, workflowId: "w2", input: {} });

    expect(r1).toBe(1);
    expect(r2).toBe(2); // fresh run after expiry
    expect(c.box.value).toBe(2);
  });

  it("different cache keys do NOT collide", async () => {
    const cache = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "keys" })
      .stepAsync(
        "discover",
        async ({ input }: any) => {
          c.bump();
          return input.tenant;
        },
        {
          cache: {
            key: (ctx) => `t:${(ctx.input as any).tenant}`,
            ttlMs: 60_000,
            store: cache,
          },
        },
      )
      .build();

    await runner.run({ workflow: wf, workflowId: "w-a", input: { tenant: "acme" } });
    await runner.run({ workflow: wf, workflowId: "w-b", input: { tenant: "beta" } });
    expect(c.box.value).toBe(2); // each tenant key ran its own body
  });
});

// ---------------------------------------------------------------------------
// Failure semantics — failed bodies do NOT poison the cache
// ---------------------------------------------------------------------------

describe("step cache — failure never caches", () => {
  it("throwing body leaves the cache empty; next run still misses and re-runs", async () => {
    const cache = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    let attempts = 0;

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "fail" })
      .stepAsync(
        "discover",
        async () => {
          attempts++;
          if (attempts === 1) throw new Error("first call flaky");
          return "ok";
        },
        {
          cache: { key: () => "k", ttlMs: 60_000, store: cache },
        },
      )
      .build();

    await expect(runner.run({ workflow: wf, workflowId: "r1", input: {} })).rejects.toBeDefined();
    expect(await cache.size()).toBe(0); // no poison

    const r2 = await runner.run({ workflow: wf, workflowId: "r2", input: {} });
    expect(r2).toBe("ok");
    expect(attempts).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Cache backend errors never fail the workflow
// ---------------------------------------------------------------------------

describe("step cache — backend errors are swallowed", () => {
  it("get() failure falls through to a miss", async () => {
    const cache = new FaultyCache();
    cache.failures.get = new Error("redis down on read");
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "get-fail" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return "value";
        },
        {
          cache: { key: () => "k", ttlMs: 60_000, store: cache },
        },
      )
      .build();

    const r = await runner.run({ workflow: wf, workflowId: "w", input: {} });
    expect(r).toBe("value");
    expect(c.box.value).toBe(1);
  });

  it("set() failure completes the workflow anyway; next run re-executes", async () => {
    const cache = new FaultyCache();
    cache.failures.set = new Error("redis down on write");
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf = workflow({ name: "set-fail" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return c.box.value;
        },
        {
          cache: { key: () => "k", ttlMs: 60_000, store: cache },
        },
      )
      .build();

    const r1 = await runner.run({ workflow: wf, workflowId: "w1", input: {} });
    expect(r1).toBe(1);
    // set failed so nothing was cached; next run runs body again.
    const r2 = await runner.run({ workflow: wf, workflowId: "w2", input: {} });
    expect(r2).toBe(2);
    expect(c.box.value).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Namespace — default is the workflow name
// ---------------------------------------------------------------------------

describe("step cache — namespace keeps different workflows isolated", () => {
  it("default namespace prefixes the workflow name", async () => {
    const shared = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const c1 = counter();
    const c2 = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf1 = workflow({ name: "wfA" })
      .stepAsync(
        "discover",
        async () => {
          c1.bump();
          return "A";
        },
        { cache: { key: () => "k", ttlMs: 60_000, store: shared } },
      )
      .build();

    const wf2 = workflow({ name: "wfB" })
      .stepAsync(
        "discover",
        async () => {
          c2.bump();
          return "B";
        },
        { cache: { key: () => "k", ttlMs: 60_000, store: shared } },
      )
      .build();

    await runner.run({ workflow: wf1, workflowId: "a1", input: {} });
    await runner.run({ workflow: wf2, workflowId: "b1", input: {} });

    expect(c1.box.value).toBe(1);
    expect(c2.box.value).toBe(1); // different namespace → both bodies ran
  });

  it("explicit namespace overrides the workflow-name default", async () => {
    const shared = new MemoryCache<string, unknown>({ ttlMs: 60_000 });
    const c = counter();

    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });

    const wf1 = workflow({ name: "sharedA" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return "ok";
        },
        {
          cache: {
            key: () => "k",
            ttlMs: 60_000,
            store: shared,
            namespace: "user-facing",
          },
        },
      )
      .build();

    const wf2 = workflow({ name: "sharedB" })
      .stepAsync(
        "discover",
        async () => {
          c.bump();
          return "ok";
        },
        {
          cache: {
            key: () => "k",
            ttlMs: 60_000,
            store: shared,
            namespace: "user-facing", // same namespace as wf1
          },
        },
      )
      .build();

    await runner.run({ workflow: wf1, workflowId: "1", input: {} });
    await runner.run({ workflow: wf2, workflowId: "2", input: {} });

    expect(c.box.value).toBe(1); // shared namespace → second run hits the cache
  });
});
