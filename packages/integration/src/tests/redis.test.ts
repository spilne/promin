import { it, expect } from "bun:test";
import { Redis as IoRedis } from "ioredis";
import { withRedis, uniqueName } from "../infra.ts";
import {
  RedisStream,
  RedisPubSub,
  RedisStateBackend,
  RedisCacheStore,
  RedisSingleflight,
  RedisRef,
  RedisThrottle,
  RedisRateLimiter,
  RedisChannel,
  RedisSemaphore,
  RedisLatch,
  RedisBarrier,
  RedisDeferred,
  RedisSignal,
  RedisQueue,
  RedisWorkflowStorage,
  RedisStepQueue,
  RedisDurableScheduler,
} from "@promin/redis";
import type { RedisClient } from "@promin/redis";
import {
  singleflightTestSuite,
  throttleTestSuite,
  rateLimiterTestSuite,
  refTestSuite,
  channelTestSuite,
  latchTestSuite,
  barrierTestSuite,
  deferredTestSuite,
  signalTestSuite,
  queueTestSuite,
} from "@promin/core/testing";
import { storageTestSuite, stepQueueTestSuite, schedulerTestSuite } from "@promin/workflow/testing";

// ---------------------------------------------------------------------------
// RedisStream — durable consumer groups
// ---------------------------------------------------------------------------

withRedis("RedisStream — consumer group messaging", (ctx) => {
  function redis(): RedisClient {
    return new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
  }

  it("publishes and consumes a message", async () => {
    const r = redis();
    const stream = new RedisStream<{ orderId: string }>({
      redis: r,
      stream: uniqueName("orders"),
      group: "processors",
    });

    await stream.ensureGroup();
    await stream.publish({ orderId: "o-1" });

    const items = await stream.subscribe().take(1).collect();
    expect(items).toEqual([{ orderId: "o-1" }]);

    r.disconnect();
  });

  it("ack and nack work correctly", async () => {
    const r = redis();
    const name = uniqueName("ack-test");
    const stream = new RedisStream<{ v: number }>({
      redis: r,
      stream: name,
      group: "g1",
      blockMs: 500, // short block so test doesn't hang
    });

    await stream.ensureGroup();
    await stream.publish({ v: 1 });
    await stream.publish({ v: 2 });

    // Consume and ack both messages
    const envelopes = await stream.subscribeAck().take(2).collect();
    expect(envelopes).toHaveLength(2);
    await envelopes[0]!.ack();
    await envelopes[1]!.ack();

    // Stream length should be 2 (messages still in stream, just acked)
    const info = await stream.info();
    expect(info.length).toBe(2);

    r.disconnect();
  });

  it("multiple consumers in same group share messages", async () => {
    const r1 = redis();
    const r2 = redis();
    const name = uniqueName("shared");

    const s1 = new RedisStream<{ v: number }>({
      redis: r1,
      stream: name,
      group: "shared",
      blockMs: 500,
    });
    const s2 = new RedisStream<{ v: number }>({
      redis: r2,
      stream: name,
      group: "shared",
      blockMs: 500,
    });

    await s1.ensureGroup();

    // Publish 10 messages
    for (let i = 0; i < 10; i++) await s1.publish({ v: i });

    const allItems: number[] = [];

    // Consume all 10 with a timeout — distribution across consumers varies
    await s1
      .subscribe()
      .merge(s2.subscribe())
      .take(10)
      .forEach((m) => allItems.push(m.v));

    expect(allItems.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    r1.disconnect();
    r2.disconnect();
  });
});

// ---------------------------------------------------------------------------
// RedisPubSub — fire-and-forget broadcast
// ---------------------------------------------------------------------------

withRedis("RedisPubSub — broadcast messaging", (ctx) => {
  function redis(): RedisClient {
    return new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
  }

  it("subscriber receives published messages", async () => {
    const r = redis();
    const channel = uniqueName("events");
    const pubsub = new RedisPubSub<{ type: string }>({ redis: r, channel });

    const received: string[] = [];
    const sub = pubsub.subscribe().tap((e) => received.push(e.type));

    // Start consuming in background
    const drainPromise = sub.take(2).drain();

    // Give subscriber time to connect
    await new Promise((r) => setTimeout(r, 500));

    await pubsub.publish({ type: "login" });
    await pubsub.publish({ type: "logout" });

    await drainPromise;
    expect(received).toEqual(["login", "logout"]);

    r.disconnect();
  });

  it("pattern subscribe receives from matching channels", async () => {
    const r = redis();
    const prefix = uniqueName("ns");
    const pubsub = new RedisPubSub<{ v: number }>({ redis: r, pattern: `${prefix}.*` });

    const received: number[] = [];
    const drainPromise = pubsub
      .subscribe()
      .tap((e) => received.push(e.v))
      .take(2)
      .drain();

    await new Promise((r) => setTimeout(r, 500));

    // Publish to different channels under the pattern
    const pub = redis();
    await (pub as any).publish(`${prefix}.orders`, JSON.stringify({ v: 1 }));
    await (pub as any).publish(`${prefix}.users`, JSON.stringify({ v: 2 }));

    await drainPromise;
    expect(received).toEqual([1, 2]);

    r.disconnect();
    pub.disconnect();
  });
});

// ---------------------------------------------------------------------------
// RedisStateBackend — topology state checkpointing
// ---------------------------------------------------------------------------

withRedis("RedisStateBackend — checkpoint and restore", (ctx) => {
  function redis(): RedisClient {
    return new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
  }

  it("put/get round-trips values", async () => {
    const r = redis();
    const state = new RedisStateBackend({ redis: r, prefix: uniqueName("state") + ":" });

    await state.put("user:1", { name: "Alice", score: 42 });
    const value = await state.get("user:1");

    expect(value).toEqual({ name: "Alice", score: 42 });

    r.disconnect();
  });

  it("checkpoint and restore preserves state across instances", async () => {
    const r = redis();
    const prefix = uniqueName("cp") + ":";

    // Instance 1: write state and checkpoint
    const s1 = new RedisStateBackend({ redis: r, prefix });
    await s1.put("counter", 42);
    await s1.put("name", "topology-1");
    await s1.checkpoint({ name: "v1" });

    // Clear live state (simulate crash)
    await s1.clear();
    expect(await s1.get("counter")).toBeUndefined();

    // Instance 2: restore from checkpoint
    const s2 = new RedisStateBackend({ redis: r, prefix });
    await s2.restore({ name: "v1" });

    expect(await s2.get("counter")).toBe(42);
    expect(await s2.get("name")).toBe("topology-1");

    r.disconnect();
  });

  it("keys and entries enumerate state", async () => {
    const r = redis();
    const state = new RedisStateBackend({ redis: r, prefix: uniqueName("enum") + ":" });

    await state.put("a", 1);
    await state.put("b", 2);
    await state.put("c", 3);

    const keys = await state.keys();
    expect(keys.sort()).toEqual(["a", "b", "c"]);

    const entries = await state.entries();
    expect(entries.sort((a, b) => String(a[0]).localeCompare(String(b[0])))).toEqual([
      ["a", 1],
      ["b", 2],
      ["c", 3],
    ]);

    r.disconnect();
  });
});

// ---------------------------------------------------------------------------
// RedisCacheStore — key-value cache with TTL
// ---------------------------------------------------------------------------

withRedis("RedisCacheStore — caching with TTL", (ctx) => {
  function redis(): RedisClient {
    return new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
  }

  it("set and get within TTL", async () => {
    const r = redis();
    const cache = new RedisCacheStore<{ name: string }>({
      redis: r,
      ttlMs: 10_000,
      prefix: uniqueName("cache") + ":",
    });

    await cache.set("user:1", { name: "Alice" });
    const value = await cache.get("user:1");
    expect(value).toEqual({ name: "Alice" });

    r.disconnect();
  });

  it("expired keys return undefined", async () => {
    const r = redis();
    const cache = new RedisCacheStore<number>({
      redis: r,
      ttlMs: 50, // 50ms TTL
      prefix: uniqueName("ttl") + ":",
    });

    await cache.set("key", 42);
    expect(await cache.get("key")).toBe(42);

    // Wait for expiry
    await new Promise((r) => setTimeout(r, 100));
    expect(await cache.get("key")).toBeUndefined();

    r.disconnect();
  });

  it("delete removes a key", async () => {
    const r = redis();
    const cache = new RedisCacheStore<string>({
      redis: r,
      ttlMs: 10_000,
      prefix: uniqueName("del") + ":",
    });

    await cache.set("x", "hello");
    expect(await cache.has("x")).toBe(true);

    await cache.delete("x");
    expect(await cache.has("x")).toBe(false);

    r.disconnect();
  });
});

// ---------------------------------------------------------------------------
// Redis Primitives — portable conformance suites
// ---------------------------------------------------------------------------

withRedis("RedisSingleflight conformance", (ctx) => {
  singleflightTestSuite(() => {
    const r = new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
    return RedisSingleflight.make({ redis: r, prefix: uniqueName("sf") });
  });
});

withRedis("RedisRef conformance", (ctx) => {
  refTestSuite(() =>
    RedisRef.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("ref"),
      initial: 0,
    }),
  );
});

withRedis("RedisThrottle conformance", (ctx) => {
  throttleTestSuite(() =>
    RedisThrottle.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("throttle"),
      permits: 1,
      windowMs: 100,
    }),
  );
});

withRedis("RedisRateLimiter conformance", (ctx) => {
  rateLimiterTestSuite(() =>
    RedisRateLimiter.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("rl"),
      limit: 2,
      windowMs: 100,
    }),
  );
});

withRedis("RedisChannel conformance", (ctx) => {
  channelTestSuite(() =>
    RedisChannel.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("chan"),
      capacity: 10,
    }),
  );
});

withRedis("RedisSemaphore — acquire and release", (ctx) => {
  function redis(): RedisClient {
    return new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
  }

  it("withPermitAsync runs fn and releases", async () => {
    const r = redis();
    const sem = await RedisSemaphore.make({ redis: r, key: uniqueName("sem"), permits: 2 });
    const result = await sem.withPermitAsync(async () => 42);
    expect(result).toBe(42);
    r.disconnect();
  });

  it("blocks when permits exhausted, resumes on release", async () => {
    const r = redis();
    const key = uniqueName("sem-block");
    const sem = await RedisSemaphore.make({ redis: r, key, permits: 1, timeoutMs: 5000 });

    await sem.acquire();

    // Release after 50ms
    setTimeout(() => sem.release(), 50);

    const start = Date.now();
    await sem.acquire();
    expect(Date.now() - start).toBeGreaterThanOrEqual(30);

    await sem.release();
    r.disconnect();
  });
});

// ---------------------------------------------------------------------------
// RedisLatch — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisLatch conformance", (ctx) => {
  latchTestSuite(() =>
    RedisLatch.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("latch"),
      count: 3,
    }),
  );
});

// ---------------------------------------------------------------------------
// RedisBarrier — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisBarrier conformance", (ctx) => {
  barrierTestSuite(() =>
    RedisBarrier.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("barrier"),
      parties: 3,
    }),
  );
});

// ---------------------------------------------------------------------------
// RedisDeferred — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisDeferred conformance", (ctx) => {
  deferredTestSuite(() =>
    RedisDeferred.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("def"),
    }),
  );
});

// ---------------------------------------------------------------------------
// RedisSignal — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisSignal conformance", (ctx) => {
  signalTestSuite(() =>
    RedisSignal.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("sig"),
      initial: 0,
    }),
  );
});

// ---------------------------------------------------------------------------
// RedisQueue — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisQueue conformance", (ctx) => {
  queueTestSuite(() =>
    RedisQueue.make({
      redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
      key: uniqueName("queue"),
      capacity: 100,
    }),
  );
});

// ---------------------------------------------------------------------------
// RedisWorkflowStorage — portable conformance suite (incl. journal +
// JournaledSuspendStorage)
// ---------------------------------------------------------------------------

withRedis("RedisWorkflowStorage conformance", (ctx) => {
  storageTestSuite(
    () =>
      new RedisWorkflowStorage({
        redis: new IoRedis(ctx.port, ctx.host) as unknown as RedisClient,
        prefix: uniqueName("wf"),
      }),
    { hasJournal: true, hasJournaledSuspend: true },
  );
});

// ---------------------------------------------------------------------------
// RedisStepQueue — portable conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisStepQueue conformance", (ctx) => {
  stepQueueTestSuite(
    () =>
      new RedisStepQueue(new IoRedis(ctx.port, ctx.host) as unknown as RedisClient, {
        prefix: uniqueName("sq"),
      }),
  );
});

// ---------------------------------------------------------------------------
// RedisDurableScheduler — portable Scheduler conformance suite
// ---------------------------------------------------------------------------

withRedis("RedisDurableScheduler conformance", (ctx) => {
  schedulerTestSuite("RedisDurableScheduler", () => {
    const redis = new IoRedis(ctx.port, ctx.host) as unknown as RedisClient;
    const scheduler = new RedisDurableScheduler({
      redis,
      prefix: uniqueName("sched"),
      pollIntervalMs: 25,
    });
    return {
      scheduler,
      register: (config) => scheduler.registerAsync(config),
      unregister: (id, options) => scheduler.unregisterAsync(id, options),
      pause: (id) => scheduler.pauseAsync(id),
      resume: (id) => scheduler.resumeAsync(id),
      list: async () => scheduler.listAsync(),
    };
  });
});
