import { describe, it, expect } from "bun:test";
import { Data, Effect } from "effect";
import {
  Pipeline,
  PipelineSemaphore,
  CircuitBreaker,
  PipelineCache,
  PipelineQueue,
  PipelineRef,
  PipelineDeferred,
  PipelineChannel,
  PipelineSignal,
  PipelinePubSub,
  PipelinePool,
} from "../../index.ts";
import { refTestSuite } from "../ref-test-suite.ts";
import { deferredTestSuite } from "../deferred-test-suite.ts";
import { channelTestSuite } from "../channel-test-suite.ts";
import { signalTestSuite } from "../signal-test-suite.ts";
import { pubsubTestSuite } from "../pubsub-test-suite.ts";
import { queueTestSuite } from "../queue-test-suite.ts";
import { poolTestSuite } from "../pool-test-suite.ts";

// ---------------------------------------------------------------------------
// Portable conformance suites (Promise API)
// ---------------------------------------------------------------------------

refTestSuite(() => PipelineRef.make(0));
deferredTestSuite(() => PipelineDeferred.make<number>());
channelTestSuite(() => PipelineChannel.make<number>(10));
signalTestSuite(() => PipelineSignal.make(0));
pubsubTestSuite(() => PipelinePubSub.make<number>(10));
queueTestSuite(() => PipelineQueue.make<number>(10));
poolTestSuite(() =>
  PipelinePool.make({
    acquire: () => ({ id: 1 }),
    release: () => {},
    size: 2,
  }),
);

// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// Semaphore
// ---------------------------------------------------------------------------

describe("PipelineSemaphore", () => {
  it("limits concurrency", async () => {
    const sem = PipelineSemaphore.make(2);
    let active = 0;
    let maxActive = 0;

    await Pipeline.forEach(
      [1, 2, 3, 4, 5],
      (n) =>
        Pipeline.fromPromise(async () => {
          active++;
          maxActive = Math.max(maxActive, active);
          await new Promise((r) => setTimeout(r, 20));
          active--;
          return n;
        }).withPermit(sem),
      { concurrency: 5 },
    ).runPromise();

    expect(maxActive).toBeLessThanOrEqual(2);
  });
});

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

describe("CircuitBreaker", () => {
  it("stays closed on success", async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3, resetTimeoutMs: 1000 });
    await Pipeline.succeed("ok").withCircuitBreaker(breaker).runPromise();
    expect(breaker.currentState).toBe("closed");
  });

  it("opens after threshold failures", async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 2, resetTimeoutMs: 5000 });
    const failPipeline = Pipeline.from(
      Effect.fail(new TestError({ message: "fail" })) as Effect.Effect<string, TestError>,
    );

    await failPipeline.withCircuitBreaker(breaker).runSafe();
    expect(breaker.currentState).toBe("closed");

    await failPipeline.withCircuitBreaker(breaker).runSafe();
    expect(breaker.currentState).toBe("open");

    // Now it fails fast
    const { error } = await Pipeline.succeed("ok").withCircuitBreaker(breaker).runSafe();
    expect(error?._tag).toBe("CircuitOpenError");
  });

  it("transitions to half-open after reset timeout", async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 50 });

    await Pipeline.from(
      Effect.fail(new TestError({ message: "fail" })) as Effect.Effect<string, TestError>,
    )
      .withCircuitBreaker(breaker)
      .runSafe();

    expect(breaker.currentState).toBe("open");

    await new Promise((r) => setTimeout(r, 60));
    expect(breaker.currentState).toBe("half-open");

    // Success in half-open closes the circuit
    await Pipeline.succeed("ok").withCircuitBreaker(breaker).runPromise();
    expect(breaker.currentState).toBe("closed");
  });

  it("respects isFailure filter", async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeoutMs: 5000,
      isFailure: (err) => err._tag === "TestError" && err.message === "critical",
    });

    await Pipeline.from(
      Effect.fail(new TestError({ message: "minor" })) as Effect.Effect<string, TestError>,
    )
      .withCircuitBreaker(breaker)
      .runSafe();
    expect(breaker.currentState).toBe("closed"); // non-critical error ignored

    await Pipeline.from(
      Effect.fail(new TestError({ message: "critical" })) as Effect.Effect<string, TestError>,
    )
      .withCircuitBreaker(breaker)
      .runSafe();
    expect(breaker.currentState).toBe("open");
  });

  it("reset() restores to closed", () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 5000 });
    // Force open by directly using protect
    Effect.runSync(Effect.either(breaker.protect(Effect.fail(new TestError({ message: "fail" })))));
    expect(breaker.currentState).toBe("open");
    breaker.reset();
    expect(breaker.currentState).toBe("closed");
  });
});

// ---------------------------------------------------------------------------
// PipelineCache
// ---------------------------------------------------------------------------

describe("PipelineCache", () => {
  it("caches the first result", async () => {
    let calls = 0;
    const cache = new PipelineCache<number>(60_000);

    const pipeline = Pipeline.fromPromise(async () => {
      calls++;
      return 42;
    }).cached(cache);

    await pipeline.runPromise();
    await pipeline.runPromise();
    await pipeline.runPromise();

    expect(calls).toBe(1);
  });

  it("re-executes after TTL expires", async () => {
    let calls = 0;
    const cache = new PipelineCache<number>(30); // 30ms TTL

    const pipeline = Pipeline.fromPromise(async () => {
      calls++;
      return calls;
    }).cached(cache);

    const first = await pipeline.runPromise();
    expect(first).toBe(1);

    await new Promise((r) => setTimeout(r, 50));

    const second = await pipeline.runPromise();
    expect(second).toBe(2);
  });

  it("invalidate forces refresh", async () => {
    let calls = 0;
    const cache = new PipelineCache<number>(60_000);

    const pipeline = Pipeline.fromPromise(async () => {
      calls++;
      return calls;
    }).cached(cache);

    await pipeline.runPromise();
    expect(calls).toBe(1);

    cache.invalidate();
    await pipeline.runPromise();
    expect(calls).toBe(2);
  });

  it("isFresh reflects cache state", async () => {
    const cache = new PipelineCache<number>(60_000);
    expect(cache.isFresh).toBe(false);

    await Pipeline.succeed(1).cached(cache).runPromise();
    expect(cache.isFresh).toBe(true);

    cache.invalidate();
    expect(cache.isFresh).toBe(false);
  });

  it("returns the cached value, not a stale one", async () => {
    let counter = 0;
    const cache = new PipelineCache<string>(60_000);

    const pipeline = Pipeline.fromPromise(async () => {
      counter++;
      return `result-${counter}`;
    }).cached(cache);

    const first = await pipeline.runPromise();
    const second = await pipeline.runPromise();
    expect(first).toBe("result-1");
    expect(second).toBe("result-1"); // same cached value
    expect(counter).toBe(1);

    cache.invalidate();
    const third = await pipeline.runPromise();
    expect(third).toBe("result-2"); // fresh value after invalidation
  });

  it("does not cache errors", async () => {
    let calls = 0;
    const cache = new PipelineCache<string>(60_000);

    const failThenSucceed = Pipeline.from(
      Effect.suspend(() => {
        calls++;
        return calls === 1
          ? Effect.fail(new TestError({ message: "first call fails" }))
          : Effect.succeed("ok");
      }),
    ).cached(cache);

    const { error } = await failThenSucceed.runSafe();
    expect(error?._tag).toBe("TestError");
    expect(cache.isFresh).toBe(false); // error was not cached

    const { data } = await failThenSucceed.runSafe();
    expect(data).toBe("ok"); // second call succeeds and caches
    expect(cache.isFresh).toBe(true);
  });

  it("shared cache across different pipelines", async () => {
    let calls = 0;
    const cache = new PipelineCache<number>(60_000);

    const pipelineA = Pipeline.fromPromise(async () => {
      calls++;
      return 42;
    }).cached(cache);

    const pipelineB = Pipeline.fromPromise(async () => {
      calls++;
      return 99;
    }).cached(cache);

    const a = await pipelineA.runPromise();
    const b = await pipelineB.runPromise(); // should return cached value from pipelineA
    expect(a).toBe(42);
    expect(b).toBe(42); // same cache
    expect(calls).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// PipelineQueue
// ---------------------------------------------------------------------------

describe("PipelineQueue", () => {
  it("producer/consumer via take", async () => {
    const queue = PipelineQueue.make<number>(10);

    await queue.offerAsync(1);
    await queue.offerAsync(2);
    await queue.offerAsync(3);

    const a = await queue.takeAsync();
    const b = await queue.takeAsync();
    const c = await queue.takeAsync();
    expect([a, b, c]).toEqual([1, 2, 3]);
  });

  it("offerAll adds multiple items", async () => {
    const queue = PipelineQueue.make<number>(10);
    await queue.offerAllAsync([10, 20, 30]);

    const a = await queue.takeAsync();
    const b = await queue.takeAsync();
    const c = await queue.takeAsync();
    expect([a, b, c]).toEqual([10, 20, 30]);
  });

  it("take blocks until item available", async () => {
    const queue = PipelineQueue.make<string>(10);

    setTimeout(() => queue.offerAsync("hello"), 20);

    const value = await queue.takeAsync();
    expect(value).toBe("hello");
  });

  it("stream consumes items until shutdown", async () => {
    const queue = PipelineQueue.make<number>(10);

    setTimeout(async () => {
      await queue.offerAsync(1);
      await queue.offerAsync(2);
      await queue.shutdownAsync();
    }, 10);

    const items = await queue.toStream().collect();
    expect(items).toEqual([1, 2]);
  });
});

// ---------------------------------------------------------------------------
// PipelineRef
// ---------------------------------------------------------------------------

describe("PipelineRef", () => {
  it("get/set/update", async () => {
    const ref = PipelineRef.make(0);

    expect(await ref.getAsync()).toBe(0);

    await ref.setAsync(10);
    expect(await ref.getAsync()).toBe(10);

    await ref.updateAsync((n) => n + 5);
    expect(await ref.getAsync()).toBe(15);
  });

  it("updateAndGet returns new value", async () => {
    const ref = PipelineRef.make(10);
    const result = await ref.updateAndGetAsync((n) => n * 2);
    expect(result).toBe(20);
  });

  it("modify atomically updates and returns derived value", async () => {
    const ref = PipelineRef.make(10);
    const derived = await Effect.runPromise(ref.modify((n) => [`was-${n}`, n + 1] as const));
    expect(derived).toBe("was-10");
    expect(await ref.getAsync()).toBe(11);
  });
});

// ---------------------------------------------------------------------------
// PipelineDeferred
// ---------------------------------------------------------------------------

describe("PipelineDeferred", () => {
  it("await resolves when succeeded", async () => {
    const deferred = PipelineDeferred.make<number>();

    setTimeout(() => deferred.succeedAsync(42), 20);

    const value = await deferred.awaitAsync();
    expect(value).toBe(42);
  });

  it("isDone reflects state", async () => {
    const deferred = PipelineDeferred.make<string>();

    expect(await deferred.isDoneAsync()).toBe(false);
    await deferred.succeedAsync("done");
    expect(await deferred.isDoneAsync()).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// PipelineChannel
// ---------------------------------------------------------------------------

describe("PipelineChannel", () => {
  it("sends and receives items, ends on close", async () => {
    const ch = PipelineChannel.make<number>(10);

    setTimeout(async () => {
      await ch.sendAsync(1);
      await ch.sendAsync(2);
      await ch.sendAsync(3);
      await ch.closeAsync();
    }, 10);

    const items = await ch.toStream().collect();
    expect(items).toEqual([1, 2, 3]);
  });

  it("isClosed reflects state", async () => {
    const ch = PipelineChannel.make<number>(10);
    expect(ch.isClosed).toBe(false);
    await ch.closeAsync();
    expect(ch.isClosed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// PipelineSignal
// ---------------------------------------------------------------------------

describe("PipelineSignal", () => {
  it("get returns current value", async () => {
    const signal = PipelineSignal.make(42);
    expect(await signal.getAsync()).toBe(42);
  });

  it("set updates the value", async () => {
    const signal = PipelineSignal.make(0);
    await signal.setAsync(99);
    expect(await signal.getAsync()).toBe(99);
  });

  it("update modifies the value", async () => {
    const signal = PipelineSignal.make(10);
    await signal.updateAsync((n) => n + 5);
    expect(await signal.getAsync()).toBe(15);
  });

  it("changes emits initial value and updates", async () => {
    const signal = PipelineSignal.make("a");

    setTimeout(() => signal.setAsync("b"), 20);
    setTimeout(() => signal.setAsync("c"), 40);

    const values = await signal.changes().take(3).collect();
    expect(values).toEqual(["a", "b", "c"]);
  });
});

// ---------------------------------------------------------------------------
// PipelinePubSub
// ---------------------------------------------------------------------------

describe("PipelinePubSub", () => {
  it("broadcasts to subscriber", async () => {
    const pubsub = PipelinePubSub.make<number>(10);

    const collectPromise = pubsub.subscribe().take(3).collect();

    setTimeout(async () => {
      await pubsub.publishAsync(1);
      await pubsub.publishAsync(2);
      await pubsub.publishAsync(3);
    }, 10);

    const items = await collectPromise;
    expect(items).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// Async API coverage — explicit tests for all *Async methods
// ---------------------------------------------------------------------------

describe("async API", () => {
  it("PipelineRef: getAsync, setAsync, updateAsync, updateAndGetAsync", async () => {
    const ref = PipelineRef.make("hello");
    expect(await ref.getAsync()).toBe("hello");
    await ref.setAsync("world");
    expect(await ref.getAsync()).toBe("world");
    await ref.updateAsync((s) => s + "!");
    expect(await ref.getAsync()).toBe("world!");
    const result = await ref.updateAndGetAsync((s) => s.toUpperCase());
    expect(result).toBe("WORLD!");
  });

  it("PipelineQueue: offerAsync, takeAsync, offerAllAsync, sizeAsync, shutdownAsync", async () => {
    const queue = PipelineQueue.make<number>(10);
    await queue.offerAsync(1);
    await queue.offerAsync(2);
    expect(await queue.sizeAsync()).toBe(2);
    expect(await queue.takeAsync()).toBe(1);
    await queue.offerAllAsync([10, 20]);
    expect(await queue.sizeAsync()).toBe(3); // 2 + [10, 20] - 1 taken
    await queue.shutdownAsync();
  });

  it("PipelineDeferred: succeedAsync, awaitAsync, isDoneAsync, failAsync", async () => {
    const d1 = PipelineDeferred.make<number>();
    expect(await d1.isDoneAsync()).toBe(false);
    await d1.succeedAsync(42);
    expect(await d1.isDoneAsync()).toBe(true);
    expect(await d1.awaitAsync()).toBe(42);

    const d2 = PipelineDeferred.make<string>();
    await d2.failAsync(new Error("boom"));
    expect(await d2.isDoneAsync()).toBe(true);
  });

  it("PipelineChannel: sendAsync, closeAsync", async () => {
    const ch = PipelineChannel.make<string>(10);
    await ch.sendAsync("a");
    await ch.sendAsync("b");
    expect(ch.isClosed).toBe(false);
    await ch.closeAsync();
    expect(ch.isClosed).toBe(true);
  });

  it("PipelineSignal: getAsync, setAsync, updateAsync", async () => {
    const sig = PipelineSignal.make(0);
    expect(await sig.getAsync()).toBe(0);
    await sig.setAsync(10);
    expect(await sig.getAsync()).toBe(10);
    await sig.updateAsync((n) => n * 2);
    expect(await sig.getAsync()).toBe(20);
  });

  it("PipelinePubSub: publishAsync, shutdownAsync", async () => {
    const ps = PipelinePubSub.make<number>(10);
    const collectPromise = ps.subscribe().take(2).collect();
    setTimeout(async () => {
      await ps.publishAsync(1);
      await ps.publishAsync(2);
    }, 10);
    const items = await collectPromise;
    expect(items).toEqual([1, 2]);
    await ps.shutdownAsync();
  });
});

// ---------------------------------------------------------------------------
// PipelinePool
// ---------------------------------------------------------------------------

describe("PipelinePool", () => {
  it("acquires and releases resources", async () => {
    let acquired = 0;
    let released = 0;

    const pool = PipelinePool.make({
      acquire: () => {
        acquired++;
        return { id: acquired };
      },
      release: () => {
        released++;
      },
      size: 2,
    });

    const result = await pool.useAsync((resource) => Promise.resolve(resource.id));
    expect(result).toBe(1);
    expect(acquired).toBe(1);
    expect(released).toBe(1);
  });

  it("releases on error", async () => {
    let released = false;
    const pool = PipelinePool.make({
      acquire: () => ({ id: 1 }),
      release: () => {
        released = true;
      },
      size: 1,
    });

    try {
      await pool.useAsync(() => Promise.reject(new Error("boom")));
    } catch {
      // expected
    }
    expect(released).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Pipeline.supervised
// ---------------------------------------------------------------------------

describe("Pipeline.supervised", () => {
  it("restarts on failure and eventually stops", async () => {
    let runs = 0;
    await Pipeline.fromPromise(async () => {
      runs++;
      if (runs <= 3) throw new Error("crash");
    })
      .supervised({ restart: "on-failure", maxRestarts: 5, intervalMs: 10 })
      .runPromise();

    expect(runs).toBe(4); // 1 initial + 3 failures + 1 success
  });

  it("respects maxRestarts", async () => {
    let runs = 0;
    await Pipeline.fromPromise(async () => {
      runs++;
      throw new Error("always fails");
    })
      .supervised({ restart: "on-failure", maxRestarts: 3, intervalMs: 10 })
      .runPromise();

    // Should stop after maxRestarts
    expect(runs).toBeGreaterThanOrEqual(2);
    expect(runs).toBeLessThanOrEqual(5);
  });
});
