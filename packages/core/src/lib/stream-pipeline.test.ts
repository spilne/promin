import { describe, it, expect } from "bun:test";
import { Data, Effect, Stream } from "effect";
import { StreamPipeline } from "../index.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe("StreamPipeline", () => {
  describe("construction", () => {
    it("from wraps a raw Effect stream", async () => {
      const items = await StreamPipeline.from(Stream.make(1, 2, 3)).collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("fromIterable creates a stream from an iterable", async () => {
      const items = await StreamPipeline.fromIterable([10, 20, 30]).collect();
      expect(items).toEqual([10, 20, 30]);
    });

    it("fromAsyncIterable creates a stream from an async iterable", async () => {
      async function* gen() {
        yield 1;
        yield 2;
        yield 3;
      }
      const items = await StreamPipeline.fromAsyncIterable(
        gen(),
        (e) => new TestError({ message: String(e) }),
      ).collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("empty creates an empty stream", async () => {
      const items = await StreamPipeline.empty().collect();
      expect(items).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // Transform
  // ---------------------------------------------------------------------------

  describe("transform", () => {
    it("map transforms each item", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .map((n) => n * 10)
        .collect();
      expect(items).toEqual([10, 20, 30]);
    });

    it("mapAsync transforms each item with a Promise", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .mapAsync(async (n) => n * 10)
        .collect();
      expect(items).toEqual([10, 20, 30]);
    });

    it("filter removes non-matching items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .filter((n) => n % 2 === 0)
        .collect();
      expect(items).toEqual([2, 4]);
    });

    it("filter with action='keep' keeps matching items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .filter((n) => n % 2 === 0, "keep")
        .collect();
      expect(items).toEqual([2, 4]);
    });

    it("filter with action='drop' removes matching items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .filter((n) => n % 2 === 0, "drop")
        .collect();
      expect(items).toEqual([1, 3, 5]);
    });

    it("filterAsync removes items by async predicate", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .filterAsync(async (n) => n % 2 === 0)
        .collect();
      expect(items).toEqual([2, 4]);
    });

    it("filterMap filters and transforms in one pass", async () => {
      const items = await StreamPipeline.fromIterable([
        { status: "ok", data: 1 },
        { status: "err", data: 2 },
        { status: "ok", data: 3 },
      ])
        .filterMap((x) => (x.status === "ok" ? x.data * 10 : undefined))
        .collect();
      expect(items).toEqual([10, 30]);
    });

    it("unNone drops undefined and null values", async () => {
      const items = await StreamPipeline.fromIterable([1, undefined, 2, null, 3])
        .unNone()
        .collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("filterMap drops all when returning undefined", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .filterMap(() => undefined)
        .collect();
      expect(items).toEqual([]);
    });

    it("tap runs side-effect without changing items", async () => {
      const sideEffects: number[] = [];
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .tap((n) => sideEffects.push(n))
        .collect();
      expect(items).toEqual([1, 2, 3]);
      expect(sideEffects).toEqual([1, 2, 3]);
    });

    it("tapAsync runs async side-effect", async () => {
      const sideEffects: number[] = [];
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .tapAsync(async (n) => {
          sideEffects.push(n);
        })
        .collect();
      expect(items).toEqual([1, 2, 3]);
      expect(sideEffects).toEqual([1, 2, 3]);
    });

    it("take takes first N items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).take(3).collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("takeWhile takes while predicate is true", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .takeWhile((n) => n < 4)
        .collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("drop skips first N items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).drop(2).collect();
      expect(items).toEqual([3, 4, 5]);
    });

    it("dropWhile skips while predicate is true", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 10, 4, 5])
        .dropWhile((n) => n < 5)
        .collect();
      expect(items).toEqual([10, 4, 5]);
    });

    it("flatMap maps each item to a sub-stream", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .flatMap((n) => StreamPipeline.fromIterable([n, n * 10]))
        .collect();
      expect(items).toEqual([1, 10, 2, 20, 3, 30]);
    });

    it("scan emits running accumulator", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .scan(0, (acc, n) => acc + n)
        .collect();
      expect(items).toEqual([0, 1, 3, 6]);
    });
  });

  // ---------------------------------------------------------------------------
  // Parallel & batching
  // ---------------------------------------------------------------------------

  describe("parallel & batching", () => {
    it("parAsyncMap transforms with bounded concurrency", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4])
        .parAsyncMap(2, async (n) => n * 10)
        .collect();
      expect(items).toEqual([10, 20, 30, 40]);
    });

    it("parAsyncMapUnordered transforms in completion order", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .parAsyncMapUnordered(3, async (n) => n * 10)
        .collect();
      expect(items.sort()).toEqual([10, 20, 30]);
    });

    it("grouped creates fixed-size batches", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).grouped(2).collect();
      expect(items).toEqual([[1, 2], [3, 4], [5]]);
    });

    it("groupWithin creates time-or-size-bounded batches", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .groupWithin(3, 5_000)
        .collect();
      // All items should arrive in batches of <=3
      const flat = items.flat();
      expect(flat).toEqual([1, 2, 3, 4, 5]);
      expect(items[0].length).toBeLessThanOrEqual(3);
    });
  });

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  describe("error handling", () => {
    it("tapError runs side-effect on error", async () => {
      let capturedTag = "";
      const items = await StreamPipeline.from(
        Stream.fail(new TestError({ message: "boom" })) as Stream.Stream<number, TestError>,
      )
        .tapError((err) => {
          capturedTag = err._tag;
        })
        .orElse(0)
        .collect();
      expect(capturedTag).toBe("TestError");
      expect(items).toEqual([0]);
    });

    it("orElse recovers with a fallback value", async () => {
      const items = await StreamPipeline.from(
        Stream.fail(new TestError({ message: "boom" })) as Stream.Stream<string, TestError>,
      )
        .orElse("fallback")
        .collect();
      expect(items).toEqual(["fallback"]);
    });
  });

  // ---------------------------------------------------------------------------
  // Merge
  // ---------------------------------------------------------------------------

  describe("merge", () => {
    it("merge interleaves two streams", async () => {
      const a = StreamPipeline.fromIterable([1, 2, 3]);
      const b = StreamPipeline.fromIterable([10, 20, 30]);
      const items = await a.merge(b).collect();
      expect(items.sort((x, y) => x - y)).toEqual([1, 2, 3, 10, 20, 30]);
    });

    it("mergeAll interleaves multiple streams", async () => {
      const items = await StreamPipeline.mergeAll(
        StreamPipeline.fromIterable([1]),
        StreamPipeline.fromIterable([2]),
        StreamPipeline.fromIterable([3]),
      ).collect();
      expect(items.sort()).toEqual([1, 2, 3]);
    });
  });

  // ---------------------------------------------------------------------------
  // Through
  // ---------------------------------------------------------------------------

  describe("through", () => {
    it("through applies a reusable transformation", async () => {
      const doubler = <E extends { _tag: string }>(s: StreamPipeline<number, E>) =>
        s.map((n) => n * 2);

      const items = await StreamPipeline.fromIterable([1, 2, 3]).through(doubler).collect();
      expect(items).toEqual([2, 4, 6]);
    });
  });

  // ---------------------------------------------------------------------------
  // Terminals
  // ---------------------------------------------------------------------------

  describe("terminals", () => {
    it("forEach processes each item", async () => {
      const collected: number[] = [];
      await StreamPipeline.fromIterable([1, 2, 3]).forEach((n) => collected.push(n));
      expect(collected).toEqual([1, 2, 3]);
    });

    it("collect gathers all items", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3]).collect();
      expect(items).toEqual([1, 2, 3]);
    });

    it("reduce folds to a single value", async () => {
      const sum = await StreamPipeline.fromIterable([1, 2, 3]).reduce(0, (acc, n) => acc + n);
      expect(sum).toBe(6);
    });

    it("drain consumes all items", async () => {
      let count = 0;
      await StreamPipeline.fromIterable([1, 2, 3])
        .tap(() => count++)
        .drain();
      expect(count).toBe(3);
    });

    it("finally runs cleanup", async () => {
      let cleaned = false;
      await StreamPipeline.fromIterable([1, 2, 3])
        .finally(() => {
          cleaned = true;
        })
        .drain();
      expect(cleaned).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Interruption
  // ---------------------------------------------------------------------------

  describe("interruption", () => {
    it("interruptOn stops when signal fires", async () => {
      const controller = new AbortController();
      const collected: number[] = [];

      // Create a stream that emits with delays
      const stream = StreamPipeline.from(
        Stream.fromEffect(Effect.succeed(1)).pipe(
          Stream.concat(Stream.fromEffect(Effect.sleep("50 millis").pipe(Effect.map(() => 2)))),
          Stream.concat(Stream.fromEffect(Effect.sleep("200 millis").pipe(Effect.map(() => 3)))),
        ),
      );

      // Abort after 100ms — should get 1 and 2, but not 3
      setTimeout(() => controller.abort(), 100);

      await stream.interruptOn(controller.signal).forEach((n) => collected.push(n));

      expect(collected).toContain(1);
      expect(collected).not.toContain(3);
    });
  });

  // ---------------------------------------------------------------------------
  // Missing coverage: mapEffect, dedupe, toStream
  // ---------------------------------------------------------------------------

  describe("mapEffect", () => {
    it("transforms items using an Effect", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .mapEffect((n) => Effect.succeed(n * 100))
        .collect();
      expect(items).toEqual([100, 200, 300]);
    });
  });

  describe("dedupe", () => {
    it("emits only when value changes", async () => {
      const items = await StreamPipeline.fromIterable([1, 1, 2, 2, 3, 3, 1]).dedupe().collect();
      expect(items).toEqual([1, 2, 3, 1]);
    });
  });

  describe("toStream", () => {
    it("returns the underlying Effect Stream", async () => {
      const stream = StreamPipeline.fromIterable([1, 2, 3]).toStream();
      const result = await Effect.runPromise(
        Stream.runCollect(stream).pipe(Effect.map((chunk) => Array.from(chunk))),
      );
      expect(result).toEqual([1, 2, 3]);
    });
  });

  describe("tapAsyncFork", () => {
    it("does not block the stream", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .tapAsyncFork(async () => {
          await new Promise((r) => setTimeout(r, 200));
        })
        .collect();
      // Collected fast — didn't wait 600ms for 3 side-effects
      expect(items).toEqual([1, 2, 3]);
    });
  });

  describe("iterate", () => {
    it("generates stream by applying function to previous value", async () => {
      const items = await StreamPipeline.iterate(1, (n) => n * 2)
        .take(5)
        .collect();
      expect(items).toEqual([1, 2, 4, 8, 16]);
    });
  });

  describe("unfold", () => {
    it("generates stream from seed until next is undefined", async () => {
      const items = await StreamPipeline.unfold(0, async (n) =>
        n < 3 ? { value: n * 10, next: n + 1 } : { value: n * 10 },
      ).collect();
      expect(items).toEqual([0, 10, 20, 30]);
    });
  });

  describe("distinctBy", () => {
    it("deduplicates by key function", async () => {
      const items = await StreamPipeline.fromIterable([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
        { id: 1, name: "c" },
        { id: 3, name: "d" },
        { id: 2, name: "e" },
      ])
        .distinctBy((x) => x.id)
        .collect();
      expect(items).toEqual([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
        { id: 3, name: "d" },
      ]);
    });
  });

  describe("runFirst", () => {
    it("returns the first item", async () => {
      const first = await StreamPipeline.fromIterable([10, 20, 30]).runFirst();
      expect(first).toBe(10);
    });

    it("returns undefined for empty stream", async () => {
      const first = await StreamPipeline.empty().runFirst();
      expect(first).toBeUndefined();
    });
  });

  describe("stream retry", () => {
    it("retries the entire stream on error", async () => {
      let attempts = 0;
      const stream = StreamPipeline.from(
        Stream.fromEffect(
          Effect.suspend(() => {
            attempts++;
            return attempts < 3 ? Effect.fail({ _tag: "TestError" as const }) : Effect.succeed(42);
          }),
        ),
      );

      const items = await stream.retry({ maxRetries: 5, baseDelayMs: 1 }).collect();
      expect(items).toEqual([42]);
      expect(attempts).toBe(3);
    });
  });

  describe("ref.value and cache.current", () => {
    it("PipelineRef.value gives sync access", () => {
      const { PipelineRef } = require("../index.ts");
      const ref = PipelineRef.make(42);
      expect(ref.value).toBe(42);
    });

    it("PipelineCache.current gives sync access", async () => {
      const { PipelineCache, Pipeline } = require("../index.ts");
      const cache = new PipelineCache(60_000);
      expect(cache.current).toBeUndefined();
      await Pipeline.succeed("hello").cached(cache).runPromise();
      expect(cache.current).toBe("hello");
    });
  });

  describe("collectWhile", () => {
    it("collects while predicate is true then stops", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 10, 4, 5]).collectWhile(
        (n) => n < 5,
      );
      expect(items).toEqual([1, 2, 3]);
    });

    it("returns empty if first item doesn't match", async () => {
      const items = await StreamPipeline.fromIterable([10, 1, 2]).collectWhile((n) => n < 5);
      expect(items).toEqual([]);
    });
  });

  describe("collectFirst", () => {
    it("returns the first matching item", async () => {
      const result = await StreamPipeline.fromIterable([
        { id: 1, status: "pending" },
        { id: 2, status: "completed" },
        { id: 3, status: "completed" },
      ]).collectFirst((item) => item.status === "completed");
      expect(result).toEqual({ id: 2, status: "completed" });
    });

    it("returns undefined when no match", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3]).collectFirst((n) => n > 10);
      expect(result).toBeUndefined();
    });
  });

  describe("tick", () => {
    it("emits sequential numbers at interval", async () => {
      const items = await StreamPipeline.tick(20).take(3).collect();
      expect(items).toEqual([0, 1, 2]);
    });
  });

  describe("zipWithIndex", () => {
    it("pairs each item with its index", async () => {
      const items = await StreamPipeline.fromIterable(["a", "b", "c"]).zipWithIndex().collect();
      expect(items).toEqual([
        ["a", 0],
        ["b", 1],
        ["c", 2],
      ]);
    });
  });

  describe("fromPipeline", () => {
    it("bridges Pipeline result into a stream", async () => {
      const { Pipeline } = await import("../index.ts");
      const items = await StreamPipeline.fromPipeline(Pipeline.succeed([1, 2, 3]))
        .flatMap((ids) => StreamPipeline.fromIterable(ids))
        .map((n) => n * 10)
        .collect();
      expect(items).toEqual([10, 20, 30]);
    });
  });

  // ---------------------------------------------------------------------------
  // New methods
  // ---------------------------------------------------------------------------

  describe("concat", () => {
    it("appends another stream after this one completes", async () => {
      const items = await StreamPipeline.fromIterable([1, 2])
        .concat(StreamPipeline.fromIterable([3, 4]))
        .collect();
      expect(items).toEqual([1, 2, 3, 4]);
    });
  });

  describe("switchMap", () => {
    it("cancels previous inner stream on new item", async () => {
      // With switch, only the last inner stream's result should dominate
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .switchMap((n) => StreamPipeline.fromIterable([n * 10]))
        .collect();
      // Each inner stream is a single item, so all emit
      expect(items).toEqual([10, 20, 30]);
    });
  });

  describe("mapAccumulate", () => {
    it("carries state and emits transformed values", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .mapAccumulate(0, (idx, val) => [idx + 1, `${idx}:${val}`] as const)
        .collect();
      expect(items).toEqual(["0:1", "1:2", "2:3"]);
    });
  });

  describe("sliding", () => {
    it("emits sliding windows", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).sliding(3).collect();
      expect(items).toEqual([
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
      ]);
    });
  });

  describe("buffer", () => {
    it("buffers items without blocking", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4, 5]).buffer(10).collect();
      expect(items).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe("debounce", () => {
    it("emits only after quiet period", async () => {
      // With a synchronous iterable, debounce should emit the last item
      const items = await StreamPipeline.fromIterable([1, 2, 3]).debounce(10).collect();
      // debounce emits last value after quiet period
      expect(items.length).toBeGreaterThanOrEqual(1);
      expect(items[items.length - 1]).toBe(3);
    });
  });

  describe("zipWith", () => {
    it("combines two streams element by element", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .zipWith(StreamPipeline.fromIterable(["a", "b", "c"]), (n, s) => `${n}${s}`)
        .collect();
      expect(items).toEqual(["1a", "2b", "3c"]);
    });

    it("stops at the shorter stream", async () => {
      const items = await StreamPipeline.fromIterable([1, 2, 3, 4])
        .zipWith(StreamPipeline.fromIterable(["a", "b"]), (n, s) => `${n}${s}`)
        .collect();
      expect(items).toEqual(["1a", "2b"]);
    });
  });

  describe("interleave", () => {
    it("alternates items from two streams", async () => {
      const items = await StreamPipeline.fromIterable([1, 3, 5])
        .interleave(StreamPipeline.fromIterable([2, 4, 6]))
        .collect();
      expect(items).toEqual([1, 2, 3, 4, 5, 6]);
    });
  });

  describe("interruptAfter", () => {
    it("stops the stream after duration", async () => {
      const items = await StreamPipeline.from(
        Stream.fromEffect(Effect.succeed(1)).pipe(
          Stream.concat(Stream.fromEffect(Effect.sleep("200 millis").pipe(Effect.map(() => 2)))),
        ),
      )
        .interruptAfter(50)
        .collect();

      expect(items).toContain(1);
      expect(items).not.toContain(2);
    });
  });

  describe("onFinalize", () => {
    it("runs async cleanup when stream ends", async () => {
      let cleaned = false;
      await StreamPipeline.fromIterable([1, 2, 3])
        .onFinalize(async () => {
          cleaned = true;
        })
        .drain();
      expect(cleaned).toBe(true);
    });
  });

  describe("mapAsyncRetry", () => {
    it("retries failed items", async () => {
      let attempts = 0;
      const items = await StreamPipeline.fromIterable([1])
        .mapAsyncRetry(
          async (n) => {
            attempts++;
            if (attempts < 3) throw new Error("transient");
            return n * 10;
          },
          { maxRetries: 5, baseDelayMs: 1 },
        )
        .collect();

      expect(items).toEqual([10]);
      expect(attempts).toBe(3);
    });
  });

  describe("broadcastThrough", () => {
    it("fans out to multiple processing pipelines", async () => {
      const sinkA: number[] = [];
      const sinkB: number[] = [];

      await StreamPipeline.fromIterable([1, 2, 3])
        .broadcastThrough(
          (s) => s.tap((n) => sinkA.push(n * 10)),
          (s) => s.tap((n) => sinkB.push(n * 100)),
        )
        .drain();

      expect(sinkA.sort()).toEqual([10, 20, 30]);
      expect(sinkB.sort()).toEqual([100, 200, 300]);
    });
  });

  describe("observe", () => {
    it("runs side-effect without blocking the main stream", async () => {
      const observed: number[] = [];
      const items = await StreamPipeline.fromIterable([1, 2, 3])
        .observe((s) => s.tap((n) => observed.push(n)))
        .collect();

      expect(items).toEqual([1, 2, 3]);
      // Observed values may arrive async, give them a moment
      await new Promise((r) => setTimeout(r, 50));
      expect(observed.sort()).toEqual([1, 2, 3]);
    });
  });

  describe("pauseWhen", () => {
    it("pauses and resumes based on ref", async () => {
      const { PipelineRef } = await import("../index.ts");
      const paused = PipelineRef.make(false);
      const collected: number[] = [];

      // Pause after first item, unpause after 100ms
      const stream = StreamPipeline.from(
        Stream.fromEffect(Effect.succeed(1)).pipe(
          Stream.concat(
            Stream.fromEffect(
              Effect.sync(() => {
                Effect.runSync(paused.set(true));
                return 2;
              }),
            ),
          ),
          Stream.concat(Stream.fromEffect(Effect.succeed(3))),
        ),
      );

      setTimeout(() => Effect.runPromise(paused.set(false)), 150);

      await stream
        .pauseWhen(paused)
        .tap((n) => collected.push(n))
        .take(3)
        .drain();

      expect(collected).toEqual([1, 2, 3]);
    });
  });
});
