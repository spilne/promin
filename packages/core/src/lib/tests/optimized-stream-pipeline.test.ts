import { describe, it, expect } from "bun:test";
import { StreamPipeline } from "../stream-pipeline.ts";
import { Stream } from "effect";

// These tests verify that StreamPipeline's built-in operator fusion works.
// .optimized() is deprecated (returns this) — fusion is automatic.

describe("StreamPipeline", () => {
  // -------------------------------------------------------------------------
  // Construction & bridge
  // -------------------------------------------------------------------------

  describe("construction", () => {
    it("creates from StreamPipeline.optimized()", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3]).optimized().collect();
      expect(result).toEqual([1, 2, 3]);
    });

    it("creates directly from Effect Stream", async () => {
      const result = await new StreamPipeline(Stream.fromIterable([4, 5, 6])).collect();
      expect(result).toEqual([4, 5, 6]);
    });

    it("optimized() returns this (fusion is automatic)", async () => {
      const sp = StreamPipeline.fromIterable([1, 2, 3]);
      expect(sp.optimized()).toBe(sp);
    });
  });

  // -------------------------------------------------------------------------
  // Fusible operators
  // -------------------------------------------------------------------------

  describe("fusible operators", () => {
    it("map transforms each item", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x * 2)
        .collect();
      expect(result).toEqual([2, 4, 6]);
    });

    it("chained maps", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x + 1)
        .map((x) => x * 10)
        .collect();
      expect(result).toEqual([20, 30, 40]);
    });

    it("filter keeps matching items", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .filter((x) => x > 3)
        .collect();
      expect(result).toEqual([4, 5]);
    });

    it("filter with drop action", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .filter((x) => x > 3, "drop")
        .collect();
      expect(result).toEqual([1, 2, 3]);
    });

    it("mixed map and filter chain", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5, 6])
        .optimized()
        .map((x) => x * 2)
        .filter((x) => x > 5)
        .map((x) => x + 100)
        .collect();
      expect(result).toEqual([106, 108, 110, 112]);
    });

    it("filterMap keeps non-undefined values", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .filterMap((x) => (x % 2 === 0 ? x * 10 : undefined))
        .collect();
      expect(result).toEqual([20, 40]);
    });

    it("tap runs side effect without changing values", async () => {
      const effects: number[] = [];
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .tap((x) => effects.push(x))
        .map((x) => x * 2)
        .collect();
      expect(result).toEqual([2, 4, 6]);
      expect(effects).toEqual([1, 2, 3]);
    });

    it("unNone removes null and undefined", async () => {
      const result = await StreamPipeline.fromIterable([1, null, 2, undefined, 3])
        .optimized()
        .unNone()
        .collect();
      expect(result).toEqual([1, 2, 3]);
    });

    it("5 chained maps produces same result as StreamPipeline", async () => {
      const data = Array.from({ length: 100 }, (_, i) => i);
      const fn = (x: number) => x * 2 + 1;
      const expected = await StreamPipeline.fromIterable(data)
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .collect();
      const optimized = await StreamPipeline.fromIterable(data)
        .optimized()
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .collect();
      expect(optimized).toEqual(expected);
    });

    it("mixed chain produces same result as StreamPipeline", async () => {
      const data = Array.from({ length: 1000 }, (_, i) => i);
      const mapFn = (x: number) => x * 2 + 1;
      const filterFn = (x: number) => x % 3 !== 0;
      const expected = await StreamPipeline.fromIterable(data)
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .collect();
      const optimized = await StreamPipeline.fromIterable(data)
        .optimized()
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .collect();
      expect(optimized).toEqual(expected);
    });
  });

  // -------------------------------------------------------------------------
  // Non-fusible operators (flush then delegate)
  // -------------------------------------------------------------------------

  describe("non-fusible operators", () => {
    it("take after fused chain", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .map((x) => x * 10)
        .take(3)
        .collect();
      expect(result).toEqual([10, 20, 30]);
    });

    it("drop after fused chain", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .map((x) => x * 10)
        .drop(2)
        .collect();
      expect(result).toEqual([30, 40, 50]);
    });

    it("mapAsync flushes then transforms", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x * 2)
        .mapAsync(async (x) => x + 100)
        .collect();
      expect(result).toEqual([102, 104, 106]);
    });

    it("grouped after fused chain", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5, 6])
        .optimized()
        .map((x) => x * 10)
        .grouped(2)
        .collect();
      expect(result).toEqual([
        [10, 20],
        [30, 40],
        [50, 60],
      ]);
    });

    it("scan after fused chain", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x * 10)
        .scan(0, (acc, x) => acc + x)
        .collect();
      expect(result).toEqual([0, 10, 30, 60]);
    });

    it("fused ops after non-fusible op", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .map((x) => x * 2) // fused segment 1
        .filter((x) => x > 4) // fused segment 1
        .take(3) // flush, non-fusible
        .map((x) => x + 100) // fused segment 2
        .collect();
      expect(result).toEqual([106, 108, 110]);
    });

    it("zipWithIndex after fused chain", async () => {
      const result = await StreamPipeline.fromIterable(["a", "b", "c"])
        .optimized()
        .map((s) => s.toUpperCase())
        .zipWithIndex()
        .collect();
      expect(result).toEqual([
        ["A", 0],
        ["B", 1],
        ["C", 2],
      ]);
    });
  });

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  describe("terminals", () => {
    it("forEach processes each item", async () => {
      const items: number[] = [];
      await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x * 2)
        .forEach((x) => items.push(x));
      expect(items).toEqual([2, 4, 6]);
    });

    it("reduce folds items", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4])
        .optimized()
        .map((x) => x * 10)
        .reduce(0, (acc, x) => acc + x);
      expect(result).toBe(100);
    });

    it("drain consumes without collecting", async () => {
      const effects: number[] = [];
      await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .tap((x) => effects.push(x))
        .drain();
      expect(effects).toEqual([1, 2, 3]);
    });

    it("runFirst returns first item", async () => {
      const result = await StreamPipeline.fromIterable([10, 20, 30])
        .optimized()
        .map((x) => x + 1)
        .runFirst();
      expect(result).toBe(11);
    });

    it("collectFirst finds first matching", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .map((x) => x * 10)
        .collectFirst((x) => x > 25);
      expect(result).toBe(30);
    });

    it("collectWhile collects while predicate true", async () => {
      const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
        .optimized()
        .map((x) => x * 10)
        .collectWhile((x) => x < 35);
      expect(result).toEqual([10, 20, 30]);
    });
  });

  // -------------------------------------------------------------------------
  // through()
  // -------------------------------------------------------------------------

  describe("through", () => {
    it("applies reusable transformer", async () => {
      const doubler = (s: StreamPipeline<number, never>) => s.map((x) => x * 2);
      const result = await StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .through(doubler)
        .collect();
      expect(result).toEqual([2, 4, 6]);
    });
  });

  // -------------------------------------------------------------------------
  // .stream getter
  // -------------------------------------------------------------------------

  describe("stream getter", () => {
    it("materializes pending ops into Effect Stream", async () => {
      const opt = StreamPipeline.fromIterable([1, 2, 3])
        .optimized()
        .map((x) => x * 2);

      // Access raw stream and use it with StreamPipeline
      const result = await StreamPipeline.from(opt.stream).collect();
      expect(result).toEqual([2, 4, 6]);
    });
  });
});
