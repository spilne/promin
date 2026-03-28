import { describe, it, expect } from "bun:test";
import { RawStream } from "./raw-stream.ts";
import { StreamPipeline } from "./stream-pipeline.ts";

describe("RawStream", () => {
  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  describe("construction", () => {
    it("fromIterable", () => {
      expect(RawStream.fromIterable([1, 2, 3]).collectSync()).toEqual([1, 2, 3]);
    });

    it("fromArray", () => {
      expect(RawStream.fromArray([4, 5, 6]).collectSync()).toEqual([4, 5, 6]);
    });

    it("empty", () => {
      expect(RawStream.empty().collectSync()).toEqual([]);
    });

    it("range", () => {
      expect(RawStream.range(0, 5).collectSync()).toEqual([0, 1, 2, 3, 4]);
    });
  });

  // -------------------------------------------------------------------------
  // Fusible operators
  // -------------------------------------------------------------------------

  describe("fusible operators", () => {
    it("map", () => {
      expect(
        RawStream.fromArray([1, 2, 3])
          .map((x) => x * 2)
          .collectSync(),
      ).toEqual([2, 4, 6]);
    });

    it("5 chained maps", () => {
      const fn = (x: number) => x * 2 + 1;
      const result = RawStream.fromArray([1, 2, 3])
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .map(fn)
        .collectSync();
      // Manually compute: start with [1,2,3], apply fn 5 times
      const expected = [1, 2, 3].map(fn).map(fn).map(fn).map(fn).map(fn);
      expect(result).toEqual(expected);
    });

    it("filter", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .filter((x) => x > 3)
          .collectSync(),
      ).toEqual([4, 5]);
    });

    it("filterMap", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .filterMap((x) => (x % 2 === 0 ? x * 10 : undefined))
          .collectSync(),
      ).toEqual([20, 40]);
    });

    it("tap", () => {
      const effects: number[] = [];
      const result = RawStream.fromArray([1, 2, 3])
        .tap((x) => effects.push(x))
        .map((x) => x * 2)
        .collectSync();
      expect(result).toEqual([2, 4, 6]);
      expect(effects).toEqual([1, 2, 3]);
    });

    it("mixed map + filter chain", () => {
      const result = RawStream.fromArray([1, 2, 3, 4, 5, 6])
        .map((x) => x * 2)
        .filter((x) => x > 5)
        .map((x) => x + 100)
        .collectSync();
      expect(result).toEqual([106, 108, 110, 112]);
    });

    it("produces same result as StreamPipeline", async () => {
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

      const raw = RawStream.fromArray(data)
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .filter(filterFn)
        .map(mapFn)
        .collectSync();

      expect(raw).toEqual(expected);
    });
  });

  // -------------------------------------------------------------------------
  // Non-fusible operators
  // -------------------------------------------------------------------------

  describe("non-fusible operators", () => {
    it("take", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .map((x) => x * 10)
          .take(3)
          .collectSync(),
      ).toEqual([10, 20, 30]);
    });

    it("drop", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .map((x) => x * 10)
          .drop(2)
          .collectSync(),
      ).toEqual([30, 40, 50]);
    });

    it("takeWhile", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .map((x) => x * 10)
          .takeWhile((x) => x < 35)
          .collectSync(),
      ).toEqual([10, 20, 30]);
    });

    it("dropWhile", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5])
          .map((x) => x * 10)
          .dropWhile((x) => x < 35)
          .collectSync(),
      ).toEqual([40, 50]);
    });

    it("grouped", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4, 5, 6])
          .map((x) => x * 10)
          .grouped(2)
          .collectSync(),
      ).toEqual([
        [10, 20],
        [30, 40],
        [50, 60],
      ]);
    });

    it("scan", () => {
      expect(
        RawStream.fromArray([1, 2, 3])
          .map((x) => x * 10)
          .scan(0, (acc, x) => acc + x)
          .collectSync(),
      ).toEqual([0, 10, 30, 60]);
    });

    it("zipWithIndex", () => {
      expect(
        RawStream.fromArray(["a", "b", "c"])
          .map((s) => s.toUpperCase())
          .zipWithIndex()
          .collectSync(),
      ).toEqual([
        ["A", 0],
        ["B", 1],
        ["C", 2],
      ]);
    });

    it("dedupe", () => {
      expect(RawStream.fromArray([1, 1, 2, 2, 3, 3, 1]).dedupe().collectSync()).toEqual([
        1, 2, 3, 1,
      ]);
    });

    it("distinctBy", () => {
      expect(
        RawStream.fromArray([
          { id: 1, name: "a" },
          { id: 2, name: "b" },
          { id: 1, name: "c" },
        ])
          .distinctBy((x) => x.id)
          .collectSync(),
      ).toEqual([
        { id: 1, name: "a" },
        { id: 2, name: "b" },
      ]);
    });

    it("flatMap", () => {
      expect(
        RawStream.fromArray([1, 2, 3])
          .flatMap((x) => RawStream.fromArray([x, x * 10]))
          .collectSync(),
      ).toEqual([1, 10, 2, 20, 3, 30]);
    });

    it("concat", () => {
      expect(
        RawStream.fromArray([1, 2])
          .concat(RawStream.fromArray([3, 4]))
          .collectSync(),
      ).toEqual([1, 2, 3, 4]);
    });

    it("fused ops after non-fusible", () => {
      const result = RawStream.fromArray([1, 2, 3, 4, 5])
        .map((x) => x * 2)
        .filter((x) => x > 4)
        .take(3)
        .map((x) => x + 100)
        .collectSync();
      expect(result).toEqual([106, 108, 110]);
    });
  });

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  describe("terminals", () => {
    it("forEach", () => {
      const items: number[] = [];
      RawStream.fromArray([1, 2, 3])
        .map((x) => x * 2)
        .forEach((x) => items.push(x));
      expect(items).toEqual([2, 4, 6]);
    });

    it("reduce", () => {
      expect(
        RawStream.fromArray([1, 2, 3, 4])
          .map((x) => x * 10)
          .reduce(0, (acc, x) => acc + x),
      ).toBe(100);
    });

    it("drain runs without collecting", () => {
      const effects: number[] = [];
      RawStream.fromArray([1, 2, 3])
        .tap((x) => effects.push(x))
        .drain();
      expect(effects).toEqual([1, 2, 3]);
    });

    it("first", () => {
      expect(
        RawStream.fromArray([10, 20, 30])
          .map((x) => x + 1)
          .first(),
      ).toBe(11);
    });

    it("collect async", async () => {
      const result = await RawStream.fromArray([1, 2, 3])
        .map((x) => x * 2)
        .collect();
      expect(result).toEqual([2, 4, 6]);
    });
  });

  // -------------------------------------------------------------------------
  // Bridge
  // -------------------------------------------------------------------------

  describe("bridge", () => {
    it("toEffectStream → StreamPipeline", async () => {
      const stream = RawStream.fromArray([1, 2, 3])
        .map((x) => x * 2)
        .toEffectStream();
      const result = await StreamPipeline.from(stream).collect();
      expect(result).toEqual([2, 4, 6]);
    });
  });
});
