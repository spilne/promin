import { group, bench, run } from "mitata";
import { Stream, Chunk } from "effect";
import { StreamPipeline } from "../stream-pipeline.ts";

/**
 * Benchmarks comparing what TopologyRunner compiles:
 * - Before fusion: chained .map().filter() on StreamPipeline (one Effect layer per op)
 * - After fusion: single mapChunks with fused function (one Effect layer total)
 *
 * We test the compiled output directly — not the full runner lifecycle.
 */

const mapFn = (x: number) => x * 2 + 1;
const filterFn = (x: number) => x % 3 !== 0;

function makeArray(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

// ---------------------------------------------------------------------------
// 5 chained maps
// ---------------------------------------------------------------------------

group("TopologyRunner compile: 5 maps (100K)", () => {
  const data = makeArray(100_000);

  bench("unfused: .map().map().map().map().map()", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });

  bench("fused: single mapChunks", async () => {
    const fused = (x: unknown) => {
      let v = x as number;
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      v = mapFn(v);
      return v;
    };
    return new StreamPipeline(
      Stream.mapChunks(Stream.fromIterable(data), Chunk.map(fused)),
    ).collect();
  });
});

// ---------------------------------------------------------------------------
// Mixed map + filter
// ---------------------------------------------------------------------------

group("TopologyRunner compile: mixed chain (100K)", () => {
  const data = makeArray(100_000);
  const SKIP = Symbol();

  bench("unfused: .map().filter().map().filter().map()", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .collect();
  });

  bench("fused: single mapChunks", async () => {
    const fused = (value: unknown): unknown => {
      let v = mapFn(value as number);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      return v;
    };
    return new StreamPipeline(
      Stream.mapChunks(Stream.fromIterable(data), (chunk) => {
        const src = Chunk.toArray(chunk);
        const result: unknown[] = [];
        for (let i = 0; i < src.length; i++) {
          const v = fused(src[i]);
          if (v !== SKIP) result.push(v);
        }
        return Chunk.unsafeFromArray(result);
      }),
    ).collect();
  });
});

// ---------------------------------------------------------------------------
// Scale: 1M elements
// ---------------------------------------------------------------------------

group("TopologyRunner compile: mixed chain (1M)", () => {
  const data = makeArray(1_000_000);
  const SKIP = Symbol();

  bench("unfused: .map().filter().map().filter().map()", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .collect();
  });

  bench("fused: single mapChunks", async () => {
    const fused = (value: unknown): unknown => {
      let v = mapFn(value as number);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      if (!filterFn(v)) return SKIP;
      v = mapFn(v);
      return v;
    };
    return new StreamPipeline(
      Stream.mapChunks(Stream.fromIterable(data), (chunk) => {
        const src = Chunk.toArray(chunk);
        const result: unknown[] = [];
        for (let i = 0; i < src.length; i++) {
          const v = fused(src[i]);
          if (v !== SKIP) result.push(v);
        }
        return Chunk.unsafeFromArray(result);
      }),
    ).collect();
  });
});

await run();
