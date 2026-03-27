import { group, bench, run } from "mitata";
import { StreamPipeline } from "./stream-pipeline.ts";
import { OptimizedStreamPipeline } from "./optimized-stream-pipeline.ts";
import { Effect, Stream, Chunk } from "effect";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeArray(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

const mapFn = (x: number) => x * 2 + 1;
const filterFn = (x: number) => x % 3 !== 0;

// ---------------------------------------------------------------------------
// 1. Map chain — raw loop vs async generator vs Effect Stream vs StreamPipeline
// ---------------------------------------------------------------------------

group("map chain (100K elements)", () => {
  const data = makeArray(100_000);

  bench("raw for loop — 1 map", () => {
    const result = new Array(data.length);
    for (let i = 0; i < data.length; i++) {
      result[i] = mapFn(data[i]);
    }
    return result;
  });

  bench("Array.map — 1 map", () => {
    return data.map(mapFn);
  });

  bench("Array.map — 5 chained maps", () => {
    return data.map(mapFn).map(mapFn).map(mapFn).map(mapFn).map(mapFn);
  });

  bench("Array.map — 10 chained maps", () => {
    return data
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn);
  });

  bench("async generator — 1 map", async () => {
    async function* gen() {
      for (const x of data) yield mapFn(x);
    }
    const result: number[] = [];
    for await (const x of gen()) result.push(x);
    return result;
  });

  bench("async generator — 5 chained maps", async () => {
    function chain(
      source: AsyncIterable<number>,
      fn: (x: number) => number,
    ): AsyncIterable<number> {
      return {
        async *[Symbol.asyncIterator]() {
          for await (const x of source) yield fn(x);
        },
      };
    }
    async function* gen() {
      for (const x of data) yield x;
    }
    let s: AsyncIterable<number> = gen();
    for (let i = 0; i < 5; i++) s = chain(s, mapFn);
    const result: number[] = [];
    for await (const x of s) result.push(x);
    return result;
  });

  bench("Effect Stream — 1 map", async () => {
    const stream = Stream.fromIterable(data).pipe(Stream.map(mapFn));
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("Effect Stream — 5 chained maps", async () => {
    const stream = Stream.fromIterable(data).pipe(
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
    );
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("Effect Stream — 10 chained maps", async () => {
    const stream = Stream.fromIterable(data).pipe(
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
      Stream.map(mapFn),
    );
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("StreamPipeline — 1 map", async () => {
    return StreamPipeline.fromIterable(data).map(mapFn).collect();
  });

  bench("StreamPipeline — 5 chained maps", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });

  bench("StreamPipeline — 10 chained maps", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });

  bench("Optimized — 1 map", async () => {
    return StreamPipeline.fromIterable(data).optimized().map(mapFn).collect();
  });

  bench("Optimized — 5 chained maps", async () => {
    return StreamPipeline.fromIterable(data)
      .optimized()
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });

  bench("Optimized — 10 chained maps", async () => {
    return StreamPipeline.fromIterable(data)
      .optimized()
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .map(mapFn)
      .collect();
  });
});

// ---------------------------------------------------------------------------
// 2. Filter chain
// ---------------------------------------------------------------------------

group("filter chain (100K elements)", () => {
  const data = makeArray(100_000);

  bench("Array.filter — 1 filter", () => {
    return data.filter(filterFn);
  });

  bench("Array.filter — 5 chained filters", () => {
    return data
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn);
  });

  bench("async generator — 1 filter", async () => {
    async function* gen() {
      for (const x of data) if (filterFn(x)) yield x;
    }
    const result: number[] = [];
    for await (const x of gen()) result.push(x);
    return result;
  });

  bench("Effect Stream — 1 filter", async () => {
    const stream = Stream.fromIterable(data).pipe(Stream.filter(filterFn));
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("Effect Stream — 5 chained filters", async () => {
    const stream = Stream.fromIterable(data).pipe(
      Stream.filter(filterFn),
      Stream.filter(filterFn),
      Stream.filter(filterFn),
      Stream.filter(filterFn),
      Stream.filter(filterFn),
    );
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("StreamPipeline — 1 filter", async () => {
    return StreamPipeline.fromIterable(data).filter(filterFn).collect();
  });

  bench("StreamPipeline — 5 chained filters", async () => {
    return StreamPipeline.fromIterable(data)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .collect();
  });

  bench("Optimized — 1 filter", async () => {
    return StreamPipeline.fromIterable(data).optimized().filter(filterFn).collect();
  });

  bench("Optimized — 5 chained filters", async () => {
    return StreamPipeline.fromIterable(data)
      .optimized()
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .filter(filterFn)
      .collect();
  });
});

// ---------------------------------------------------------------------------
// 3. Mixed map + filter
// ---------------------------------------------------------------------------

group("map + filter mixed (100K elements)", () => {
  const data = makeArray(100_000);

  bench("Array — map.filter.map.filter.map", () => {
    return data.map(mapFn).filter(filterFn).map(mapFn).filter(filterFn).map(mapFn);
  });

  bench("raw fused loop — map.filter.map.filter.map", () => {
    const result: number[] = [];
    for (let i = 0; i < data.length; i++) {
      let v = mapFn(data[i]);
      if (!filterFn(v)) continue;
      v = mapFn(v);
      if (!filterFn(v)) continue;
      v = mapFn(v);
      result.push(v);
    }
    return result;
  });

  bench("async generator — map.filter.map.filter.map", async () => {
    async function* gen() {
      for (const x of data) {
        let v = mapFn(x);
        if (!filterFn(v)) continue;
        v = mapFn(v);
        if (!filterFn(v)) continue;
        yield mapFn(v);
      }
    }
    const result: number[] = [];
    for await (const x of gen()) result.push(x);
    return result;
  });

  bench("Effect Stream — map.filter.map.filter.map", async () => {
    const stream = Stream.fromIterable(data).pipe(
      Stream.map(mapFn),
      Stream.filter(filterFn),
      Stream.map(mapFn),
      Stream.filter(filterFn),
      Stream.map(mapFn),
    );
    return Effect.runPromise(Stream.runCollect(stream));
  });

  bench("StreamPipeline — map.filter.map.filter.map", async () => {
    return StreamPipeline.fromIterable(data)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .collect();
  });

  bench("Optimized — map.filter.map.filter.map", async () => {
    return StreamPipeline.fromIterable(data)
      .optimized()
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .filter(filterFn)
      .map(mapFn)
      .collect();
  });
});

// ---------------------------------------------------------------------------
// 4. Scale test — varying element counts
// ---------------------------------------------------------------------------

for (const size of [10_000, 100_000, 1_000_000]) {
  const label = size >= 1_000_000 ? `${size / 1_000_000}M` : `${size / 1_000}K`;

  group(`scale: 5 maps (${label} elements)`, () => {
    const data = makeArray(size);

    bench("raw fused loop", () => {
      const result = new Array(data.length);
      for (let i = 0; i < data.length; i++) {
        result[i] = mapFn(mapFn(mapFn(mapFn(mapFn(data[i])))));
      }
      return result;
    });

    bench("Array.map x5", () => {
      return data.map(mapFn).map(mapFn).map(mapFn).map(mapFn).map(mapFn);
    });

    bench("Effect Stream x5", async () => {
      const stream = Stream.fromIterable(data).pipe(
        Stream.map(mapFn),
        Stream.map(mapFn),
        Stream.map(mapFn),
        Stream.map(mapFn),
        Stream.map(mapFn),
      );
      return Effect.runPromise(Stream.runCollect(stream));
    });

    bench("StreamPipeline x5", async () => {
      return StreamPipeline.fromIterable(data)
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .collect();
    });

    bench("Optimized x5", async () => {
      return StreamPipeline.fromIterable(data)
        .optimized()
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .map(mapFn)
        .collect();
    });
  });
}

// ---------------------------------------------------------------------------
// 5. Grouped / batching
// ---------------------------------------------------------------------------

group("grouped batching (100K elements)", () => {
  const data = makeArray(100_000);

  bench("manual chunking — batch 1000", () => {
    const result: number[][] = [];
    for (let i = 0; i < data.length; i += 1000) {
      result.push(data.slice(i, i + 1000));
    }
    return result;
  });

  bench("StreamPipeline.grouped(1000)", async () => {
    return StreamPipeline.fromIterable(data).grouped(1000).collect();
  });

  bench("Effect Stream.grouped(1000)", async () => {
    const stream = Stream.fromIterable(data).pipe(Stream.grouped(1000), Stream.map(Chunk.toArray));
    return Effect.runPromise(Stream.runCollect(stream));
  });
});

await run();
