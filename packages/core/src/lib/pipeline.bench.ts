import { group, bench, run } from "mitata";
import { Pipeline } from "./pipeline.ts";

// ---------------------------------------------------------------------------
// Pipeline.run — basic execution overhead
// ---------------------------------------------------------------------------

group("Pipeline.run: basic execution", () => {
  bench("raw Promise", async () => {
    return Promise.resolve(42);
  });

  bench("Pipeline.succeed", async () => {
    return Pipeline.succeed(42).runPromise();
  });

  bench("Pipeline.fromPromise", async () => {
    return Pipeline.fromPromise(() => Promise.resolve(42)).runPromise();
  });

  bench("Pipeline.fromPromise + map", async () => {
    return Pipeline.fromPromise(() => Promise.resolve(21))
      .map((x) => x * 2)
      .runPromise();
  });

  bench("Pipeline.fromPromise + 5x map", async () => {
    return Pipeline.fromPromise(() => Promise.resolve(1))
      .map((x) => x + 1)
      .map((x) => x + 1)
      .map((x) => x + 1)
      .map((x) => x + 1)
      .map((x) => x + 1)
      .runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline.flatMap chain depth
// ---------------------------------------------------------------------------

group("Pipeline.flatMap chain", () => {
  bench("raw Promise.then x5", async () => {
    return Promise.resolve(1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1)
      .then((x) => x + 1);
  });

  bench("Pipeline.flatMap x5", async () => {
    return Pipeline.succeed(1)
      .flatMap((x) => Pipeline.succeed(x + 1))
      .flatMap((x) => Pipeline.succeed(x + 1))
      .flatMap((x) => Pipeline.succeed(x + 1))
      .flatMap((x) => Pipeline.succeed(x + 1))
      .flatMap((x) => Pipeline.succeed(x + 1))
      .runPromise();
  });

  bench("Pipeline.flatMapAsync x5", async () => {
    return Pipeline.succeed(1)
      .flatMapAsync(async (x) => x + 1)
      .flatMapAsync(async (x) => x + 1)
      .flatMapAsync(async (x) => x + 1)
      .flatMapAsync(async (x) => x + 1)
      .flatMapAsync(async (x) => x + 1)
      .runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline.retry
// ---------------------------------------------------------------------------

group("Pipeline.retry", () => {
  bench("Pipeline succeeds (no retry needed)", async () => {
    return Pipeline.fromPromise(() => Promise.resolve(42))
      .retry(3)
      .runPromise();
  });

  bench("Pipeline with retry policy (succeeds)", async () => {
    return Pipeline.fromPromise(() => Promise.resolve(42))
      .retry({ maxRetries: 3, baseDelayMs: 1 })
      .runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline.timeout
// ---------------------------------------------------------------------------

group("Pipeline.timeout", () => {
  bench("raw Promise.race", async () => {
    return Promise.race([
      Promise.resolve(42),
      new Promise((_, reject) => setTimeout(() => reject("timeout"), 5000)),
    ]);
  });

  bench("Pipeline.timeout (succeeds fast)", async () => {
    return Pipeline.succeed(42).timeout(5000).runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline.all (parallel execution)
// ---------------------------------------------------------------------------

for (const count of [10, 100, 1000]) {
  group(`Pipeline.all — ${count} items`, () => {
    const items = Array.from({ length: count }, (_, i) => i);

    bench("Promise.all", async () => {
      return Promise.all(items.map((i) => Promise.resolve(i * 2)));
    });

    bench("Pipeline.all", async () => {
      return Pipeline.all(...items.map((i) => Pipeline.succeed(i * 2))).runPromise();
    });

    bench("Pipeline.forEach", async () => {
      return Pipeline.forEach(items, (i) => Pipeline.succeed(i * 2), {
        concurrency: count,
      }).runPromise();
    });
  });
}

// ---------------------------------------------------------------------------
// Pipeline.race
// ---------------------------------------------------------------------------

group("Pipeline.race", () => {
  bench("Promise.race (3 items)", async () => {
    return Promise.race([Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)]);
  });

  bench("Pipeline.race (3 items)", async () => {
    return Pipeline.race(
      Pipeline.succeed(1),
      Pipeline.succeed(2),
      Pipeline.succeed(3),
    ).runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline error handling
// ---------------------------------------------------------------------------

group("Pipeline error handling", () => {
  bench("Pipeline.handleError", async () => {
    return Pipeline.fail({ _tag: "TestError" as const })
      .handleError(() => 0)
      .runPromise();
  });

  bench("Pipeline.orElse", async () => {
    return (Pipeline.fail({ _tag: "TestError" as const }) as any).orElse(0).runPromise();
  });

  bench("Pipeline.catch by tag", async () => {
    return (Pipeline.fail({ _tag: "TestError" as const }) as any)
      .catch("TestError", () => 0)
      .runPromise();
  });
});

// ---------------------------------------------------------------------------
// Pipeline.runSafe
// ---------------------------------------------------------------------------

group("Pipeline.runSafe", () => {
  bench("runSafe — success", async () => {
    return Pipeline.succeed(42).runSafe();
  });

  bench("runSafe — failure", async () => {
    return Pipeline.fail({ _tag: "Err" as const }).runSafe();
  });

  bench("runPromise — success", async () => {
    return Pipeline.succeed(42).runPromise();
  });
});

await run();
