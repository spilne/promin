import { describe, it, expect } from "bun:test";
import { Effect } from "effect";
import { barrierTestSuite } from "../barrier-test-suite.ts";
import { PipelineBarrier } from "../barrier.ts";

barrierTestSuite(() => PipelineBarrier.make({ parties: 3 }));

describe("PipelineBarrier Effect API", () => {
  it("await unblocks all fibers via Effect.all", async () => {
    const b = PipelineBarrier.make({ parties: 3 });
    const results: number[] = [];

    await Effect.runPromise(
      Effect.all(
        [
          b.await.pipe(Effect.map(() => results.push(1))),
          b.await.pipe(Effect.map(() => results.push(2))),
          b.await.pipe(Effect.map(() => results.push(3))),
        ],
        { concurrency: "unbounded" },
      ),
    );

    expect(results).toHaveLength(3);
  });

  it("arrived returns count via Effect", async () => {
    const b = PipelineBarrier.make({ parties: 2 });

    const p = b.awaitAsync();
    await new Promise((r) => setTimeout(r, 10));

    const count = await Effect.runPromise(b.arrived);
    expect(count).toBe(1);

    const p2 = b.awaitAsync();
    await Promise.all([p, p2]);
  });
});
