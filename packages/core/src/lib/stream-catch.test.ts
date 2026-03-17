import { describe, it, expect } from "bun:test";
import { StreamPipeline, Either } from "./stream-pipeline.ts";

describe("mapAsyncAttempt — per-element error handling via Either", () => {
  it("successful elements are Right, failures are Left with original value", async () => {
    const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
      .mapAsyncAttempt(async (n) => {
        if (n === 3) throw new Error("bad");
        return n * 10;
      })
      .collect();

    const rights = result.filter(Either.isRight).map((r) => r.value);
    const lefts = result.filter(Either.isLeft).map((l) => l.error);

    expect(rights).toEqual([10, 20, 40, 50]);
    expect(lefts).toHaveLength(1);
    expect(lefts[0]!.value).toBe(3); // original value preserved
    expect((lefts[0]!.error as Error).message).toBe("bad");
  });

  it("all succeed — all Right", async () => {
    const result = await StreamPipeline.fromIterable([1, 2, 3])
      .mapAsyncAttempt(async (n) => n * 2)
      .collect();

    expect(result.every(Either.isRight)).toBe(true);
    expect(result.map((r) => (r as any).value)).toEqual([2, 4, 6]);
  });

  it("all fail — all Left, stream still completes", async () => {
    const result = await StreamPipeline.fromIterable([1, 2, 3])
      .mapAsyncAttempt(async () => {
        throw new Error("fail");
      })
      .collect();

    expect(result.every(Either.isLeft)).toBe(true);
    expect(result).toHaveLength(3);
  });
});

describe("parAsyncMapAttempt — parallel processing with Either results", () => {
  it("processes in parallel, failures don't kill the stream", async () => {
    const result = await StreamPipeline.fromIterable([1, 2, 3, 4, 5])
      .parAsyncMapAttempt(3, async (n) => {
        if (n % 2 === 0) throw new Error("even");
        return n * 100;
      })
      .collect();

    const rights = result.filter(Either.isRight);
    const lefts = result.filter(Either.isLeft);

    expect(rights).toHaveLength(3); // 1, 3, 5
    expect(lefts).toHaveLength(2); // 2, 4
  });
});

describe("rights() and lefts() — filter Either streams", () => {
  it("rights extracts successful values", async () => {
    const result = await StreamPipeline.fromIterable([1, 2, 3])
      .mapAsyncAttempt(async (n) => {
        if (n === 2) throw new Error("skip");
        return n * 10;
      })
      .rights()
      .collect();

    expect(result).toEqual([10, 30]);
  });

  it("lefts extracts failures for DLQ routing", async () => {
    const failures = await StreamPipeline.fromIterable([1, 2, 3])
      .mapAsyncAttempt(async (n) => {
        if (n === 2) throw new Error("bad");
        return n;
      })
      .lefts()
      .collect();

    expect(failures).toHaveLength(1);
    expect(failures[0]!.value).toBe(2);
  });
});

describe("DLQ pattern — route failures while continuing the stream", () => {
  it("process orders, send failures to DLQ, continue with successes", async () => {
    const dlq: { error: unknown; value: number }[] = [];

    const processed = await StreamPipeline.fromIterable([100, 200, -1, 300, -2])
      .mapAsyncAttempt(async (amount) => {
        if (amount < 0) throw new Error(`invalid amount: ${amount}`);
        return { charged: true, amount };
      })
      .tapAsync(async (either) => {
        if (Either.isLeft(either)) {
          dlq.push(either.error);
        }
      })
      .rights()
      .collect();

    expect(processed).toEqual([
      { charged: true, amount: 100 },
      { charged: true, amount: 200 },
      { charged: true, amount: 300 },
    ]);
    expect(dlq).toHaveLength(2);
    expect(dlq[0]!.value).toBe(-1);
    expect(dlq[1]!.value).toBe(-2);
  });
});
