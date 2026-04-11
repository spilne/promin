import { describe, it, expect } from "bun:test";
import { Data, Effect } from "effect";
import { Pipeline } from "./pipeline.ts";
import { PipelineSingleflight } from "./singleflight.ts";
import { singleflightTestSuite } from "./singleflight-test-suite.ts";

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Portable conformance suite (Promise API)
// ---------------------------------------------------------------------------

singleflightTestSuite(() => PipelineSingleflight.make());

// ---------------------------------------------------------------------------
// Effect API — doEffect
// ---------------------------------------------------------------------------

describe("PipelineSingleflight — doEffect", () => {
  it("single call executes normally", async () => {
    const sf = PipelineSingleflight.make();
    const result = await Effect.runPromise(sf.doEffect("k", Effect.succeed(42)));
    expect(result).toBe(42);
  });

  it("two concurrent calls with same key — one execution", async () => {
    const sf = PipelineSingleflight.make();
    let executions = 0;

    const work = sf.doEffect(
      "k",
      Effect.promise(async () => {
        executions++;
        await delay(50);
        return "result";
      }),
    );

    const [a, b] = await Effect.runPromise(Effect.all([work, work], { concurrency: 2 }));
    expect(executions).toBe(1);
    expect(a).toBe("result");
    expect(b).toBe("result");
  });

  it("different keys execute independently", async () => {
    const sf = PipelineSingleflight.make();
    let executions = 0;

    const makeWork = (key: string, value: number) =>
      sf.doEffect(
        key,
        Effect.promise(async () => {
          executions++;
          await delay(30);
          return value;
        }),
      );

    const [a, b] = await Effect.runPromise(
      Effect.all([makeWork("a", 1), makeWork("b", 2)], { concurrency: 2 }),
    );
    expect(executions).toBe(2);
    expect(a).toBe(1);
    expect(b).toBe(2);
  });

  it("failure propagates to all joiners", async () => {
    const sf = PipelineSingleflight.make();
    let executions = 0;

    const work = sf.doEffect(
      "k",
      Effect.flatMap(
        Effect.promise(async () => {
          executions++;
          await delay(50);
        }),
        () => Effect.fail(new TestError({ message: "boom" })),
      ),
    );

    const results = await Effect.runPromise(
      Effect.all([Effect.either(work), Effect.either(work)], { concurrency: 2 }),
    );

    expect(executions).toBe(1);
    for (const r of results) {
      expect(r._tag).toBe("Left");
      if (r._tag === "Left") {
        expect((r.left as TestError).message).toBe("boom");
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Pipeline API — do
// ---------------------------------------------------------------------------

describe("PipelineSingleflight — do (Pipeline)", () => {
  it("deduplicates and returns Pipeline", async () => {
    const sf = PipelineSingleflight.make();
    let executions = 0;

    const work = () =>
      sf.do(
        "k",
        Pipeline.fn(async () => {
          executions++;
          await delay(50);
          return "hello";
        }),
      );

    const [a, b] = await Promise.all([work().runPromise(), work().runPromise()]);
    expect(executions).toBe(1);
    expect(a).toBe("hello");
    expect(b).toBe("hello");
  });
});
