import { describe, it, expect } from "bun:test";
import { Data, Effect, Either } from "effect";
import { Pipeline, PipelineResult, type PipelineDefaults } from "../index.ts";

// ---------------------------------------------------------------------------
// Test error types
// ---------------------------------------------------------------------------

class TestError extends Data.TaggedError("TestError")<{
  readonly message: string;
}> {}

class OtherError extends Data.TaggedError("OtherError")<{
  readonly code: number;
}> {}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe("Pipeline", () => {
  describe("construction", () => {
    it("succeed creates a successful pipeline", async () => {
      const result = await Pipeline.succeed(42).runPromise();
      expect(result).toBe(42);
    });

    it("fail creates a failed pipeline", async () => {
      const { error } = await Pipeline.fail(new TestError({ message: "boom" })).runSafe();
      expect(error?._tag).toBe("TestError");
    });

    it("from wraps a raw Effect", async () => {
      const effect = Effect.succeed("hello");
      const result = await Pipeline.from(effect).runPromise();
      expect(result).toBe("hello");
    });

    it("fromPromise wraps a Promise-returning function", async () => {
      const result = await Pipeline.fromPromise(() => Promise.resolve(99)).runPromise();
      expect(result).toBe(99);
    });
  });

  // ---------------------------------------------------------------------------
  // Transform
  // ---------------------------------------------------------------------------

  describe("transform", () => {
    it("map transforms the value", async () => {
      const result = await Pipeline.succeed(10)
        .map((n) => n * 2)
        .runPromise();
      expect(result).toBe(20);
    });

    it("flatMap chains pipelines", async () => {
      const result = await Pipeline.succeed(5)
        .flatMap((n) => Pipeline.succeed(n + 3))
        .runPromise();
      expect(result).toBe(8);
    });

    it("tap runs side-effect without changing value", async () => {
      let sideEffect = 0;
      const result = await Pipeline.succeed(42)
        .tap((n) => {
          sideEffect = n;
        })
        .runPromise();
      expect(result).toBe(42);
      expect(sideEffect).toBe(42);
    });

    it("tapAsync runs async side-effect", async () => {
      let sideEffect = 0;
      const result = await Pipeline.succeed(42)
        .tapAsync(async (n) => {
          sideEffect = n;
        })
        .runPromise();
      expect(result).toBe(42);
      expect(sideEffect).toBe(42);
    });

    it("mapAsync transforms with a Promise", async () => {
      const result = await Pipeline.succeed(10)
        .mapAsync(async (n) => n * 3)
        .runPromise();
      expect(result).toBe(30);
    });

    it("filter passes matching values", async () => {
      const result = await Pipeline.succeed(10)
        .filter({
          predicate: (n) => n > 5,
          orFail: () => new TestError({ message: "too small" }),
        })
        .runPromise();
      expect(result).toBe(10);
    });

    it("filter fails on non-matching values", async () => {
      const { error } = await Pipeline.succeed(3)
        .filter({
          predicate: (n) => n > 5,
          orFail: () => new TestError({ message: "too small" }),
        })
        .runSafe();
      expect(error?._tag).toBe("TestError");
    });
  });

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  describe("error handling", () => {
    it("orElse recovers with a fallback value", async () => {
      const result = await Pipeline.from(
        Effect.fail(new TestError({ message: "oops" })) as Effect.Effect<string, TestError>,
      )
        .orElse("fallback")
        .runPromise();
      expect(result).toBe("fallback");
    });

    it("orElsePipeline recovers with another pipeline", async () => {
      const result = await Pipeline.from(
        Effect.fail(new TestError({ message: "oops" })) as Effect.Effect<string, TestError>,
      )
        .orElsePipeline(() => Pipeline.succeed("recovered"))
        .runPromise();
      expect(result).toBe("recovered");
    });

    it("catch recovers from specific error tags", async () => {
      const result = await Pipeline.from(
        Effect.fail(new TestError({ message: "oops" })) as Effect.Effect<string, TestError>,
      )
        .catch("TestError", (err) => `caught: ${err.message}`)
        .runPromise();
      expect(result).toBe("caught: oops");
    });

    it("catch lets non-matching errors through", async () => {
      const pipeline = Pipeline.from(
        Effect.fail(new OtherError({ code: 404 })) as Effect.Effect<string, TestError | OtherError>,
      ).catch("TestError", () => "caught");

      const { error } = await pipeline.runSafe();
      expect(error?._tag).toBe("OtherError");
    });

    it("tapError runs side-effect on error", async () => {
      let capturedTag = "";
      await Pipeline.fail(new TestError({ message: "oops" }))
        .tapError((err) => {
          capturedTag = err._tag;
        })
        .runSafe();
      expect(capturedTag).toBe("TestError");
    });
  });

  // ---------------------------------------------------------------------------
  // Resilience
  // ---------------------------------------------------------------------------

  describe("resilience", () => {
    it("retry retries on typed errors", async () => {
      let attempts = 0;
      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: `attempt ${attempts}` }))
            : Effect.succeed("ok");
        }),
      )
        .retry({ maxRetries: 3, baseDelayMs: 1 })
        .runPromise();

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });

    it("retryAll retries on success values too", async () => {
      let attempts = 0;
      const result = await Pipeline.from(
        Effect.sync(() => {
          attempts++;
          return { status: attempts < 3 ? "pending" : "done" };
        }),
      )
        .retryAll({
          maxRetries: 5,
          baseDelayMs: 1,
          shouldRetry: (r) => PipelineResult.isSuccess(r) && r.value.status !== "done",
        })
        .runPromise();

      expect(result.status).toBe("done");
      expect(attempts).toBe(3);
    });

    it("timeout fails with TimeoutError when exceeded", async () => {
      const { error } = await Pipeline.fromPromise(
        () => new Promise((resolve) => setTimeout(resolve, 5000)),
      )
        .timeout(50)
        .runSafe();
      expect(error?._tag).toBe("TimeoutError");
    });
  });

  // ---------------------------------------------------------------------------
  // Static combinators
  // ---------------------------------------------------------------------------

  describe("combinators", () => {
    it("all runs pipelines in parallel", async () => {
      const [a, b, c] = await Pipeline.all(
        Pipeline.succeed(1),
        Pipeline.succeed("two"),
        Pipeline.succeed(true),
      ).runPromise();

      expect(a).toBe(1);
      expect(b).toBe("two");
      expect(c).toBe(true);
    });

    it("allSettled returns Either for each", async () => {
      const results = await Pipeline.allSettled(
        Pipeline.succeed(1),
        Pipeline.fail(new TestError({ message: "boom" })),
      ).runPromise();

      expect(results.length).toBe(2);
    });

    it("race returns the first to succeed", async () => {
      const result = await Pipeline.race(
        Pipeline.fromPromise(() => new Promise<number>((r) => setTimeout(() => r(1), 100))),
        Pipeline.succeed(2),
      ).runPromise();

      expect(result).toBe(2);
    });

    it("fallback tries in order", async () => {
      const result = await Pipeline.fallback(
        Pipeline.fail(new TestError({ message: "first" })),
        Pipeline.succeed("second"),
      ).runPromise();

      expect(result).toBe("second");
    });

    it("forEach runs with bounded concurrency", async () => {
      const results = await Pipeline.forEach([1, 2, 3, 4, 5], (n) => Pipeline.succeed(n * 10), {
        concurrency: 2,
      }).runPromise();

      expect(results).toEqual([10, 20, 30, 40, 50]);
    });
  });

  // ---------------------------------------------------------------------------
  // Scoped resources
  // ---------------------------------------------------------------------------

  describe("scoped", () => {
    it("acquires and releases resources", async () => {
      let released = false;
      const result = await Pipeline.scoped({
        acquire: () => ({ value: 42 }),
        release: () => {
          released = true;
        },
        use: (resource) => Pipeline.succeed(resource.value),
      }).runPromise();

      expect(result).toBe(42);
      expect(released).toBe(true);
    });

    it("releases on error", async () => {
      let released = false;
      const { error } = await Pipeline.scoped({
        acquire: () => ({ value: 42 }),
        release: () => {
          released = true;
        },
        use: () => Pipeline.fail(new TestError({ message: "boom" })),
      }).runSafe();

      expect(error?._tag).toBe("TestError");
      expect(released).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Terminals
  // ---------------------------------------------------------------------------

  describe("terminals", () => {
    it("runSafe returns data on success", async () => {
      const result = await Pipeline.succeed(42).runSafe();
      expect(result).toEqual({ data: 42, error: null });
    });

    it("runSafe returns error on failure", async () => {
      const result = await Pipeline.fail(new TestError({ message: "oops" })).runSafe();
      expect(result.data).toBeNull();
      expect(result.error?._tag).toBe("TestError");
    });

    it("runEither returns Right on success", async () => {
      const either = await Pipeline.succeed(42).runEither();
      expect(either._tag).toBe("Right");
    });

    it("runEither returns Left on failure", async () => {
      const either = await Pipeline.fail(new TestError({ message: "oops" })).runEither();
      expect(either._tag).toBe("Left");
    });

    it("finally runs cleanup on success", async () => {
      let cleaned = false;
      await Pipeline.succeed(42)
        .finally(() => {
          cleaned = true;
        })
        .runPromise();
      expect(cleaned).toBe(true);
    });

    it("finally runs cleanup on failure", async () => {
      let cleaned = false;
      await Pipeline.fail(new TestError({ message: "oops" }))
        .finally(() => {
          cleaned = true;
        })
        .runSafe();
      expect(cleaned).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Polling
  // ---------------------------------------------------------------------------

  describe("polling", () => {
    it("pollUntil repeats until condition met", async () => {
      let count = 0;
      const result = await Pipeline.from(
        Effect.sync(() => {
          count++;
          return count;
        }),
      )
        .pollUntil({
          until: (n) => n >= 3,
          intervalMs: 10,
          maxAttempts: 10,
        })
        .runPromise();

      expect(result).toBe(3);
    });

    it("pollUntil fails with PollTimeoutError on exhaustion", async () => {
      const { error } = await Pipeline.succeed("not-ready")
        .pollUntil({
          until: () => false,
          intervalMs: 10,
          maxAttempts: 3,
          maxDurationMs: 5_000,
        })
        .runSafe();

      expect(error?._tag).toBe("PollTimeoutError");
    });

    it("pollUntilWithBackoff repeats with exponential backoff", async () => {
      let count = 0;
      const result = await Pipeline.from(
        Effect.sync(() => {
          count++;
          return count;
        }),
      )
        .pollUntilWithBackoff({
          until: (n) => n >= 3,
          initialIntervalMs: 10,
          maxIntervalMs: 50,
          maxAttempts: 10,
        })
        .runPromise();

      expect(result).toBe(3);
    });
  });

  // ---------------------------------------------------------------------------
  // raceWith
  // ---------------------------------------------------------------------------

  describe("concurrently", () => {
    it("runs two pipelines in parallel", async () => {
      const [a, b] = await Pipeline.succeed(1).concurrently(Pipeline.succeed("two")).runPromise();
      expect(a).toBe(1);
      expect(b).toBe("two");
    });

    it("runs multiple pipelines in parallel with flat tuple", async () => {
      const [a, b, c] = await Pipeline.succeed(1)
        .concurrently(Pipeline.succeed("two"), Pipeline.succeed(true))
        .runPromise();
      expect(a).toBe(1);
      expect(b).toBe("two");
      expect(c).toBe(true);
    });
  });

  describe("race (instance)", () => {
    it("races against one other pipeline", async () => {
      const slow = Pipeline.fromPromise(
        () => new Promise<string>((r) => setTimeout(() => r("slow"), 200)),
      );
      const fast = Pipeline.succeed("fast");

      const result = await slow.race(fast).runPromise();
      expect(result).toBe("fast");
    });

    it("races against multiple others", async () => {
      const slow1 = Pipeline.fromPromise(
        () => new Promise<number>((r) => setTimeout(() => r(1), 200)),
      );
      const slow2 = Pipeline.fromPromise(
        () => new Promise<number>((r) => setTimeout(() => r(2), 200)),
      );
      const fast = Pipeline.succeed(3);

      const result = await slow1.race(slow2, fast).runPromise();
      expect(result).toBe(3);
    });
  });

  // ---------------------------------------------------------------------------
  // hedged
  // ---------------------------------------------------------------------------

  describe("hedged", () => {
    it("returns result from primary or backup", async () => {
      const pipeline = Pipeline.fromPromise(() => Promise.resolve("done"));
      const result = await Pipeline.hedged(pipeline, { hedgeDelayMs: 50 }).runPromise();
      expect(result).toBe("done");
    });
  });

  // ---------------------------------------------------------------------------
  // toEffect
  // ---------------------------------------------------------------------------

  describe("toEffect", () => {
    it("returns the underlying Effect", async () => {
      const pipeline = Pipeline.succeed(42);
      const effect = pipeline.toEffect();
      const result = await Effect.runPromise(effect);
      expect(result).toBe(42);
    });
  });

  // ---------------------------------------------------------------------------
  // PipelineDefaults
  // ---------------------------------------------------------------------------

  describe("PipelineDefaults", () => {
    it("retry uses defaults.retryWhen when no explicit when provided", async () => {
      let attempts = 0;
      const defaults: PipelineDefaults<TestError | OtherError> = {
        retryWhen: (err) => err._tag === "TestError", // only retry TestError
      };

      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: "transient" }))
            : Effect.succeed("ok");
        }),
        { defaults },
      )
        .retry({ maxRetries: 5, baseDelayMs: 1 })
        .runPromise();

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });

    it("retry with defaults does NOT retry errors excluded by retryWhen", async () => {
      let attempts = 0;
      const defaults: PipelineDefaults<TestError | OtherError> = {
        retryWhen: (err) => err._tag === "TestError", // only retry TestError
      };

      const { error } = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return Effect.fail(new OtherError({ code: 404 }));
        }) as Effect.Effect<string, TestError | OtherError>,
        { defaults },
      )
        .retry({ maxRetries: 3, baseDelayMs: 1 })
        .runSafe();

      expect(error?._tag).toBe("OtherError");
      expect(attempts).toBe(1); // no retries for OtherError
    });

    it("defaults propagate through map/tap chain", async () => {
      let attempts = 0;
      const defaults: PipelineDefaults<TestError> = {
        retryWhen: () => true,
      };

      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: "fail" }))
            : Effect.succeed(10);
        }),
        { defaults },
      )
        .map((n) => n * 2)
        .tap(() => {})
        .retry({ maxRetries: 5, baseDelayMs: 1 })
        .runPromise();

      expect(result).toBe(20);
      expect(attempts).toBe(3);
    });

    it("explicit when overrides defaults.retryWhen", async () => {
      let attempts = 0;
      const defaults: PipelineDefaults<TestError> = {
        retryWhen: () => true, // default: retry everything
      };

      const { error } = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return Effect.fail(new TestError({ message: "fail" }));
        }),
        { defaults },
      )
        .retry({ maxRetries: 3, baseDelayMs: 1, when: () => false }) // override: retry nothing
        .runSafe();

      expect(error?._tag).toBe("TestError");
      expect(attempts).toBe(1); // explicit when=false prevents retries
    });
  });

  // ---------------------------------------------------------------------------
  // PipelineResult ADT
  // ---------------------------------------------------------------------------

  describe("PipelineResult", () => {
    it("success creates a Success result", () => {
      const result = PipelineResult.success(42);
      expect(result._tag).toBe("success");
      expect(result.value).toBe(42);
    });

    it("typedError creates a TypedError result", () => {
      const err = new TestError({ message: "boom" });
      const result = PipelineResult.typedError(err);
      expect(result._tag).toBe("typedError");
      expect(result.error).toBe(err);
    });

    it("defect creates a Defect result", () => {
      const err = new Error("crash");
      const result = PipelineResult.defect(err);
      expect(result._tag).toBe("defect");
      expect(result.error).toBe(err);
    });

    it("isSuccess type guard works", () => {
      const s = PipelineResult.success(1);
      const e = PipelineResult.typedError(new TestError({ message: "" }));
      expect(PipelineResult.isSuccess(s)).toBe(true);
      expect(PipelineResult.isSuccess(e)).toBe(false);
    });

    it("isTypedError type guard works", () => {
      const s = PipelineResult.success(1);
      const e = PipelineResult.typedError(new TestError({ message: "" }));
      expect(PipelineResult.isTypedError(e)).toBe(true);
      expect(PipelineResult.isTypedError(s)).toBe(false);
    });

    it("isDefect type guard works", () => {
      const d = PipelineResult.defect(new Error("crash"));
      const s = PipelineResult.success(1);
      expect(PipelineResult.isDefect(d)).toBe(true);
      expect(PipelineResult.isDefect(s)).toBe(false);
    });
  });

  // ---------------------------------------------------------------------------
  // allSettled with Either inspection
  // ---------------------------------------------------------------------------

  describe("allSettled detailed", () => {
    it("returns Right for success and Left for failure", async () => {
      const results = await Pipeline.allSettled(
        Pipeline.succeed(42),
        Pipeline.fail(new TestError({ message: "boom" })),
      ).runPromise();

      expect(Either.isRight(results[0])).toBe(true);
      expect(Either.isLeft(results[1])).toBe(true);
      if (Either.isRight(results[0])) expect(results[0].right).toBe(42);
      if (Either.isLeft(results[1])) expect(results[1].left._tag).toBe("TestError");
    });
  });

  // ---------------------------------------------------------------------------
  // New methods
  // ---------------------------------------------------------------------------

  describe("flatMapAsync", () => {
    it("chains a Promise-returning function", async () => {
      const result = await Pipeline.succeed(5)
        .flatMapAsync(async (n) => n * 10)
        .runPromise();
      expect(result).toBe(50);
    });
  });

  describe("retry shorthand", () => {
    it("accepts a number as shorthand for maxRetries", async () => {
      let attempts = 0;
      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: "fail" }))
            : Effect.succeed("ok");
        }),
      )
        .retry(5)
        .runPromise();

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });
  });

  describe("tapAsyncFork", () => {
    it("does not block the pipeline", async () => {
      let sideEffectDone = false;
      const start = performance.now();

      const result = await Pipeline.succeed(42)
        .tapAsyncFork(async () => {
          await new Promise((r) => setTimeout(r, 200));
          sideEffectDone = true;
        })
        .runPromise();

      const elapsed = performance.now() - start;
      expect(result).toBe(42);
      expect(elapsed).toBeLessThan(100); // didn't wait for the 200ms side-effect
      expect(sideEffectDone).toBe(false); // hasn't completed yet
    });
  });

  describe("tapFork", () => {
    it("forks a side-effect pipeline without blocking", async () => {
      let sideEffectDone = false;
      const start = performance.now();

      const result = await Pipeline.succeed(42)
        .tapFork(() =>
          Pipeline.fromPromise(async () => {
            await new Promise((r) => setTimeout(r, 200));
            sideEffectDone = true;
          }),
        )
        .runPromise();

      const elapsed = performance.now() - start;
      expect(result).toBe(42);
      expect(elapsed).toBeLessThan(100);
      expect(sideEffectDone).toBe(false);
    });
  });

  describe("Pipeline.fn", () => {
    it("is shorthand for fromPromise", async () => {
      const result = await Pipeline.fn(() => Promise.resolve(42)).runPromise();
      expect(result).toBe(42);
    });
  });

  describe("Pipeline.sleep", () => {
    it("waits for the given duration", async () => {
      const start = performance.now();
      await Pipeline.sleep(50).runPromise();
      expect(performance.now() - start).toBeGreaterThanOrEqual(40);
    });
  });

  describe("withSpan", () => {
    it("annotates without changing value", async () => {
      const result = await Pipeline.succeed(42).withSpan("test").runPromise();
      expect(result).toBe(42);
    });
  });

  describe("withTag", () => {
    it("annotates without changing value", async () => {
      const result = await Pipeline.succeed(42).withSpan("test").withTag("key", "val").runPromise();
      expect(result).toBe(42);
    });
  });

  describe("mapError", () => {
    it("transforms the error type", async () => {
      const { error } = await Pipeline.from(
        Effect.fail(new TestError({ message: "original" })) as Effect.Effect<string, TestError>,
      )
        .mapError((e) => new OtherError({ code: e.message.length }))
        .runSafe();

      expect(error?._tag).toBe("OtherError");
      if (error?._tag === "OtherError") expect(error.code).toBe(8);
    });
  });

  describe("tapPipeline", () => {
    it("runs a side-effect pipeline without changing the value", async () => {
      let sideEffect = "";
      const result = await Pipeline.succeed("hello")
        .tapPipeline(() =>
          Pipeline.fromPromise(async () => {
            sideEffect = "ran";
          }),
        )
        .runPromise();

      expect(result).toBe("hello");
      expect(sideEffect).toBe("ran");
    });
  });

  describe("delay", () => {
    it("delays execution", async () => {
      const start = performance.now();
      await Pipeline.succeed(42).delay(50).runPromise();
      expect(performance.now() - start).toBeGreaterThanOrEqual(40);
    });
  });

  describe("when", () => {
    it("executes when condition is true", async () => {
      const result = await Pipeline.succeed(42)
        .when(() => true)
        .runPromise();
      expect(result).toBe(42);
    });

    it("returns undefined when condition is false", async () => {
      const result = await Pipeline.succeed(42)
        .when(() => false)
        .runPromise();
      expect(result).toBeUndefined();
    });
  });

  describe("repeat", () => {
    it("repeats the pipeline N times", async () => {
      let count = 0;
      await Pipeline.fromPromise(async () => {
        count++;
        return count;
      })
        .repeat({ times: 3 })
        .runPromise();

      expect(count).toBe(4); // 1 initial + 3 repeats
    });
  });

  describe("validate", () => {
    it("accumulates all errors", async () => {
      // validate should not short-circuit — collect all errors
      const pipeline = Pipeline.validate(
        Pipeline.succeed(1),
        Pipeline.fail(new TestError({ message: "a" })),
        Pipeline.fail(new TestError({ message: "b" })),
      );

      const { error } = await pipeline.runSafe();
      // With validate mode, the error should exist
      expect(error).not.toBeNull();
    });
  });

  describe("tapCause", () => {
    it("inspects the cause on error", async () => {
      let sawCause = false;
      await Pipeline.from(
        Effect.fail(new TestError({ message: "oops" })) as Effect.Effect<string, TestError>,
      )
        .tapCause(() => {
          sawCause = true;
        })
        .runSafe();

      expect(sawCause).toBe(true);
    });
  });

  describe("onInterrupt", () => {
    it("runs cleanup only on interruption", async () => {
      let interrupted = false;
      // Normal completion should NOT trigger onInterrupt
      await Pipeline.succeed(42)
        .onInterrupt(() => {
          interrupted = true;
        })
        .runPromise();

      expect(interrupted).toBe(false);
    });
  });

  describe("jittered retry", () => {
    it("retry with jitter succeeds", async () => {
      let attempts = 0;
      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: "fail" }))
            : Effect.succeed("ok");
        }),
      )
        .retry({ maxRetries: 5, baseDelayMs: 1, jitter: true })
        .runPromise();

      expect(result).toBe("ok");
      expect(attempts).toBe(3);
    });

    it("retry with maxDelayMs caps backoff", async () => {
      let attempts = 0;
      const result = await Pipeline.from(
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(new TestError({ message: "fail" }))
            : Effect.succeed("ok");
        }),
      )
        .retry({ maxRetries: 5, baseDelayMs: 1, maxDelayMs: 10 })
        .runPromise();

      expect(result).toBe("ok");
    });

    it("retry with timeBudgetMs stops after budget", async () => {
      const { error } = await Pipeline.from(Effect.fail(new TestError({ message: "always fails" })))
        .retry({ maxRetries: 100, baseDelayMs: 50, timeBudgetMs: 100 })
        .runSafe();

      expect(error?._tag).toBe("TestError");
    });
  });
});
