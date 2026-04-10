import { describe, it, expect } from "bun:test";
import { Data } from "effect";
import { Pipeline } from "./pipeline.ts";

class ParseError extends Data.TaggedError("ParseError")<{
  readonly message: string;
}> {}

class ValidationError extends Data.TaggedError("ValidationError")<{
  readonly field: string;
}> {}

class AuthError extends Data.TaggedError("AuthError")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// trapError
// ---------------------------------------------------------------------------

describe("trapError", () => {
  it("catches a thrown tagged error and puts it in the E channel", async () => {
    const result = await Pipeline.succeed("bad json")
      .map((_x) => {
        throw new ParseError({ message: "invalid" });
      })
      .trapError(ParseError)
      .runSafe();

    expect(result.error).toBeInstanceOf(ParseError);
    expect((result.error as ParseError)._tag).toBe("ParseError");
  });

  it("does not catch unrelated defects", async () => {
    const pipeline = Pipeline.succeed("x")
      .map((_x) => {
        throw new Error("random bug");
      })
      .trapError(ParseError);

    await expect(pipeline.runPromise()).rejects.toThrow("random bug");
  });

  it("catches multiple error classes (variadic)", async () => {
    const result = await Pipeline.succeed("x")
      .map((_x) => {
        throw new ValidationError({ field: "email" });
      })
      .trapError(ParseError, ValidationError)
      .runSafe();

    expect(result.error).toBeInstanceOf(ValidationError);
    expect((result.error as ValidationError)._tag).toBe("ValidationError");
  });

  it("widens the error type with sequential calls", async () => {
    const pipeline = Pipeline.succeed("x")
      .map((_x) => {
        throw new ParseError({ message: "bad" });
      })
      .trapError(ParseError)
      .trapError(ValidationError);

    const result = await pipeline.runSafe();
    expect(result.error).toBeInstanceOf(ParseError);
  });

  it("works with handleError after trapping", async () => {
    const result = await Pipeline.succeed("x")
      .map((_x) => {
        throw new ParseError({ message: "bad" });
      })
      .trapError(ParseError)
      .handleError((e) => {
        if (e._tag === "ParseError") return "recovered";
        return "other";
      })
      .runPromise();

    expect(result).toBe("recovered");
  });

  it("works with recover after trapping", async () => {
    const result = await Pipeline.succeed("x")
      .map((_x) => {
        throw new ParseError({ message: "bad" });
      })
      .trapError(ParseError)
      .recover(
        (e) => e._tag === "ParseError",
        () => "fallback",
      )
      .runPromise();

    expect(result).toBe("fallback");
  });

  it("passes through successful values unchanged", async () => {
    const result = await Pipeline.succeed(42).trapError(ParseError).runPromise();
    expect(result).toBe(42);
  });

  it("passes through typed errors unchanged", async () => {
    const result = await Pipeline.fail(new AuthError({ message: "denied" }))
      .trapError(ParseError)
      .runSafe();

    expect(result.error).toBeInstanceOf(AuthError);
  });
});

// ---------------------------------------------------------------------------
// tapAnyError
// ---------------------------------------------------------------------------

describe("tapAnyError", () => {
  it("observes typed errors", async () => {
    const seen: unknown[] = [];
    await Pipeline.fail(new ParseError({ message: "bad" }))
      .tapAnyError((e) => seen.push(e))
      .runSafe();

    expect(seen).toHaveLength(1);
    expect(seen[0]).toBeInstanceOf(ParseError);
  });

  it("observes defects", async () => {
    const seen: unknown[] = [];
    const pipeline = Pipeline.succeed("x")
      .map((_x) => {
        throw new Error("boom");
      })
      .tapAnyError((e) => seen.push(e));

    await pipeline.runSafe({ catchAll: true });

    expect(seen).toHaveLength(1);
    expect(seen[0]).toBeInstanceOf(Error);
    expect((seen[0] as Error).message).toBe("boom");
  });

  it("does not change the error", async () => {
    const result = await Pipeline.fail(new ParseError({ message: "bad" }))
      .tapAnyError(() => {})
      .runSafe();

    expect(result.error).toBeInstanceOf(ParseError);
    expect((result.error as ParseError).message).toBe("bad");
  });

  it("does not affect successful values", async () => {
    const seen: unknown[] = [];
    const result = await Pipeline.succeed(42)
      .tapAnyError((e) => seen.push(e))
      .runPromise();

    expect(result).toBe(42);
    expect(seen).toHaveLength(0);
  });

  it("works with trapError together", async () => {
    const seen: unknown[] = [];
    const result = await Pipeline.succeed("x")
      .map((_x) => {
        throw new ParseError({ message: "invalid" });
      })
      .tapAnyError((e) => seen.push(e))
      .trapError(ParseError)
      .handleError(() => "recovered")
      .runPromise();

    expect(result).toBe("recovered");
    expect(seen).toHaveLength(1);
    expect(seen[0]).toBeInstanceOf(ParseError);
  });
});
