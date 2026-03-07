/**
 * Pipeline basics — lazy, composable async operations
 *
 * Pipeline<T, E> wraps Effect<T, E> with a fluent API.
 * Nothing executes until a terminal (.runPromise(), .runSafe()).
 */

import { Data } from "effect";
import { Pipeline } from "@promin/core";

class FetchError extends Data.TaggedError("FetchError")<{
  readonly message: string;
}> {}

// Succeed and fail
async function succeedAndFail() {
  const ok = await Pipeline.succeed(42).runPromise();
  console.log(ok); // 42

  const { data, error } = await Pipeline.fail(
    new FetchError({ message: "not found" }),
  ).runSafe();
  console.log(data, error); // null, FetchError
}

// From promises — wrap existing async code
async function fromPromise() {
  const result = await Pipeline.fromPromise(async () => {
    const res = await fetch("https://httpbin.org/json");
    return res.json();
  }).runPromise();

  console.log(result);
}

// Map, flatMap — transform values
async function transformations() {
  const result = await Pipeline.succeed("hello")
    .map((s) => s.toUpperCase())
    .map((s) => s.length)
    .runPromise();

  console.log(result); // 5

  // flatMap — chain pipelines
  const chained = await Pipeline.succeed(5)
    .flatMap((n) => Pipeline.succeed(n * 2))
    .flatMap((n) => Pipeline.succeed(`Result: ${n}`))
    .runPromise();

  console.log(chained); // "Result: 10"
}

// Parallel execution
async function parallel() {
  const [a, b, c] = await Pipeline.all(
    Pipeline.succeed(1),
    Pipeline.succeed(2),
    Pipeline.succeed(3),
  ).runPromise();

  console.log(a, b, c); // 1 2 3
}

// Race — first to complete wins, others are cancelled
async function racing() {
  const result = await Pipeline.race(
    Pipeline.fromPromise(async () => {
      await new Promise((r) => setTimeout(r, 100));
      return "slow";
    }),
    Pipeline.fromPromise(async () => {
      await new Promise((r) => setTimeout(r, 10));
      return "fast";
    }),
  ).runPromise();

  console.log(result); // "fast"
}

// Timeout
async function timeout() {
  const { error } = await Pipeline.fromPromise(async () => {
    await new Promise((r) => setTimeout(r, 5000));
    return "done";
  })
    .timeout(100)
    .runSafe();

  console.log("Timed out:", error !== null); // true
}

export { succeedAndFail, fromPromise, transformations, parallel, racing, timeout };
