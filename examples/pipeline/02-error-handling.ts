/**
 * Error handling — typed errors, recovery, validation
 *
 * Pipeline tracks errors in the type system via the E parameter.
 * Recover from specific errors, accumulate validation errors,
 * or let them propagate.
 */

import { Data } from "effect";
import { Pipeline } from "@promin/core";

class HttpError extends Data.TaggedError("HttpError")<{
  readonly status: number;
  readonly message: string;
}> {}

class ValidationError extends Data.TaggedError("ValidationError")<{
  readonly field: string;
  readonly message: string;
}> {}

// orElse — provide a fallback value on any error
async function fallbackValue() {
  const fetchUser = (): Pipeline<string, HttpError> =>
    Pipeline.fail(new HttpError({ status: 404, message: "not found" }));

  const result = await fetchUser().orElse("fallback-value").runPromise();

  console.log(result); // "fallback-value"
}

// orElsePipeline — fallback to another pipeline
async function fallbackPipeline() {
  const primary = Pipeline.fail(new HttpError({ status: 503, message: "down" }));
  const backup = Pipeline.succeed("from backup");

  const result = await primary.orElsePipeline(() => backup).runPromise();

  console.log(result); // "from backup"
}

// recover — handle specific error types
async function recoverSpecific() {
  const result = await Pipeline.fail(new HttpError({ status: 404, message: "not found" }))
    .recover(
      (err) => err._tag === "HttpError" && (err as HttpError).status === 404,
      () => "default-value",
    )
    .runPromise();

  console.log(result); // "default-value"
}

// handleError — catch all typed errors
async function handleAllErrors() {
  const result = await Pipeline.fail(new HttpError({ status: 500, message: "server error" }))
    .handleError((err) => `Recovered from: ${err.message}`)
    .runPromise();

  console.log(result); // "Recovered from: server error"
}

// tapError — side effect on error without changing the pipeline
async function tapOnError() {
  const { error } = await Pipeline.fail(new HttpError({ status: 500, message: "boom" }))
    .tapError((err) => {
      console.log("Error occurred:", err.message); // "Error occurred: boom"
    })
    .runSafe();

  console.log("Still failed:", error !== null); // true
}

// validate — accumulate ALL errors instead of short-circuiting
async function accumulateErrors() {
  const validateTitle = (title: string) =>
    title.length > 0 && title.length <= 100
      ? Pipeline.succeed(title)
      : Pipeline.fail(new ValidationError({ field: "title", message: "1-100 chars required" }));

  const validateTags = (tags: string[]) =>
    tags.length <= 30
      ? Pipeline.succeed(tags)
      : Pipeline.fail(new ValidationError({ field: "tags", message: "Max 30 tags" }));

  const validateScore = (score: number) =>
    score >= 0 && score <= 100
      ? Pipeline.succeed(score)
      : Pipeline.fail(new ValidationError({ field: "score", message: "0-100 required" }));

  // All three run in parallel; errors are accumulated, not short-circuited
  const { data: _data, error } = await Pipeline.validate(
    validateTitle(""),
    validateTags(Array(50).fill("tag")),
    validateScore(150),
  ).runSafe();

  if (error) {
    console.log("Validation errors:", error);
    // Contains all 3 errors, not just the first
  }
}

// redeem — handle both success and failure in one operation
async function redeemBoth() {
  const result = await Pipeline.fail(new HttpError({ status: 404, message: "not found" }))
    .redeem(
      (err) => `Error: ${err.message}`,
      (data) => `Success: ${data}`,
    )
    .runPromise();

  console.log(result); // "Error: not found"
}

export {
  fallbackValue,
  fallbackPipeline,
  recoverSpecific,
  handleAllErrors,
  tapOnError,
  accumulateErrors,
  redeemBoth,
};
