/**
 * Step failure strategies, retry policies, and workflow-level retry
 *
 * Steps can retry on failure, skip, use fallback values, or make
 * dynamic decisions. Workflow-level retry re-runs from the failed step.
 */

import { Data } from "effect";
import { flow, workflow, Pipeline, InMemoryWorkflowStorage } from "@promin/core";

class HttpError extends Data.TaggedError("HttpError")<{
  readonly status: number;
  readonly message: string;
}> {}

class TimeoutError extends Data.TaggedError("TimeoutError")<{
  readonly message: string;
}> {}

// Step retry with exponential backoff
async function stepRetry() {
  let attempts = 0;

  const result = await flow<string>("retry-example")
    .step(
      "flaky-api",
      () => {
        attempts++;
        if (attempts < 3) return Pipeline.fail(new HttpError({ status: 503, message: "retry" }));
        return Pipeline.succeed("success");
      },
      {
        retry: { maxRetries: 5, baseDelayMs: 100 },
      },
    )
    .execute("x");

  console.log(result, `(took ${attempts} attempts)`); // "success (took 3 attempts)"
}

// Selective retry with `when` predicate
async function selectiveRetry() {
  const result = await flow<string>("selective-retry")
    .step(
      "api-call",
      (): Pipeline<string, HttpError | TimeoutError> => {
        return Pipeline.fail(new HttpError({ status: 400, message: "bad request" }));
      },
      {
        retry: {
          maxRetries: 3,
          when: (err) => {
            // Only retry 5xx errors, not 4xx
            if (err._tag === "HttpError") return (err as HttpError).status >= 500;
            // Always retry timeouts
            if (err._tag === "TimeoutError") return true;
            return false;
          },
        },
      },
    )
    .executeSafe("x");

  // 400 error → not retried → fails immediately
  console.log("Error:", result.error);
}

// onFailure: skip — continue with undefined
async function skipOnFailure() {
  const result = await flow<string>("skip-example")
    .step("optional", () => Pipeline.fail(new HttpError({ status: 404, message: "not found" })), {
      onFailure: "skip",
    })
    .step("continue", ({ prev }) => Pipeline.succeed(`Got: ${prev}`))
    .execute("x");

  console.log(result); // "Got: undefined"
}

// onFailure: fallback — use a default value
async function fallbackOnFailure() {
  const result = await flow<string>("fallback-example")
    .step("risky", () => Pipeline.fail(new HttpError({ status: 500, message: "down" })), {
      onFailure: { fallback: () => ({ name: "Unknown", score: 0 }) },
    })
    .step("use-it", ({ prev }) => Pipeline.succeed(`${prev.name}: ${prev.score}`))
    .execute("x");

  console.log(result); // "Unknown: 0"
}

// Workflow-level retry — re-runs from the failed step
async function workflowRetry() {
  const storage = new InMemoryWorkflowStorage();
  let step2Calls = 0;

  const result = await workflow<number>({
    name: "wf-retry",
    storage,
    retry: { maxRetries: 2, baseDelayMs: 10 },
  })
    .step("step-1", ({ input }) => Pipeline.succeed(input * 2)) // runs once, checkpointed
    .step("step-2", ({ prev }) => {
      step2Calls++;
      if (step2Calls < 2) {
        return Pipeline.fail(new HttpError({ status: 503, message: "transient" }));
      }
      return Pipeline.succeed(prev + 100);
    })
    .run({ workflowId: "retry-1", input: 5 });

  console.log(result); // 110 — step-1 (5*2=10) checkpointed, step-2 retried (10+100=110)
}

// Workflow retry with `when` predicate — skip retry for non-retryable errors
async function workflowRetryPredicate() {
  const storage = new InMemoryWorkflowStorage();

  const { error } = await workflow<string>({
    name: "wf-retry-when",
    storage,
    retry: {
      maxRetries: 3,
      when: (err: any) => err._tag === "HttpError" && err.status >= 500,
    },
  })
    .step("api", () => Pipeline.fail(new HttpError({ status: 400, message: "bad request" })))
    .runSafe({ workflowId: "retry-when-1", input: "x" });

  // 400 is not retryable → no workflow retry → fails immediately
  console.log("Failed with:", (error as any)?.message);
}

export {
  stepRetry,
  selectiveRetry,
  skipOnFailure,
  fallbackOnFailure,
  workflowRetry,
  workflowRetryPredicate,
};
