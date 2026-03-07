/**
 * flow() — non-durable workflow composition
 *
 * Same API as workflow() but no persistence, no workflowId needed.
 * Great for request handlers, scripts, and one-off compositions.
 */

import { flow, Pipeline } from "@promin/core";

// Linear chain — each step depends on the previous
async function linearChain() {
  const result = await flow<{ name: string; age: number }>("greet")
    .step("format", ({ input }) => Pipeline.succeed(`${input.name} is ${input.age}`))
    .step("uppercase", ({ prev }) => Pipeline.succeed(prev.toUpperCase()))
    .execute({ name: "Alice", age: 30 });

  console.log(result); // "ALICE IS 30"
}

// Async steps — when you don't need Pipeline's error channel
async function asyncSteps() {
  const result = await flow<{ url: string }>("fetch")
    .stepAsync("download", async ({ input }) => {
      const res = await fetch(input.url);
      return res.text();
    })
    .step("count", ({ prev }) => Pipeline.succeed(prev.length))
    .execute({ url: "https://example.com" });

  console.log(`Downloaded ${result} bytes`);
}

// Safe execution — returns { data, error } instead of throwing
async function safeExecution() {
  const { data, error } = await flow<string>("risky")
    .step("might-fail", ({ input }) => {
      if (input === "bad") return Pipeline.fail(new Error("bad input") as never);
      return Pipeline.succeed(input.toUpperCase());
    })
    .executeSafe("bad");

  if (error) {
    console.log("Failed:", error.message);
  } else {
    console.log("Success:", data);
  }
}

export { linearChain, asyncSteps, safeExecution };
