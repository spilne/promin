// ---------------------------------------------------------------------------
// Retry classification tests for journaled activities.
//
// Three levers control whether the retry loop re-attempts a failed activity:
//   1. TerminalError   — always stops the loop; `retry.when` isn't consulted.
//   2. RetryableError  — always forces retry; `retry.when` isn't consulted.
//   3. RetryPolicy.when (from @promin/core) — the predicate on the retry
//      config itself. Consulted for any other error; return false to bail.
//
// Point (3) intentionally reuses the existing `when` from RetryPolicy rather
// than adding a duplicate top-level option. The class-based short-circuits
// take precedence so a lax `when` can't resurrect a TerminalError and a
// strict one can't skip a RetryableError.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { runJournaledStep } from "./journaled-step.ts";
import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { RetryableError, TerminalError } from "./durable-pipeline-error.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Keeps a ledger of attempt counts so tests can assert precisely. */
function attemptCounter() {
  const box = { count: 0 };
  return {
    box,
    bump() {
      box.count++;
    },
  };
}

const fastRetry = { maxRetries: 3, baseDelayMs: 0, jitter: false } as const;

// ---------------------------------------------------------------------------
// Default behaviour — retry everything up to maxRetries
// ---------------------------------------------------------------------------

describe("activity retry — default (no classification)", () => {
  it("retries all errors until maxRetries, then surfaces the last one", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-default",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "flaky",
            async () => {
              a.bump();
              throw new Error("boom");
            },
            { retry: fastRetry },
          );
        },
      }),
    ).rejects.toThrow("boom");

    expect(a.box.count).toBe(fastRetry.maxRetries + 1); // initial + retries
  });
});

// ---------------------------------------------------------------------------
// TerminalError — never retried, regardless of predicate or retry policy
// ---------------------------------------------------------------------------

describe("activity retry — TerminalError", () => {
  it("short-circuits retry loop on TerminalError", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-terminal",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "validate",
            async () => {
              a.bump();
              throw new TerminalError({ message: "schema violation" });
            },
            { retry: fastRetry },
          );
        },
      }),
    ).rejects.toBeInstanceOf(TerminalError);

    expect(a.box.count).toBe(1); // ran once, no retries
  });

  it("TerminalError wins even when `retry.when` would return true", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-term-over-pred",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "validate",
            async () => {
              a.bump();
              throw new TerminalError({ message: "nope" });
            },
            {
              retry: {
                ...fastRetry,
                when: () => true, // predicate says "retry" — Terminal still wins
              },
            },
          );
        },
      }),
    ).rejects.toBeInstanceOf(TerminalError);

    expect(a.box.count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// RetryableError — always retried, regardless of predicate
// ---------------------------------------------------------------------------

describe("activity retry — RetryableError", () => {
  it("forces retry even when `retry.when` would return false", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-forced-retry",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "fetch",
            async () => {
              a.bump();
              throw new RetryableError({ message: "transient" });
            },
            {
              retry: {
                ...fastRetry,
                when: () => false, // predicate says "don't retry" — Retryable wins
              },
            },
          );
        },
      }),
    ).rejects.toBeInstanceOf(RetryableError);

    expect(a.box.count).toBe(fastRetry.maxRetries + 1);
  });

  it("eventually succeeds after a transient RetryableError", async () => {
    const storage = new InMemoryWorkflowStorage();
    let attempts = 0;

    const result = await runJournaledStep<unknown, unknown, number>({
      input: undefined,
      prev: undefined,
      workflowId: "wf-eventual-success",
      stepName: "s",
      storage,
      body: function* (ctx) {
        return yield* ctx.activity(
          "fetch",
          async () => {
            attempts++;
            if (attempts < 3) throw new RetryableError({ message: "503" });
            return 42;
          },
          { retry: fastRetry },
        );
      },
    });

    expect(result).toBe(42);
    expect(attempts).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// retry.when predicate — consulted for any other error type
// ---------------------------------------------------------------------------

class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValidationError";
  }
}

class ServiceUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ServiceUnavailable";
  }
}

describe("activity retry — retry.when predicate", () => {
  it("predicate returns false → skip retries", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-pred-false",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "charge",
            async () => {
              a.bump();
              throw new ValidationError("bad input");
            },
            {
              retry: {
                ...fastRetry,
                when: (err) => err instanceof ServiceUnavailable,
              },
            },
          );
        },
      }),
    ).rejects.toThrow("bad input");

    expect(a.box.count).toBe(1);
  });

  it("predicate returns true → retry up to maxRetries", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-pred-true",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "charge",
            async () => {
              a.bump();
              throw new ServiceUnavailable("503");
            },
            {
              retry: {
                ...fastRetry,
                when: (err) => err instanceof ServiceUnavailable,
              },
            },
          );
        },
      }),
    ).rejects.toThrow("503");

    expect(a.box.count).toBe(fastRetry.maxRetries + 1);
  });

  it("without a retry policy, any error propagates after one attempt", async () => {
    const storage = new InMemoryWorkflowStorage();
    const a = attemptCounter();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-no-retry",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity("charge", async () => {
            a.bump();
            throw new Error("boom");
          });
        },
      }),
    ).rejects.toThrow("boom");

    expect(a.box.count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Failure journaling — classification errors are recorded as Failure
// ---------------------------------------------------------------------------

describe("activity retry — journal Failure after exhausting retries", () => {
  it("records TerminalError in the journal after a single attempt", async () => {
    const storage = new InMemoryWorkflowStorage();

    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-terminal-journal",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "validate",
            async () => {
              throw new TerminalError({ message: "forbidden" });
            },
            { retry: fastRetry },
          );
        },
      }),
    ).rejects.toBeInstanceOf(TerminalError);

    const journal = await storage.loadJournal("wf-terminal-journal", "s");
    expect(journal).toHaveLength(1);
    expect(journal[0]!.phase).toBe("completed");
    expect(journal[0]!.exit?.tag).toBe("Failure");

    // Replay rethrows the recorded failure without re-running.
    let ran = false;
    await expect(
      runJournaledStep<unknown, unknown, number>({
        input: undefined,
        prev: undefined,
        workflowId: "wf-terminal-journal",
        stepName: "s",
        storage,
        body: function* (ctx) {
          return yield* ctx.activity(
            "validate",
            async () => {
              ran = true;
              return 1;
            },
            { retry: fastRetry },
          );
        },
      }),
    ).rejects.toThrow("forbidden");
    expect(ran).toBe(false);
  });
});
