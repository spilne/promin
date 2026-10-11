// ---------------------------------------------------------------------------
// LLM retry driven by classifyLLMError: transient and rate-limit failures
// are retried with backoff (or the server's Retry-After) on the injected
// clock; permanent ones and over-long Retry-After waits fail at once.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { agentAction } from "../agent-action.ts";
import { callLlmWithRetry } from "../agent-shared.ts";
import { LLMHttpError } from "../adapters/llm-http.ts";
import type { LLMProvider, LLMResponse } from "../llm-provider.ts";
import { collectingBus, freshRunner, until } from "./engine-harness.ts";

function httpError(status: number, retryAfterMs?: number): LLMHttpError {
  return new LLMHttpError({
    status,
    provider: "test",
    body: "{}",
    ...(retryAfterMs !== undefined && { retryAfterMs }),
  });
}

/** Fails with `errors` in order, then answers. */
function failingLLM(errors: ReadonlyArray<unknown>): LLMProvider & { calls: number } {
  const llm = {
    calls: 0,
    chat: async (): Promise<LLMResponse> => {
      llm.calls++;
      const err = errors[llm.calls - 1];
      if (err !== undefined) throw err;
      return { content: "ok", finishReason: "stop" };
    },
  };
  return llm;
}

describe("callLlmWithRetry", () => {
  it("retries a transient failure after exponential backoff on the clock", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const done = callLlmWithRetry({
      retry: { baseDelayMs: 100 },
      clock,
      run: async () => {
        calls++;
        if (calls < 3) throw httpError(503);
        return "ok";
      },
    });
    await until(() => clock.pendingCount() === 1);
    clock.advance(99);
    expect(calls).toBe(1);
    clock.advance(1);
    await until(() => calls === 2 && clock.pendingCount() === 1);
    clock.advance(200);
    expect(await done).toBe("ok");
    expect(calls).toBe(3);
  });

  it("waits the server's Retry-After on a rate limit", async () => {
    const clock = FakeWallClock.create(0);
    let calls = 0;
    const done = callLlmWithRetry({
      retry: { baseDelayMs: 10 },
      clock,
      run: async () => {
        calls++;
        if (calls === 1) throw httpError(429, 5_000);
        return "ok";
      },
    });
    await until(() => clock.pendingCount() === 1);
    clock.advance(4_999);
    expect(calls).toBe(1);
    clock.advance(1);
    expect(await done).toBe("ok");
  });

  it("fails at once on a permanent error", async () => {
    let calls = 0;
    await expect(
      callLlmWithRetry({
        retry: {},
        clock: FakeWallClock.create(0),
        run: async () => {
          calls++;
          throw httpError(401);
        },
      }),
    ).rejects.toThrow("401");
    expect(calls).toBe(1);
  });

  it("fails when the server asks to wait longer than maxRetryAfterMs", async () => {
    let calls = 0;
    await expect(
      callLlmWithRetry({
        retry: { maxRetryAfterMs: 1_000 },
        clock: FakeWallClock.create(0),
        run: async () => {
          calls++;
          throw httpError(429, 60_000);
        },
      }),
    ).rejects.toThrow("429");
    expect(calls).toBe(1);
  });

  it("gives up after maxRetries", async () => {
    let calls = 0;
    await expect(
      callLlmWithRetry({
        retry: { maxRetries: 2, baseDelayMs: 0 },
        clock: FakeWallClock.create(0),
        run: async () => {
          calls++;
          throw httpError(500);
        },
      }),
    ).rejects.toThrow("500");
    expect(calls).toBe(3);
  });
});

describe("agentAction — LLM retry", () => {
  it("retries a transient LLM failure inside the think step and emits llm.retry", async () => {
    const { runner } = freshRunner();
    const { bus, events } = collectingBus();
    const llm = failingLLM([httpError(529)]);
    const result = await runner.run({
      workflow: agentAction({
        name: "retry",
        llm,
        bus,
        llmRetry: { baseDelayMs: 0 },
      }),
      workflowId: "retry-1",
      input: { task: "hi" },
    });
    expect(result.answer).toBe("ok");
    expect(llm.calls).toBe(2);
    expect(events.find((e) => e.type === "llm.retry")).toMatchObject({
      step: 0,
      attempt: 1,
      errorClass: "transient",
    });
  });

  it("fails the turn on a permanent LLM error without retrying", async () => {
    const { runner } = freshRunner();
    const llm = failingLLM([httpError(400)]);
    const { error } = await runner.runSafe({
      workflow: agentAction({ name: "permanent", llm, llmRetry: { baseDelayMs: 0 } }),
      workflowId: "permanent-1",
      input: { task: "hi" },
    });
    expect(String(error)).toContain("400");
    expect(llm.calls).toBe(1);
  });

  it("llmRetry: false runs the call once", async () => {
    const { runner } = freshRunner();
    const llm = failingLLM([httpError(503)]);
    const { error } = await runner.runSafe({
      workflow: agentAction({ name: "no-retry", llm, llmRetry: false }),
      workflowId: "no-retry-1",
      input: { task: "hi" },
    });
    expect(error).toBeTruthy();
    expect(llm.calls).toBe(1);
  });
});

describe("agentAction — cancel during retry backoff", () => {
  it("ends the turn as cancelled instead of failing with the retried error", async () => {
    const clock = FakeWallClock.create(0);
    const controller = new AbortController();
    const llm = failingLLM([httpError(503), httpError(503), httpError(503), httpError(503)]);
    const { runner } = freshRunner();
    const run = runner.runSafe({
      workflow: agentAction({ name: "cancel-backoff", llm, clock, signal: controller.signal }),
      workflowId: "cancel-backoff-1",
      input: { task: "hi" },
    });
    await until(() => clock.pendingCount() > 0);
    controller.abort();
    const { data, error } = await run;
    expect(error).toBeNull();
    expect(data?.finishReason).toBe("cancelled");
    expect(llm.calls).toBe(1);
  });
});
