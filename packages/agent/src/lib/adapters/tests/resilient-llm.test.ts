// ---------------------------------------------------------------------------
// resilientLLM — verifies retry / fallback semantics by injecting a
// scripted LLMProvider that throws on demand.
//
// Pins:
//   - chat: transient error → retry, then succeed
//   - chat: rate_limit exhausted + no fallback → throws
//   - chat: rate_limit exhausted + fallback → switches
//   - chat: context_overflow → fallback (one shot)
//   - chat: permanent → propagates immediately, no retry
//   - chatStream: error before first chunk → invisible retry
//   - chatStream: error after partial → throws by default; retries when retryAfterPartial set
//   - metrics: llm.retries + llm.fallback.switches counters fire
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { resilientLLM } from "../resilient-llm.ts";
import { classifyLLMError } from "../llm-error-classification.ts";
import { InMemoryAgentMetrics } from "../../metrics/types.ts";
import type {
  LLMChatParams,
  LLMProvider,
  LLMResponse,
  LLMStreamChunk,
} from "../../llm-provider.ts";

const OK: LLMResponse = { content: "ok", finishReason: "stop" };

function llmThatThrows(...errors: Array<Error | undefined>): {
  llm: LLMProvider;
  callCount: () => number;
} {
  let i = 0;
  return {
    llm: {
      chat: async (_p: LLMChatParams): Promise<LLMResponse> => {
        const err = errors[i++];
        if (err) throw err;
        return OK;
      },
    },
    callCount: () => i,
  };
}

describe("classifyLLMError", () => {
  it("recognises transient network errors", () => {
    expect(classifyLLMError(Object.assign(new TypeError("fetch failed"), {}))).toBe("transient");
  });
  it("recognises rate limits via 429 + message", () => {
    expect(classifyLLMError(Object.assign(new Error("HTTP 429 Too Many Requests"), {}))).toBe(
      "rate_limit",
    );
    expect(classifyLLMError(Object.assign(new Error("Rate limit exceeded"), {}))).toBe(
      "rate_limit",
    );
  });
  it("recognises context overflow", () => {
    expect(classifyLLMError(new Error("prompt is too long: 250000 tokens"))).toBe(
      "context_overflow",
    );
    expect(classifyLLMError(new Error("maximum context length"))).toBe("context_overflow");
  });
  it("recognises 5xx as transient", () => {
    expect(classifyLLMError(Object.assign(new Error("status 503 service unavailable"), {}))).toBe(
      "transient",
    );
  });
  it("recognises Anthropic 'overloaded'", () => {
    expect(classifyLLMError(new Error("API overloaded, try again"))).toBe("transient");
  });
  it("falls back to permanent for unknown errors", () => {
    expect(classifyLLMError(new Error("invalid api key"))).toBe("permanent");
  });
  it("AbortError is not retried", () => {
    const e = new Error("aborted");
    e.name = "AbortError";
    expect(classifyLLMError(e)).toBe("permanent");
  });
  it("status property on the error object beats the message", () => {
    const e = Object.assign(new Error("opaque"), { status: 429 });
    expect(classifyLLMError(e)).toBe("rate_limit");
  });
});

describe("resilientLLM.chat — retry semantics", () => {
  const transient = () => Object.assign(new Error("status 503 service unavailable"), {});
  const rateLimit = () => Object.assign(new Error("HTTP 429 rate limit"), {});
  const overflow = () => new Error("prompt is too long");
  const permanent = () => new Error("invalid api key");

  it("transient error → retry, then succeed", async () => {
    const { llm, callCount } = llmThatThrows(transient(), undefined);
    const wrapped = resilientLLM(llm, { transientRetries: 3, backoffMs: () => 0 });
    const r = await wrapped.chat({ messages: [] });
    expect(r.content).toBe("ok");
    expect(callCount()).toBe(2);
  });

  it("transient retries exhausted → throws (no fallback)", async () => {
    const { llm } = llmThatThrows(transient(), transient(), transient(), transient());
    const wrapped = resilientLLM(llm, { transientRetries: 2, backoffMs: () => 0 });
    expect(wrapped.chat({ messages: [] })).rejects.toThrow();
  });

  it("rate_limit exhausted + fallback → switches to fallback", async () => {
    const primary = llmThatThrows(rateLimit(), rateLimit(), rateLimit());
    const fallback: LLMProvider = {
      chat: async () => ({ content: "fallback ok", finishReason: "stop" }),
    };
    const wrapped = resilientLLM(primary.llm, {
      transientRetries: 1,
      rateLimitRetries: 1,
      fallback,
      backoffMs: () => 0,
    });
    const r = await wrapped.chat({ messages: [] });
    expect(r.content).toBe("fallback ok");
  });

  it("context_overflow → switches to fallback immediately (no retry)", async () => {
    const primary = llmThatThrows(overflow());
    const fallback: LLMProvider = {
      chat: async () => ({ content: "bigger window ok", finishReason: "stop" }),
    };
    const wrapped = resilientLLM(primary.llm, {
      transientRetries: 5,
      fallback,
      backoffMs: () => 0,
    });
    const r = await wrapped.chat({ messages: [] });
    expect(r.content).toBe("bigger window ok");
    expect(primary.callCount()).toBe(1); // single primary attempt
  });

  it("permanent → propagates immediately, no retry", async () => {
    const { llm, callCount } = llmThatThrows(permanent());
    const wrapped = resilientLLM(llm, { transientRetries: 5, backoffMs: () => 0 });
    expect(wrapped.chat({ messages: [] })).rejects.toThrow("invalid api key");
    // Single call — no retries on permanent.
    setTimeout(() => expect(callCount()).toBe(1), 0);
  });

  it("metrics fire on retry + fallback", async () => {
    const metrics = new InMemoryAgentMetrics();
    const primary = llmThatThrows(transient(), rateLimit(), rateLimit());
    const fallback: LLMProvider = { chat: async () => OK };
    const wrapped = resilientLLM(primary.llm, {
      transientRetries: 1,
      rateLimitRetries: 1,
      fallback,
      backoffMs: () => 0,
      metrics,
      metricLabels: { agent: "writer" },
    });
    await wrapped.chat({ messages: [] });
    // 1 transient retry, 1 rate-limit retry, 1 fallback switch.
    expect(metrics.counterTotal("llm.retries")).toBe(2);
    expect(metrics.counterTotal("llm.fallback.switches")).toBe(1);
    const reasons = metrics
      .counterSamples("llm.retries")
      .map((s) => s.labels.reason)
      .sort();
    expect(reasons).toEqual(["rate_limit", "transient"]);
  });
});

describe("resilientLLM.chatStream — partial-stream semantics", () => {
  function streamingProvider(scripts: LLMStreamChunk[][]): {
    llm: LLMProvider;
    attempts: () => number;
  } {
    let i = 0;
    return {
      llm: {
        chat: async () => OK,
        chatStream: async function* (_p) {
          const script = scripts[i++];
          if (!script) throw new Error("no more scripts");
          for (const chunk of script) {
            if (chunk.delta === "<<error>>") throw Object.assign(new Error("HTTP 503"), {});
            if (chunk.delta === "<<perm>>") throw new Error("invalid api key");
            yield chunk;
          }
        },
      },
      attempts: () => i,
    };
  }

  it("error before first chunk → invisible retry; caller sees clean stream", async () => {
    const p = streamingProvider([
      [{ delta: "<<error>>" }],
      [{ delta: "hello" }, { delta: " world", finishReason: "stop" }],
    ]);
    const wrapped = resilientLLM(p.llm, { transientRetries: 2, backoffMs: () => 0 });
    const out: string[] = [];
    for await (const c of wrapped.chatStream!({ messages: [] })) {
      if (c.delta) out.push(c.delta);
    }
    expect(out.join("")).toBe("hello world");
    expect(p.attempts()).toBe(2);
  });

  it("error after partial chunks → throws by default", async () => {
    const p = streamingProvider([[{ delta: "hel" }, { delta: "<<error>>" }]]);
    const wrapped = resilientLLM(p.llm, { transientRetries: 2, backoffMs: () => 0 });
    await expect(async () => {
      for await (const _ of wrapped.chatStream!({ messages: [] })) {
        /* drain */
      }
    }).toThrow();
  });

  it("error after partial → retries when retryAfterPartial enabled", async () => {
    const p = streamingProvider([
      [{ delta: "hel" }, { delta: "<<error>>" }],
      [{ delta: "hello", finishReason: "stop" }],
    ]);
    const wrapped = resilientLLM(p.llm, {
      transientRetries: 2,
      retryAfterPartial: true,
      backoffMs: () => 0,
    });
    const out: string[] = [];
    for await (const c of wrapped.chatStream!({ messages: [] })) {
      if (c.delta) out.push(c.delta);
    }
    // Caller sees both attempts' deltas — that's the cost of replay.
    expect(out.join("")).toBe("helhello");
    expect(p.attempts()).toBe(2);
  });

  it("permanent error never retries even if before first chunk", async () => {
    const p = streamingProvider([[{ delta: "<<perm>>" }]]);
    const wrapped = resilientLLM(p.llm, { transientRetries: 5, backoffMs: () => 0 });
    await expect(async () => {
      for await (const _ of wrapped.chatStream!({ messages: [] })) {
        /* drain */
      }
    }).toThrow("invalid api key");
    expect(p.attempts()).toBe(1);
  });
});
