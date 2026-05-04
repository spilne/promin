// ---------------------------------------------------------------------------
// Anthropic adapter — rate-limit-hint plumbing.
// Mocks globalThis.fetch with a Response carrying the rate-limit headers
// Anthropic publishes; verifies the adapter projects them onto the
// protocol-level `LLMResponse.rateLimitHint`.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { anthropic } from "../anthropic.ts";

const ORIGINAL_FETCH = globalThis.fetch;

function jsonResponse(body: unknown, headers: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json", ...headers },
  });
}

function fakeAnthropicBody() {
  return {
    content: [{ type: "text", text: "ok" }],
    stop_reason: "end_turn",
    usage: { input_tokens: 10, output_tokens: 5 },
  };
}

describe("anthropic — chat() rateLimitHint", () => {
  beforeEach(() => {
    process.env["ANTHROPIC_API_KEY"] = "test-key";
  });
  afterEach(() => {
    globalThis.fetch = ORIGINAL_FETCH;
  });

  it("populates rateLimitHint from x-ratelimit-* response headers", async () => {
    const reset = "2026-05-03T20:00:00Z";
    globalThis.fetch = (async () =>
      jsonResponse(fakeAnthropicBody(), {
        "x-ratelimit-remaining-tokens": "12345",
        "x-ratelimit-remaining-requests": "42",
        "x-ratelimit-reset-tokens": reset,
      })) as typeof fetch;

    const llm = anthropic("claude-sonnet-4-6");
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });

    expect(res.rateLimitHint).toBeDefined();
    expect(res.rateLimitHint!.remainingTokens).toBe(12345);
    expect(res.rateLimitHint!.remainingRequests).toBe(42);
    expect(res.rateLimitHint!.resetsAt).toBe(Date.parse(reset));
  });

  it("falls back to reset-requests when reset-tokens is missing", async () => {
    const reset = "2026-05-03T20:30:00Z";
    globalThis.fetch = (async () =>
      jsonResponse(fakeAnthropicBody(), {
        "x-ratelimit-remaining-tokens": "5000",
        "x-ratelimit-reset-requests": reset,
      })) as typeof fetch;

    const llm = anthropic("claude-sonnet-4-6");
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });

    expect(res.rateLimitHint!.resetsAt).toBe(Date.parse(reset));
  });

  it("omits rateLimitHint entirely when no rate-limit headers are present", async () => {
    globalThis.fetch = (async () => jsonResponse(fakeAnthropicBody(), {})) as typeof fetch;
    const llm = anthropic("claude-sonnet-4-6");
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });
    expect(res.rateLimitHint).toBeUndefined();
  });

  it("ignores malformed headers (non-numeric token counts, unparseable dates)", async () => {
    globalThis.fetch = (async () =>
      jsonResponse(fakeAnthropicBody(), {
        "x-ratelimit-remaining-tokens": "not-a-number",
        "x-ratelimit-reset-tokens": "obviously-not-a-date",
      })) as typeof fetch;

    const llm = anthropic("claude-sonnet-4-6");
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });
    // All three fields stripped → hint is undefined entirely.
    expect(res.rateLimitHint).toBeUndefined();
  });
});
