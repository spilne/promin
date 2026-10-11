// ---------------------------------------------------------------------------
// Anthropic adapter — rate-limit-hint plumbing.
// Injects a fetch that answers with the rate-limit headers Anthropic
// publishes; verifies the adapter projects them onto the protocol-level
// `LLMResponse.rateLimitHint`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { anthropic } from "../anthropic.ts";
import type { LLMProvider } from "../../llm-provider.ts";

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

function llmAnswering(headers: Record<string, string>): LLMProvider {
  return anthropic("claude-sonnet-4-6", {
    apiKey: "test-key",
    fetch: async () => jsonResponse(fakeAnthropicBody(), headers),
  });
}

describe("anthropic — chat() rateLimitHint", () => {
  it("populates rateLimitHint from x-ratelimit-* response headers", async () => {
    const reset = "2026-05-03T20:00:00Z";
    const llm = llmAnswering({
      "x-ratelimit-remaining-tokens": "12345",
      "x-ratelimit-remaining-requests": "42",
      "x-ratelimit-reset-tokens": reset,
    });
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });

    expect(res.rateLimitHint).toBeDefined();
    expect(res.rateLimitHint!.remainingTokens).toBe(12345);
    expect(res.rateLimitHint!.remainingRequests).toBe(42);
    expect(res.rateLimitHint!.resetsAt).toBe(Date.parse(reset));
  });

  it("falls back to reset-requests when reset-tokens is missing", async () => {
    const reset = "2026-05-03T20:30:00Z";
    const llm = llmAnswering({
      "x-ratelimit-remaining-tokens": "5000",
      "x-ratelimit-reset-requests": reset,
    });
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });

    expect(res.rateLimitHint!.resetsAt).toBe(Date.parse(reset));
  });

  it("omits rateLimitHint entirely when no rate-limit headers are present", async () => {
    const llm = llmAnswering({});
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });
    expect(res.rateLimitHint).toBeUndefined();
  });

  it("ignores malformed headers (non-numeric token counts, unparseable dates)", async () => {
    const llm = llmAnswering({
      "x-ratelimit-remaining-tokens": "not-a-number",
      "x-ratelimit-reset-tokens": "obviously-not-a-date",
    });
    const res = await llm.chat({ messages: [{ role: "user", content: "hi" }] });
    // All three fields stripped → hint is undefined entirely.
    expect(res.rateLimitHint).toBeUndefined();
  });
});
