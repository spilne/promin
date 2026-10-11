// ---------------------------------------------------------------------------
// Anthropic adapter — message conversion, response parsing, SSE error
// paths, request timeouts and typed errors, all against recorded responses.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { anthropic } from "../anthropic.ts";
import { classifyLLMError } from "../llm-error-classification.ts";
import { LLMHttpError, LLMStreamInterruptedError, LLMTimeoutError } from "../llm-http.ts";
import type { Message } from "../../message.ts";
import {
  collect,
  fakeFetch,
  hangingResponse,
  jsonResponse,
  sseResponse,
  waitFor,
} from "./fake-fetch.ts";

const OK_BODY = {
  content: [{ type: "text", text: "ok" }],
  stop_reason: "end_turn",
  usage: { input_tokens: 1, output_tokens: 1 },
};

const USER: Message = { role: "user", content: "hi" };

function streamOf(...blocks: unknown[]): unknown[] {
  return [
    { type: "message_start", message: { usage: { input_tokens: 3 } } },
    ...blocks,
    { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } },
    { type: "message_stop" },
  ];
}

describe("anthropic — message conversion", () => {
  it("leaves out assistant turns with no text and no tool calls", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(OK_BODY));
    const llm = anthropic("m", { apiKey: "k", fetch });
    await llm.chat({
      messages: [
        USER,
        { role: "assistant", content: null },
        { role: "user", content: "again" },
        { role: "assistant", content: "   " },
        { role: "user", content: "and again" },
      ],
    });
    const sent = requests[0]!.body["messages"] as Array<{ role: string }>;
    expect(sent.map((m) => m.role)).toEqual(["user", "user", "user"]);
  });

  it("sends a tool-call-only assistant turn without an empty text block", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(OK_BODY));
    const llm = anthropic("m", { apiKey: "k", fetch });
    await llm.chat({
      messages: [
        USER,
        { role: "assistant", content: "", toolCalls: [{ id: "t1", name: "x", input: {} }] },
        { role: "tool", toolCallId: "t1", content: "done" },
      ],
    });
    const sent = requests[0]!.body["messages"] as Array<{ role: string; content: unknown }>;
    expect(sent[1]).toEqual({
      role: "assistant",
      content: [{ type: "tool_use", id: "t1", name: "x", input: {} }],
    });
  });

  it("raises max_tokens above the thinking budget when the default would not fit", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(OK_BODY));
    const llm = anthropic("m", { apiKey: "k", fetch });
    await llm.chat({ messages: [USER], thinkingBudgetTokens: 10_000 });
    await llm.chat({ messages: [USER], thinkingBudgetTokens: 10_000, maxTokens: 20_000 });
    await llm.chat({ messages: [USER] });
    expect(requests[0]!.body["max_tokens"]).toBe(14_096);
    expect(requests[1]!.body["max_tokens"]).toBe(20_000);
    expect(requests[2]!.body["max_tokens"]).toBe(4096);
  });
});

describe("anthropic — chat() response parsing", () => {
  it("concatenates every text block", async () => {
    const { fetch } = fakeFetch(() =>
      jsonResponse({
        ...OK_BODY,
        content: [
          { type: "text", text: "Hello, " },
          { type: "text", text: "world" },
        ],
      }),
    );
    const res = await anthropic("m", { apiKey: "k", fetch }).chat({ messages: [USER] });
    expect(res.content).toBe("Hello, world");
  });

  it("throws LLMHttpError with status and Retry-After on a 429", async () => {
    const { fetch } = fakeFetch(() =>
      jsonResponse(
        { type: "error", error: { type: "rate_limit_error", message: "slow down" } },
        { status: 429, headers: { "retry-after": "7" } },
      ),
    );
    const err = await anthropic("m", { apiKey: "k", fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LLMHttpError);
    expect((err as LLMHttpError).status).toBe(429);
    expect((err as LLMHttpError).retryAfterMs).toBe(7_000);
    expect((err as LLMHttpError).provider).toBe("anthropic");
    expect((err as LLMHttpError).body).toContain("rate_limit_error");
    expect(classifyLLMError(err)).toBe("rate_limit");
  });

  it("classifies a 500 api_error as transient and a 401 as permanent", async () => {
    const status500 = fakeFetch(() =>
      jsonResponse({ type: "error", error: { type: "api_error", message: "x" } }, { status: 500 }),
    );
    const err500 = await anthropic("m", { apiKey: "k", fetch: status500.fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect(classifyLLMError(err500)).toBe("transient");

    const status401 = fakeFetch(() =>
      jsonResponse(
        { type: "error", error: { type: "authentication_error", message: "bad key" } },
        { status: 401 },
      ),
    );
    const err401 = await anthropic("m", { apiKey: "k", fetch: status401.fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect(classifyLLMError(err401)).toBe("permanent");
  });
});

describe("anthropic — chatStream()", () => {
  it("turns an SSE error event into a thrown LLMHttpError", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse([
        { type: "message_start", message: { usage: { input_tokens: 3 } } },
        { type: "content_block_start", index: 0, content_block: { type: "text" } },
        { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "par" } },
        { type: "error", error: { type: "overloaded_error", message: "Overloaded" } },
      ]),
    );
    const err = await collect(
      anthropic("m", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LLMHttpError);
    expect((err as LLMHttpError).status).toBe(529);
    expect(classifyLLMError(err)).toBe("transient");
  });

  it("throws when the stream ends before message_stop", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse([
        { type: "message_start", message: { usage: { input_tokens: 3 } } },
        { type: "content_block_start", index: 0, content_block: { type: "text" } },
        { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "cut" } },
      ]),
    );
    const err = await collect(
      anthropic("m", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LLMStreamInterruptedError);
    expect(classifyLLMError(err)).toBe("transient");
  });

  it("marks truncated tool JSON with inputError instead of a silent {}", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse([
        { type: "message_start", message: { usage: { input_tokens: 3 } } },
        {
          type: "content_block_start",
          index: 0,
          content_block: { type: "tool_use", id: "t1", name: "search" },
        },
        {
          type: "content_block_delta",
          index: 0,
          delta: { type: "input_json_delta", partial_json: '{"query": "unfini' },
        },
        {
          type: "message_delta",
          delta: { stop_reason: "max_tokens" },
          usage: { output_tokens: 9 },
        },
        { type: "message_stop" },
      ]),
    );
    const chunks = await collect(
      anthropic("m", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    );
    const final = chunks.at(-1)!;
    expect(final.finishReason).toBe("length");
    expect(final.toolCalls).toHaveLength(1);
    expect(final.toolCalls![0]!.input).toEqual({});
    expect(final.toolCalls![0]!.inputError).toContain("output token limit");
  });

  it("parses complete tool JSON and an argument-less tool call", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse(
        streamOf(
          {
            type: "content_block_start",
            index: 0,
            content_block: { type: "tool_use", id: "a", name: "x" },
          },
          {
            type: "content_block_delta",
            index: 0,
            delta: { type: "input_json_delta", partial_json: '{"q":1}' },
          },
          {
            type: "content_block_start",
            index: 1,
            content_block: { type: "tool_use", id: "b", name: "y" },
          },
        ),
      ),
    );
    const chunks = await collect(
      anthropic("m", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    );
    expect(chunks.at(-1)!.toolCalls).toEqual([
      { id: "a", name: "x", input: { q: 1 } },
      { id: "b", name: "y", input: {} },
    ]);
  });
});

describe("anthropic — request timeout and cancellation", () => {
  it("aborts the request after timeoutMs with an LLMTimeoutError", async () => {
    const clock = FakeWallClock.create(0);
    const { fetch } = fakeFetch(hangingResponse);
    const pending = anthropic("m", { apiKey: "k", fetch, clock, timeoutMs: 5_000 })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    await waitFor(() => clock.pendingCount() > 0);
    clock.advance(5_000);
    const err = await pending;
    expect(err).toBeInstanceOf(LLMTimeoutError);
    expect(classifyLLMError(err)).toBe("transient");
  });

  it("times out a stream that stalls after its first event", async () => {
    const clock = FakeWallClock.create(0);
    const encoder = new TextEncoder();
    const { fetch } = fakeFetch(
      (signal) =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(
                encoder.encode(
                  `data: ${JSON.stringify({ type: "message_start", message: { usage: { input_tokens: 1 } } })}\n\n`,
                ),
              );
              signal?.addEventListener("abort", () => controller.error(signal.reason));
            },
          }),
        ),
    );
    const pending = collect(
      anthropic("m", { apiKey: "k", fetch, clock, timeoutMs: 1_000 }).chatStream!({
        messages: [USER],
      }),
    ).catch((e: unknown) => e);
    await waitFor(() => clock.pendingCount() > 0);
    clock.advance(1_000);
    expect(await pending).toBeInstanceOf(LLMTimeoutError);
  });

  it("aborts with the caller's signal and clears the timeout timer", async () => {
    const clock = FakeWallClock.create(0);
    const { fetch } = fakeFetch(hangingResponse);
    const caller = new AbortController();
    const pending = anthropic("m", { apiKey: "k", fetch, clock })
      .chat({ messages: [USER], signal: caller.signal })
      .catch((e: unknown) => e);
    await waitFor(() => clock.pendingCount() > 0);
    caller.abort();
    const err = await pending;
    expect(err).not.toBeInstanceOf(LLMTimeoutError);
    expect((err as Error).name).toBe("AbortError");
    expect(clock.pendingCount()).toBe(0);
  });
});
