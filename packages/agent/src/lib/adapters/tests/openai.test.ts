// ---------------------------------------------------------------------------
// OpenAI-compatible adapters (openai, gemini, llamacpp) and ollama —
// endpoints, message conversion, guarded tool-argument parsing, streamed
// errors and typed HTTP errors, against recorded responses.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { openai } from "../openai.ts";
import { gemini } from "../gemini.ts";
import { llamacpp } from "../llamacpp.ts";
import { ollama } from "../ollama.ts";
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

const USER: Message = { role: "user", content: "hi" };

function completion(message: Record<string, unknown>, finishReason = "stop") {
  return {
    choices: [{ message: { content: null, ...message }, finish_reason: finishReason }],
    usage: { prompt_tokens: 1, completion_tokens: 1 },
  };
}

describe("openai-compatible endpoints", () => {
  it("openai posts to /v1/chat/completions", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(completion({ content: "ok" })));
    await openai("gpt", { apiKey: "k", fetch }).chat({ messages: [USER] });
    expect(requests[0]!.url).toBe("https://api.openai.com/v1/chat/completions");
  });

  it("gemini posts to Google's OpenAI-compatible endpoint", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(completion({ content: "ok" })));
    await gemini("gemini-2.5-flash", { apiKey: "k", fetch }).chat({ messages: [USER] });
    expect(requests[0]!.url).toBe(
      "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
    );
  });

  it("llamacpp posts to the server's /v1/chat/completions", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(completion({ content: "ok" })));
    await llamacpp({ baseURL: "http://box:8080/", fetch }).chat({ messages: [USER] });
    expect(requests[0]!.url).toBe("http://box:8080/v1/chat/completions");
  });
});

describe("openai — message conversion", () => {
  it("leaves out assistant turns with no text and no tool calls", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(completion({ content: "ok" })));
    await openai("gpt", { apiKey: "k", fetch }).chat({
      messages: [USER, { role: "assistant", content: null }, { role: "user", content: "again" }],
    });
    const sent = requests[0]!.body["messages"] as Array<{ role: string }>;
    expect(sent.map((m) => m.role)).toEqual(["user", "user"]);
  });
});

describe("openai — tool arguments", () => {
  it("chat(): malformed arguments become an inputError instead of throwing", async () => {
    const { fetch } = fakeFetch(() =>
      jsonResponse(
        completion(
          {
            tool_calls: [
              { id: "c1", type: "function", function: { name: "search", arguments: "{oops" } },
              { id: "c2", type: "function", function: { name: "search", arguments: '{"q":"x"}' } },
            ],
          },
          "tool_calls",
        ),
      ),
    );
    const res = await openai("gpt", { apiKey: "k", fetch }).chat({ messages: [USER] });
    expect(res.toolCalls![0]!.input).toEqual({});
    expect(res.toolCalls![0]!.inputError).toContain("not valid JSON");
    expect(res.toolCalls![1]).toEqual({ id: "c2", name: "search", input: { q: "x" } });
  });

  it("chatStream(): truncated arguments become an inputError", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse(
        [
          {
            choices: [
              {
                delta: {
                  tool_calls: [
                    { index: 0, id: "c1", function: { name: "search", arguments: '{"q": "ab' } },
                  ],
                },
              },
            ],
          },
          { choices: [{ delta: {}, finish_reason: "length" }] },
        ],
        { done: true },
      ),
    );
    const chunks = await collect(
      openai("gpt", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    );
    const call = chunks.at(-1)!.toolCalls![0]!;
    expect(call.input).toEqual({});
    expect(call.inputError).toContain("output token limit");
  });
});

describe("openai — errors", () => {
  it("throws LLMHttpError with retry-after-ms on a 429", async () => {
    const { fetch } = fakeFetch(() =>
      jsonResponse(
        { error: { message: "Rate limit reached", code: "rate_limit_exceeded" } },
        { status: 429, headers: { "retry-after-ms": "1500", "retry-after": "2" } },
      ),
    );
    const err = await openai("gpt", { apiKey: "k", fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LLMHttpError);
    expect((err as LLMHttpError).retryAfterMs).toBe(1500);
    expect(classifyLLMError(err)).toBe("rate_limit");
  });

  it("classifies a 400 context_length_exceeded as context overflow", async () => {
    const { fetch } = fakeFetch(() =>
      jsonResponse(
        { error: { message: "This model's maximum context length is 128000 tokens" } },
        { status: 400 },
      ),
    );
    const err = await gemini("g", { apiKey: "k", fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect((err as LLMHttpError).provider).toBe("gemini");
    expect(classifyLLMError(err)).toBe("context_overflow");
  });

  it("turns a streamed error chunk into a thrown LLMHttpError", async () => {
    const { fetch } = fakeFetch(() =>
      sseResponse([
        { choices: [{ delta: { content: "par" } }] },
        { error: { code: 503, message: "The model is overloaded" } },
      ]),
    );
    const err = await collect(
      openai("gpt", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    ).catch((e: unknown) => e);
    expect((err as LLMHttpError).status).toBe(503);
    expect(classifyLLMError(err)).toBe("transient");
  });

  it("throws when the stream ends without a finish_reason", async () => {
    const { fetch } = fakeFetch(() => sseResponse([{ choices: [{ delta: { content: "cut" } }] }]));
    const err = await collect(
      openai("gpt", { apiKey: "k", fetch }).chatStream!({ messages: [USER] }),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LLMStreamInterruptedError);
  });

  it("times out after timeoutMs", async () => {
    const clock = FakeWallClock.create(0);
    const { fetch } = fakeFetch(hangingResponse);
    const pending = openai("gpt", { apiKey: "k", fetch, clock, timeoutMs: 2_000 })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    await waitFor(() => clock.pendingCount() > 0);
    clock.advance(2_000);
    expect(await pending).toBeInstanceOf(LLMTimeoutError);
  });
});

describe("ollama", () => {
  it("guards string tool arguments, drops empty assistant turns, types HTTP errors", async () => {
    const { fetch, requests } = fakeFetch(() =>
      jsonResponse({
        message: {
          role: "assistant",
          content: "",
          tool_calls: [{ function: { name: "search", arguments: "{bad" } }],
        },
        done: true,
      }),
    );
    const res = await ollama({ model: "llama", fetch }).chat({
      messages: [USER, { role: "assistant", content: null }, { role: "user", content: "again" }],
    });
    expect(res.toolCalls![0]!.inputError).toContain("not valid JSON");
    const sent = requests[0]!.body["messages"] as Array<{ role: string }>;
    expect(sent.map((m) => m.role)).toEqual(["user", "user"]);

    const failing = fakeFetch(() => jsonResponse({ error: "model not found" }, { status: 404 }));
    const err = await ollama({ model: "llama", fetch: failing.fetch })
      .chat({ messages: [USER] })
      .catch((e: unknown) => e);
    expect((err as LLMHttpError).status).toBe(404);
    expect(classifyLLMError(err)).toBe("permanent");
  });
});
