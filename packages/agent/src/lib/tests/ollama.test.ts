import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { ollama } from "../adapters/ollama.ts";

// ---- fetch mock helpers ----

type FetchFn = typeof fetch;
let originalFetch: FetchFn;

beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  globalThis.fetch = originalFetch;
});

function mockFetch(body: string, status = 200) {
  globalThis.fetch = async (_url: string, _init?: RequestInit) => {
    return new Response(body, {
      status,
      headers: { "content-type": "application/json" },
    });
  };
}

function mockStreamFetch(lines: string[]) {
  globalThis.fetch = async (_url: string, _init?: RequestInit) => {
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        for (const line of lines) controller.enqueue(encoder.encode(`${line}\n`));
        controller.close();
      },
    });
    return new Response(stream, { status: 200 });
  };
}

// ---- helpers ----

function ollamaChunk(content: string, done: boolean, extra: object = {}): string {
  return JSON.stringify({
    model: "llama3.2",
    created_at: "2024-01-01T00:00:00Z",
    message: { role: "assistant", content },
    done,
    ...extra,
  });
}

const userMsg = { task: "hello", messages: [] };

// ---- chat() ----

describe("ollama — chat()", () => {
  it("returns parsed content and usage", async () => {
    mockFetch(
      JSON.stringify({
        message: { role: "assistant", content: "Hello there!" },
        done: true,
        done_reason: "stop",
        prompt_eval_count: 10,
        eval_count: 5,
      }),
    );

    const provider = ollama({ model: "llama3.2" });
    const result = await provider.chat({ messages: [{ role: "user", content: "hi" }] });
    expect(result.content).toBe("Hello there!");
    expect(result.finishReason).toBe("stop");
    expect(result.usage).toEqual({ inputTokens: 10, outputTokens: 5 });
  });

  it("maps done_reason:length to finish reason 'length'", async () => {
    mockFetch(
      JSON.stringify({
        message: { role: "assistant", content: "truncated" },
        done: true,
        done_reason: "length",
      }),
    );
    const result = await ollama({ model: "llama3.2" }).chat({
      messages: [{ role: "user", content: "hi" }],
    });
    expect(result.finishReason).toBe("length");
  });

  it("returns tool_calls and sets finishReason to tool_calls", async () => {
    mockFetch(
      JSON.stringify({
        message: {
          role: "assistant",
          content: null,
          tool_calls: [{ function: { name: "get_weather", arguments: { city: "Paris" } } }],
        },
        done: true,
        done_reason: "stop",
      }),
    );
    const result = await ollama({ model: "llama3.2" }).chat({
      messages: [{ role: "user", content: "weather?" }],
    });
    expect(result.finishReason).toBe("tool_calls");
    expect(result.toolCalls).toHaveLength(1);
    expect(result.toolCalls![0].name).toBe("get_weather");
    expect(result.toolCalls![0].input).toEqual({ city: "Paris" });
    expect(result.toolCalls![0].id).toBe("call_0");
  });

  it("handles tool arguments as JSON string (defensive)", async () => {
    mockFetch(
      JSON.stringify({
        message: {
          role: "assistant",
          content: null,
          tool_calls: [{ function: { name: "search", arguments: '{"query":"bun"}' } }],
        },
        done: true,
        done_reason: "stop",
      }),
    );
    const result = await ollama({ model: "llama3.2" }).chat({
      messages: [{ role: "user", content: "search" }],
    });
    expect(result.toolCalls![0].input).toEqual({ query: "bun" });
  });

  it("throws on non-OK response", async () => {
    mockFetch("bad request", 400);
    await expect(
      ollama({ model: "llama3.2" }).chat({ messages: [{ role: "user", content: "hi" }] }),
    ).rejects.toThrow("Ollama API error 400");
  });

  it("sends model name and messages to /api/chat", async () => {
    let capturedBody: Record<string, unknown> | undefined;
    globalThis.fetch = async (_url: string, init?: RequestInit) => {
      capturedBody = JSON.parse(init?.body as string);
      return new Response(
        JSON.stringify({ message: { role: "assistant", content: "ok" }, done: true }),
        { status: 200 },
      );
    };
    await ollama({ model: "mistral" }).chat({ messages: [{ role: "user", content: "hi" }] });
    expect(capturedBody?.model).toBe("mistral");
    expect(capturedBody?.stream).toBe(false);
  });

  it("sends temperature and maxTokens in options object", async () => {
    let capturedBody: Record<string, unknown> | undefined;
    globalThis.fetch = async (_url: string, init?: RequestInit) => {
      capturedBody = JSON.parse(init?.body as string);
      return new Response(
        JSON.stringify({ message: { role: "assistant", content: "ok" }, done: true }),
        { status: 200 },
      );
    };
    await ollama({ model: "llama3.2", temperature: 0.2, maxTokens: 500 }).chat({
      messages: [{ role: "user", content: "hi" }],
    });
    expect((capturedBody?.options as Record<string, unknown>)?.temperature).toBe(0.2);
    expect((capturedBody?.options as Record<string, unknown>)?.num_predict).toBe(500);
  });
});

// ---- chatStream() ----

describe("ollama — chatStream()", () => {
  it("yields text deltas then final chunk", async () => {
    mockStreamFetch([
      ollamaChunk("Hello", false),
      ollamaChunk(" world", false),
      ollamaChunk("", true, { done_reason: "stop", prompt_eval_count: 8, eval_count: 4 }),
    ]);

    const chunks: Array<{ delta: string; finishReason?: string }> = [];
    for await (const chunk of ollama({ model: "llama3.2" }).chatStream!({
      messages: [{ role: "user", content: "hi" }],
    })) {
      chunks.push(chunk);
    }

    expect(chunks[0].delta).toBe("Hello");
    expect(chunks[1].delta).toBe(" world");
    const last = chunks[chunks.length - 1];
    expect(last.finishReason).toBe("stop");
    expect(last.delta).toBe("");
  });

  it("assembles tool calls from streaming chunks and sets finishReason tool_calls", async () => {
    mockStreamFetch([
      JSON.stringify({
        message: {
          role: "assistant",
          content: "",
          tool_calls: [{ function: { name: "lookup", arguments: { id: 42 } } }],
        },
        done: false,
      }),
      JSON.stringify({
        message: { role: "assistant", content: "" },
        done: true,
        done_reason: "stop",
        prompt_eval_count: 5,
        eval_count: 3,
      }),
    ]);

    const chunks: import("../llm-provider.ts").LLMStreamChunk[] = [];
    for await (const chunk of ollama({ model: "llama3.2" }).chatStream!({
      messages: [{ role: "user", content: "look it up" }],
    })) {
      chunks.push(chunk);
    }

    const last = chunks[chunks.length - 1];
    expect(last.finishReason).toBe("tool_calls");
    expect(last.toolCalls).toHaveLength(1);
    expect(last.toolCalls![0].name).toBe("lookup");
    expect(last.toolCalls![0].input).toEqual({ id: 42 });
  });

  it("includes usage from the done chunk", async () => {
    mockStreamFetch([
      ollamaChunk("Hi", false),
      ollamaChunk("", true, { done_reason: "stop", prompt_eval_count: 12, eval_count: 7 }),
    ]);

    const chunks: import("../llm-provider.ts").LLMStreamChunk[] = [];
    for await (const chunk of ollama({ model: "llama3.2" }).chatStream!({
      messages: [{ role: "user", content: "hi" }],
    })) {
      chunks.push(chunk);
    }
    const last = chunks[chunks.length - 1];
    expect(last.usage).toEqual({ inputTokens: 12, outputTokens: 7 });
  });

  it("throws on non-OK response", async () => {
    mockFetch("service unavailable", 503);
    async function drain() {
      for await (const _ of ollama({ model: "llama3.2" }).chatStream!({
        messages: [{ role: "user", content: "hi" }],
      })) {
        /* empty */
      }
    }
    await expect(drain()).rejects.toThrow("Ollama API error 503");
  });
});
