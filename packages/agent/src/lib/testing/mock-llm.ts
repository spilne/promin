// ---------------------------------------------------------------------------
// Reusable `LLMProvider` test/demo helpers. Replaces the inline `mockLLM`
// pattern that every test had been re-rolling.
//
// Three flavors:
//
//   - `mockLLM(responses)` — replays a fixed array of `LLMResponse`s in
//     order. Matches the existing test idiom; throws "Mock LLM exhausted"
//     when the agent makes more calls than scripted.
//
//   - `echoLLM(opts?)` — auto-generates a one-line reply for the most
//     recent user message. No script required; great for demos.
//
//   - `streamingMockLLM(responses, deltas?)` — like `mockLLM`, but also
//     implements `chatStream` by chunking each response into N deltas.
//     Useful when exercising the streaming path.
// ---------------------------------------------------------------------------

import type { LLMChatParams, LLMProvider, LLMResponse, LLMStreamChunk } from "../llm-provider.ts";

/**
 * Replay a scripted list of `LLMResponse`s. Each `chat()` call consumes
 * the next entry; calling past the end throws.
 *
 * ```ts
 * const llm = mockLLM([
 *   { content: "first reply",  finishReason: "stop" },
 *   { content: "second reply", finishReason: "stop" },
 * ]);
 * ```
 */
export function mockLLM(responses: ReadonlyArray<LLMResponse>): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r)
        throw new Error(`Mock LLM exhausted (called ${i} times, scripted ${responses.length})`);
      return r;
    },
  };
}

export interface EchoLLMOptions {
  /**
   * Templated reply pattern. `{task}` is replaced with the user's most
   * recent message, `{n}` with the call count.
   * Default: `"I heard you say: '{task}'. (mock reply #{n})"`.
   */
  readonly template?: string;
  /** Deterministic stub usage. Default: `{ inputTokens: 10, outputTokens: 8 }`. */
  readonly usage?: { readonly inputTokens: number; readonly outputTokens: number };
}

/**
 * Auto-replies with a templated echo of the most recent user message.
 * No scripting required — handy for demos that don't need a real LLM.
 */
export function echoLLM(opts: EchoLLMOptions = {}): LLMProvider {
  const template = opts.template ?? "I heard you say: '{task}'. (mock reply #{n})";
  const usage = opts.usage ?? { inputTokens: 10, outputTokens: 8 };
  let n = 0;
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => {
      n += 1;
      const lastUser = lastUserText(params);
      const content = template.replace("{task}", lastUser).replace("{n}", String(n));
      return { content, finishReason: "stop", usage };
    },
  };
}

/**
 * Variant of `mockLLM` that also implements `chatStream`. Each scripted
 * `LLMResponse.content` is chunked into `chunkSize`-character deltas so
 * consumers exercising the streaming path see deltas + a final chunk
 * with `finishReason`.
 */
export function streamingMockLLM(
  responses: ReadonlyArray<LLMResponse>,
  opts: { readonly chunkSize?: number } = {},
): LLMProvider {
  const chunkSize = opts.chunkSize ?? 8;
  let i = 0;
  return {
    chat: async () => {
      const r = responses[i++];
      if (!r)
        throw new Error(`Mock LLM exhausted (called ${i} times, scripted ${responses.length})`);
      return r;
    },
    chatStream: async function* (_params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      const r = responses[i++];
      if (!r)
        throw new Error(`Mock LLM exhausted (called ${i} times, scripted ${responses.length})`);
      const text = r.content ?? "";
      for (let pos = 0; pos < text.length; pos += chunkSize) {
        yield { delta: text.slice(pos, pos + chunkSize) };
      }
      yield {
        delta: "",
        finishReason: r.finishReason,
        toolCalls: r.toolCalls,
        usage: r.usage,
        thinkingBlocks: r.thinkingBlocks,
      };
    },
  };
}

/** Pull the most recent user `content` from `params.messages`. */
function lastUserText(params: LLMChatParams): string {
  for (let i = params.messages.length - 1; i >= 0; i--) {
    const m = params.messages[i]!;
    if (m.role === "user" && typeof m.content === "string") return m.content;
  }
  return "(no user message)";
}
