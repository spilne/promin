// ---------------------------------------------------------------------------
// tool_choice across adapters, and Anthropic's is_error on tool results.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { anthropic } from "../anthropic.ts";
import { ollama } from "../ollama.ts";
import { openai } from "../openai.ts";
import type { LLMToolChoice, LLMToolDefinition } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";
import { fakeFetch, jsonResponse } from "./fake-fetch.ts";

const TOOLS: LLMToolDefinition[] = [
  { name: "search", description: "s", parameters: { type: "object", properties: {} } },
  { name: "_respond", description: "r", parameters: { type: "object", properties: {} } },
];
const USER: Message = { role: "user", content: "hi" };

const ANTHROPIC_OK = {
  content: [{ type: "text", text: "ok" }],
  stop_reason: "end_turn",
  usage: { input_tokens: 1, output_tokens: 1 },
};
const OPENAI_OK = {
  choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
  usage: { prompt_tokens: 1, completion_tokens: 1 },
};
const OLLAMA_OK = { message: { role: "assistant", content: "ok" }, done: true };

describe("anthropic — tool_choice and is_error", () => {
  const cases: Array<[LLMToolChoice, unknown]> = [
    ["auto", { type: "auto" }],
    ["required", { type: "any" }],
    ["none", { type: "none" }],
    [{ name: "_respond" }, { type: "tool", name: "_respond" }],
  ];
  for (const [choice, wire] of cases) {
    it(`maps ${JSON.stringify(choice)}`, async () => {
      const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
      await anthropic("m", { apiKey: "k", fetch }).chat({
        messages: [USER],
        tools: TOOLS,
        toolChoice: choice,
      });
      expect(requests[0]?.body.tool_choice).toEqual(wire);
    });
  }

  it("omits tool_choice when none is given", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
    await anthropic("m", { apiKey: "k", fetch }).chat({ messages: [USER], tools: TOOLS });
    expect(requests[0]?.body.tool_choice).toBeUndefined();
  });

  it("marks failed tool results with is_error", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
    await anthropic("m", { apiKey: "k", fetch }).chat({
      tools: TOOLS,
      messages: [
        USER,
        {
          role: "assistant",
          content: null,
          toolCalls: [
            { id: "a", name: "search", input: {} },
            { id: "b", name: "search", input: {} },
          ],
        },
        { role: "tool", toolCallId: "a", content: "Tool timed out after 5ms.", isError: true },
        { role: "tool", toolCallId: "b", content: "fine" },
      ],
    });
    const messages = requests[0]?.body.messages as Array<{ content: unknown }>;
    expect(messages[2]?.content).toEqual([
      {
        type: "tool_result",
        tool_use_id: "a",
        content: "Tool timed out after 5ms.",
        is_error: true,
      },
      { type: "tool_result", tool_use_id: "b", content: "fine" },
    ]);
  });
});

describe("openai — tool_choice", () => {
  const cases: Array<[LLMToolChoice | undefined, unknown]> = [
    [undefined, "auto"],
    ["required", "required"],
    ["none", "none"],
    [{ name: "_respond" }, { type: "function", function: { name: "_respond" } }],
  ];
  for (const [choice, wire] of cases) {
    it(`maps ${JSON.stringify(choice)}`, async () => {
      const { fetch, requests } = fakeFetch(() => jsonResponse(OPENAI_OK));
      await openai("m", { apiKey: "k", fetch }).chat({
        messages: [USER],
        tools: TOOLS,
        ...(choice !== undefined && { toolChoice: choice }),
      });
      expect(requests[0]?.body.tool_choice).toEqual(wire);
    });
  }
});

describe("ollama — tool_choice approximation", () => {
  it("sends only the forced tool, and no tools for none", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(OLLAMA_OK));
    const llm = ollama({ model: "m", fetch });
    await llm.chat({ messages: [USER], tools: TOOLS, toolChoice: { name: "_respond" } });
    await llm.chat({ messages: [USER], tools: TOOLS, toolChoice: "none" });
    await llm.chat({ messages: [USER], tools: TOOLS });
    const names = requests.map((r) =>
      ((r.body.tools as Array<{ function: { name: string } }> | undefined) ?? []).map(
        (t) => t.function.name,
      ),
    );
    expect(names).toEqual([["_respond"], [], ["search", "_respond"]]);
  });
});

describe("anthropic — no prefill where Anthropic forbids it", () => {
  const PROSE: Message = { role: "assistant", content: "the answer is 3" };

  it("closes a forced-tool request that ends on an assistant turn with a user turn", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
    await anthropic("m", { apiKey: "k", fetch }).chat({
      messages: [USER, PROSE],
      tools: TOOLS,
      toolChoice: { name: "_respond" },
    });
    const messages = requests[0]?.body.messages as Array<{ role: string }>;
    expect(messages.at(-1)?.role).toBe("user");
  });

  it("keeps a plain prefill when nothing forbids it", async () => {
    const { fetch, requests } = fakeFetch(() => jsonResponse(ANTHROPIC_OK));
    await anthropic("m", { apiKey: "k", fetch }).chat({ messages: [USER, PROSE] });
    const messages = requests[0]?.body.messages as Array<{ role: string }>;
    expect(messages.at(-1)?.role).toBe("assistant");
  });
});

describe("ollama — tool call ids", () => {
  it("are unique across responses, since Ollama sends none", async () => {
    const withCall = {
      message: {
        role: "assistant",
        content: "",
        tool_calls: [{ function: { name: "search", arguments: {} } }],
      },
      done: true,
    };
    const { fetch } = fakeFetch(() => jsonResponse(withCall));
    const llm = ollama({ model: "m", fetch });
    const a = await llm.chat({ messages: [USER], tools: TOOLS });
    const b = await llm.chat({ messages: [USER], tools: TOOLS });
    expect(a.toolCalls?.[0]?.id).toBeTruthy();
    expect(a.toolCalls?.[0]?.id).not.toBe(b.toolCalls?.[0]?.id);
  });
});
