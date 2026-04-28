// ---------------------------------------------------------------------------
// `redactPriorThinkingBlocks` — strip extended-thinking from messages
// older than the most recent user turn. Pure helper; tested in isolation.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { redactPriorThinkingBlocks } from "../agent-loop.ts";
import type { Message } from "../message.ts";

describe("redactPriorThinkingBlocks", () => {
  it("preserves thinking in the current turn (after the most recent user msg)", () => {
    const msgs: Message[] = [
      { role: "user", content: "first" },
      {
        role: "assistant",
        content: "answer1",
        thinkingBlocks: [{ thinking: "old reasoning", signature: "sig1" }],
      },
      { role: "user", content: "second" },
      {
        role: "assistant",
        content: "answer2",
        thinkingBlocks: [{ thinking: "current reasoning", signature: "sig2" }],
      },
    ];
    const out = redactPriorThinkingBlocks(msgs);
    // First assistant: thinking dropped.
    expect((out[1] as { thinkingBlocks?: unknown }).thinkingBlocks).toBeUndefined();
    // Second assistant (after the most recent user): preserved.
    expect((out[3] as { thinkingBlocks?: unknown }).thinkingBlocks).toEqual([
      { thinking: "current reasoning", signature: "sig2" },
    ]);
  });

  it("preserves content + toolCalls; only drops thinkingBlocks", () => {
    const msgs: Message[] = [
      { role: "user", content: "first" },
      {
        role: "assistant",
        content: "I'll search",
        toolCalls: [{ id: "t1", name: "search", input: { q: "x" } }],
        thinkingBlocks: [{ thinking: "search rationale", signature: "sig" }],
      },
      { role: "tool", toolCallId: "t1", content: "result" },
      { role: "user", content: "next" },
    ];
    const out = redactPriorThinkingBlocks(msgs);
    const assistant = out[1] as Message & { toolCalls?: unknown; thinkingBlocks?: unknown };
    expect(assistant.role).toBe("assistant");
    expect(assistant.content).toBe("I'll search");
    expect(assistant.toolCalls).toEqual([{ id: "t1", name: "search", input: { q: "x" } }]);
    expect(assistant.thinkingBlocks).toBeUndefined();
  });

  it("is a no-op when there's no user message yet (system-only prefix)", () => {
    const msgs: Message[] = [{ role: "system", content: "you are helpful" }];
    const out = redactPriorThinkingBlocks(msgs);
    expect(out).toEqual(msgs);
  });

  it("is a no-op when only one user turn has happened (nothing prior to redact)", () => {
    const msgs: Message[] = [
      { role: "system", content: "rules" },
      { role: "user", content: "hi" },
      {
        role: "assistant",
        content: "hello",
        thinkingBlocks: [{ thinking: "reasoning", signature: "sig" }],
      },
    ];
    const out = redactPriorThinkingBlocks(msgs);
    // The assistant message is in the current turn (after the only user msg).
    expect((out[2] as { thinkingBlocks?: unknown }).thinkingBlocks).toEqual([
      { thinking: "reasoning", signature: "sig" },
    ]);
  });

  it("redacts across multiple prior turns", () => {
    const msgs: Message[] = [
      { role: "user", content: "u1" },
      {
        role: "assistant",
        content: "a1",
        thinkingBlocks: [{ thinking: "t1", signature: "s1" }],
      },
      { role: "user", content: "u2" },
      {
        role: "assistant",
        content: "a2",
        thinkingBlocks: [{ thinking: "t2", signature: "s2" }],
      },
      { role: "user", content: "u3" },
      {
        role: "assistant",
        content: "a3",
        thinkingBlocks: [{ thinking: "t3", signature: "s3" }],
      },
    ];
    const out = redactPriorThinkingBlocks(msgs);
    // a1 and a2 are prior turns — redacted.
    expect((out[1] as { thinkingBlocks?: unknown }).thinkingBlocks).toBeUndefined();
    expect((out[3] as { thinkingBlocks?: unknown }).thinkingBlocks).toBeUndefined();
    // a3 is current — preserved.
    expect((out[5] as { thinkingBlocks?: unknown }).thinkingBlocks).toEqual([
      { thinking: "t3", signature: "s3" },
    ]);
  });

  it("doesn't mutate the input array", () => {
    const msgs: Message[] = [
      { role: "user", content: "u1" },
      {
        role: "assistant",
        content: "a1",
        thinkingBlocks: [{ thinking: "t1", signature: "s1" }],
      },
      { role: "user", content: "u2" },
    ];
    const before = JSON.parse(JSON.stringify(msgs));
    redactPriorThinkingBlocks(msgs);
    expect(msgs).toEqual(before);
  });
});
