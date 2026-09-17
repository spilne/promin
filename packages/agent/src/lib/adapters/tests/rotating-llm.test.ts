// ---------------------------------------------------------------------------
// rotatingLLM — slot rotation with optional rate-limit hints + shared
// cooldown coordination.
//
// Pins:
//   - Round-robin distribution when no slot reports a rateLimitHint.
//   - Least-remaining selection picks the slot with the most headroom.
//   - 429 from one slot triggers failover to the next; the 429-ed slot is
//     marked exhausted in the CapacityStore.
//   - Exhausted slots are skipped until resetsAt passes; then they
//     re-enter rotation.
//   - A shared CapacityStore propagates exhaustion across two rotatingLLM
//     instances (multi-replica scenario).
//   - Proactive exhaustion: a near-zero `remainingTokens` hint marks the
//     slot in the store BEFORE the next 429 fires.
//   - chatStream: pre-stream 429 fails over; mid-stream errors throw.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { rotatingLLM } from "../rotating-llm.ts";
import { InMemoryCapacityStore } from "../capacity-store.ts";
import type { CapacityStore } from "../capacity-store.ts";
import type {
  LLMProvider,
  LLMChatParams,
  LLMResponse,
  LLMStreamChunk,
} from "../../llm-provider.ts";

interface ScriptedSlot {
  llm: LLMProvider;
  calls: () => number;
  /** Force the next chat() to throw the supplied error. */
  setNextError: (err: Error | undefined) => void;
}

function scriptedSlot(label: string, defaultResponse?: LLMResponse): ScriptedSlot {
  let nextError: Error | undefined;
  let count = 0;
  let response: LLMResponse =
    defaultResponse ?? ({ content: label, finishReason: "stop" } as LLMResponse);
  return {
    llm: {
      chat: async (_p: LLMChatParams): Promise<LLMResponse> => {
        count++;
        if (nextError) {
          const err = nextError;
          nextError = undefined;
          throw err;
        }
        return response;
      },
      chatStream: async function* (_p: LLMChatParams): AsyncIterable<LLMStreamChunk> {
        count++;
        if (nextError) {
          const err = nextError;
          nextError = undefined;
          throw err;
        }
        if (response.content) yield { delta: response.content };
        yield {
          delta: "",
          finishReason: response.finishReason,
          ...(response.toolCalls && { toolCalls: response.toolCalls }),
          ...(response.usage && { usage: response.usage }),
          ...(response.rateLimitHint && { rateLimitHint: response.rateLimitHint }),
        };
      },
    },
    calls: () => count,
    setNextError: (err) => {
      nextError = err;
    },
    // Hook used inside tests to swap the response (e.g. attach a rate-limit hint).
    ...({
      setResponse(r: LLMResponse) {
        response = r;
      },
    } as { setResponse(r: LLMResponse): void }),
  };
}

function rateLimit429(retryAfterSec = 1): Error {
  const err = new Error("HTTP 429 Too Many Requests");
  (err as unknown as { headers: Record<string, string> }).headers = {
    "retry-after": String(retryAfterSec),
  };
  return err;
}

const TASK: LLMChatParams = { messages: [{ role: "user", content: "hi" }] };

describe("rotatingLLM — selection strategies", () => {
  it("round-robin distributes calls across slots when no hints are reported", async () => {
    const a = scriptedSlot("A");
    const b = scriptedSlot("B");
    const c = scriptedSlot("C");
    const llm = rotatingLLM([a.llm, b.llm, c.llm], { strategy: "round-robin" });

    for (let i = 0; i < 6; i++) await llm.chat(TASK);

    // Two passes through the rotation. Each slot used twice.
    expect(a.calls()).toBe(2);
    expect(b.calls()).toBe(2);
    expect(c.calls()).toBe(2);
  });

  it("least-remaining picks the slot with the most headroom once hints are seen", async () => {
    const a = scriptedSlot("A", {
      content: "A",
      finishReason: "stop",
      rateLimitHint: { remainingTokens: 100, resetsAt: Date.now() + 60_000 },
    });
    const b = scriptedSlot("B", {
      content: "B",
      finishReason: "stop",
      rateLimitHint: { remainingTokens: 90_000, resetsAt: Date.now() + 60_000 },
    });

    const llm = rotatingLLM([a.llm, b.llm]);

    // Cold start with no hints — first call goes round-robin (slot A,
    // index 0). After A's response lands, the next 5 calls should
    // prefer B, which reports 90k remaining tokens vs A's 100.
    for (let i = 0; i < 6; i++) await llm.chat(TASK);

    expect(a.calls()).toBe(1);
    expect(b.calls()).toBe(5);
  });
});

describe("rotatingLLM — 429 failover", () => {
  it("on 429, marks the slot exhausted and retries against the next slot", async () => {
    const a = scriptedSlot("A");
    const b = scriptedSlot("B");
    const store = new InMemoryCapacityStore();
    const llm = rotatingLLM([a.llm, b.llm], { strategy: "round-robin", capacityStore: store });

    a.setNextError(rateLimit429(2));
    const result = await llm.chat(TASK);
    expect(result.content).toBe("B"); // failed over
    expect(a.calls()).toBe(1);
    expect(b.calls()).toBe(1);

    const exhausted = await store.getExhausted();
    expect(exhausted.has("slot-0")).toBe(true);
    // retry-after: 2s → resetsAt should be ~2s in the future.
    const cooldownMs = exhausted.get("slot-0")! - Date.now();
    expect(cooldownMs).toBeGreaterThan(500);
    expect(cooldownMs).toBeLessThan(3_000);
  });

  it("skips exhausted slots until their cooldown expires", async () => {
    const a = scriptedSlot("A");
    const b = scriptedSlot("B");
    const store = new InMemoryCapacityStore();
    // Pre-mark slot 0 as exhausted in the past — read should evict it
    // automatically; both slots remain eligible.
    await store.markExhausted("slot-0", Date.now() - 1_000);
    const llm = rotatingLLM([a.llm, b.llm], { strategy: "round-robin", capacityStore: store });
    await llm.chat(TASK);
    // First-pass round-robin: cursor 0 → slot 0 since both are eligible.
    expect(a.calls()).toBe(1);

    // Now mark slot 0 in the future — next call must skip A.
    await store.markExhausted("slot-0", Date.now() + 60_000);
    await llm.chat(TASK);
    expect(b.calls()).toBe(1);
    expect(a.calls()).toBe(1); // unchanged
  });

  it("when every slot is exhausted, surfaces the most recent error", async () => {
    const a = scriptedSlot("A");
    const b = scriptedSlot("B");
    const store = new InMemoryCapacityStore();
    await store.markExhausted("slot-0", Date.now() + 60_000);
    await store.markExhausted("slot-1", Date.now() + 60_000);
    const llm = rotatingLLM([a.llm, b.llm], { capacityStore: store });

    await expect(llm.chat(TASK)).rejects.toThrow(/all slots exhausted/i);
  });
});

describe("rotatingLLM — shared CapacityStore across instances", () => {
  it("an exhaustion mark by one instance is honoured by another", async () => {
    const store: CapacityStore = new InMemoryCapacityStore();
    const a1 = scriptedSlot("A");
    const b1 = scriptedSlot("B");
    const llmOne = rotatingLLM([a1.llm, b1.llm], {
      strategy: "round-robin",
      capacityStore: store,
    });
    a1.setNextError(rateLimit429(5));
    await llmOne.chat(TASK); // marks slot-0 exhausted

    const a2 = scriptedSlot("A");
    const b2 = scriptedSlot("B");
    const llmTwo = rotatingLLM([a2.llm, b2.llm], {
      strategy: "round-robin",
      capacityStore: store,
    });
    await llmTwo.chat(TASK);
    // Second instance shares the same store → slot-0 is in cooldown,
    // so the call must land on slot-1.
    expect(a2.calls()).toBe(0);
    expect(b2.calls()).toBe(1);
  });
});

describe("rotatingLLM — proactive exhaustion via near-zero hint", () => {
  it("marks a slot exhausted when remainingTokens drops below the threshold", async () => {
    const resetsAt = Date.now() + 60_000;
    const a = scriptedSlot("A", {
      content: "A",
      finishReason: "stop",
      rateLimitHint: { remainingTokens: 50, resetsAt },
    });
    const b = scriptedSlot("B");
    const store = new InMemoryCapacityStore();
    const llm = rotatingLLM([a.llm, b.llm], {
      strategy: "round-robin",
      capacityStore: store,
      exhaustionTokenThreshold: 1_000,
    });

    await llm.chat(TASK); // hits A; A reports 50 remaining + resetsAt
    // Allow the void-promised markExhausted write to settle.
    await new Promise<void>((r) => setTimeout(r, 0));

    const exhausted = await store.getExhausted();
    expect(exhausted.get("slot-0")).toBe(resetsAt);
    // Next call must skip A in favour of B.
    await llm.chat(TASK);
    expect(b.calls()).toBe(1);
  });

  it("threshold=0 disables proactive cooldown — slot stays in rotation", async () => {
    const a = scriptedSlot("A", {
      content: "A",
      finishReason: "stop",
      rateLimitHint: { remainingTokens: 1, resetsAt: Date.now() + 60_000 },
    });
    const b = scriptedSlot("B");
    const store = new InMemoryCapacityStore();
    const llm = rotatingLLM([a.llm, b.llm], {
      strategy: "round-robin",
      capacityStore: store,
      exhaustionTokenThreshold: 0,
    });

    await llm.chat(TASK);
    expect((await store.getExhausted()).size).toBe(0);
  });
});

describe("rotatingLLM — chatStream", () => {
  it("pre-stream 429 fails over to the next slot", async () => {
    const a = scriptedSlot("A");
    const b = scriptedSlot("B");
    const llm = rotatingLLM([a.llm, b.llm], { strategy: "round-robin" });
    a.setNextError(rateLimit429(1));

    const chunks: string[] = [];
    let finalReason: string | undefined;
    for await (const chunk of llm.chatStream!(TASK)) {
      if (chunk.delta) chunks.push(chunk.delta);
      if (chunk.finishReason) finalReason = chunk.finishReason;
    }
    expect(chunks.join("")).toBe("B");
    expect(finalReason).toBe("stop");
  });
});
