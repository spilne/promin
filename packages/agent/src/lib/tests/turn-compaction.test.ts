// ---------------------------------------------------------------------------
// In-turn compaction (`agentAction({ compaction })`):
//
//   - a long turn over the token budget folds its older steps into ONE
//     fenced summary (reference data, user role), replaced on every
//     compaction — summaries never pile up;
//   - a compaction folds down to `targetRatio * maxTokens`, so the next one
//     is several steps away (no summary per step);
//   - when the seed or the kept steps alone are over budget, the turn
//     reports it once and compacts only every few steps;
//   - the seed and the user task are never compacted, every request keeps
//     tool calls and results paired, and the last steps verbatim;
//   - the summary request is plain text: no tools, no tool blocks; it is
//     retried like any LLM call and capped in size, as is the digest;
//   - the summary call counts in usage, events, metrics and the trace;
//   - the provider's reported usage counts towards the estimate;
//   - a replay rebuilds the same list from the journal, without calling the
//     summary LLM again, even when the compaction config changed;
//   - an invalid config fails at build time.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { agentAction } from "../agent-action.ts";
import { LocalAgent } from "../agent/local-agent.ts";
import type { LLMChatParams, LLMProvider, LLMResponse } from "../llm-provider.ts";
import type { Message } from "../message.ts";
import { InMemoryAgentMetrics } from "../metrics/types.ts";
import { isReferenceDataMessage } from "../reference-data.ts";
import { tool } from "../tool.ts";
import {
  capSummary,
  estimateMessageTokens,
  planCompaction,
  TURN_SUMMARY_SOURCE,
  type TurnCompactionConfig,
} from "../turn-compaction.ts";
import {
  collectingBus,
  crashAndRecover,
  deferred,
  freshRunner,
  scriptedLLM,
} from "./engine-harness.ts";

const BIG = "x".repeat(800);

const fetchTool = tool({
  name: "fetch",
  description: "fetch a page",
  parameters: z.object({ page: z.number() }),
  execute: async ({ page }) => `page ${page}: ${BIG}`,
});

/** Budget for the engine tests: a step is ~215 tokens, a summary at most 100. */
const BUDGET: TurnCompactionConfig = { maxTokens: 2_000, summaryMaxTokens: 100 };

function callFetch(page: number, usage?: LLMResponse["usage"]): LLMResponse {
  return {
    content: null,
    finishReason: "tool_calls",
    toolCalls: [{ id: `c${page}`, name: "fetch", input: { page } }],
    ...(usage && { usage }),
  };
}

const done: LLMResponse = { content: "done", finishReason: "stop" };

/** `n` fetch steps, then an answer. */
function fetchingLLM(n: number, usage?: LLMResponse["usage"]) {
  return scriptedLLM([...Array.from({ length: n }, (_, i) => callFetch(i, usage)), done]);
}

/** A summary LLM that answers `summary <n>` and records its requests. */
function summaryLLM(usage?: LLMResponse["usage"]): LLMProvider & { calls: LLMChatParams[] } {
  const calls: LLMChatParams[] = [];
  return {
    calls,
    chat: async (params) => {
      calls.push(params);
      return { content: `summary ${calls.length}`, finishReason: "stop", ...(usage && { usage }) };
    },
  };
}

function summaryMessages(messages: ReadonlyArray<Message>): Message[] {
  return messages.filter(
    (m) =>
      m.role === "user" &&
      isReferenceDataMessage(m) &&
      m.content.includes(`source="${TURN_SUMMARY_SOURCE}"`),
  );
}

/** Every tool call has its result and every result its call. */
function expectPaired(messages: ReadonlyArray<Message>): void {
  const calls = new Set<string>();
  const results = new Set<string>();
  for (const m of messages) {
    if (m.role === "assistant") for (const c of m.toolCalls ?? []) calls.add(c.id);
    if (m.role === "tool") {
      expect(calls.has(m.toolCallId)).toBe(true);
      results.add(m.toolCallId);
    }
  }
  expect([...calls].sort()).toEqual([...results].sort());
}

async function runLong(params: {
  steps: number;
  compaction: TurnCompactionConfig;
  systemPrompt?: string;
  maxSteps?: number;
}) {
  const llm = fetchingLLM(params.steps);
  const { bus, events } = collectingBus();
  const { runner } = freshRunner();
  const result = await runner.run({
    workflow: agentAction({
      name: "long",
      llm,
      tools: { fetch: fetchTool },
      ...(params.systemPrompt !== undefined && { systemPrompt: params.systemPrompt }),
      maxSteps: params.maxSteps ?? 50,
      stall: false,
      compaction: params.compaction,
      bus,
    }),
    workflowId: "long-1",
    input: { task: "read every page" },
  });
  return { result, llm, events };
}

describe("planCompaction", () => {
  const step = (id: string): Message[] => [
    { role: "assistant", content: null, toolCalls: [{ id, name: "fetch", input: {} }] },
    { role: "tool", toolCallId: id, content: BIG },
  ];
  const seed: Message[] = [{ role: "user", content: "task" }];
  const steps = (n: number): Message[] => Array.from({ length: n }, (_, i) => step(`s${i}`)).flat();

  it("does nothing under the budget", () => {
    const messages = [...seed, ...steps(3)];
    expect(planCompaction({ messages, stepStart: 1, config: { maxTokens: 10_000 } })).toBeNull();
  });

  it("folds the fewest old steps that reach the target", () => {
    const messages = [...seed, ...steps(10)];
    const plan = planCompaction({
      messages,
      stepStart: 1,
      config: { maxTokens: 2_000, summaryMaxTokens: 100 },
    })!;
    // Target 1200 tokens: the summary (≤ 200 with its fence) plus four steps.
    expect(plan.cut).toBe(1 + 2 * 6);
    expect(plan.after).toBeLessThanOrEqual(1_200);
    expect(plan.ineffective).toBe(false);
  });

  it("keeps keepRecentSteps however far over the target", () => {
    const messages = [...seed, ...steps(4)];
    const plan = planCompaction({
      messages,
      stepStart: 1,
      config: { maxTokens: 300, keepRecentSteps: 2, summaryMaxTokens: 64 },
    })!;
    expect(plan.cut).toBe(1 + 2 * 2);
    expect(plan.ineffective).toBe(true);
  });

  it("does nothing when only the kept steps are over the budget", () => {
    const messages = [...seed, ...steps(2)];
    expect(estimateMessageTokens(messages)).toBeGreaterThan(300);
    expect(planCompaction({ messages, stepStart: 1, config: { maxTokens: 300 } })).toBeNull();
  });

  it("never touches the seed, however large", () => {
    const hugeSeed: Message[] = [{ role: "user", content: BIG.repeat(20) }, ...step("a")];
    expect(planCompaction({ messages: hugeSeed, stepStart: 3, config: BUDGET })).toBeNull();
  });

  it("counts the provider's reported tokens", () => {
    const messages = [...seed, ...steps(3)];
    expect(planCompaction({ messages, stepStart: 1, config: BUDGET })).toBeNull();
    expect(
      planCompaction({ messages, stepStart: 1, config: BUDGET, reportedTokens: 5_000 }),
    ).toMatchObject({ cut: 3, tokens: 5_000 });
  });
});

describe("capSummary", () => {
  it("keeps the most recent lines behind a marker", () => {
    const text = ["alpha one", "bravo two", "charlie three", "delta four"].join("\n");
    expect(capSummary({ text, maxChars: 100 })).toBe(text);
    expect(capSummary({ text, maxChars: 40 })).toBe("… (earlier steps omitted)\ndelta four");
    expect(capSummary({ text, maxChars: 30 })).toBe("… (earlier steps omitted)\nfour");
  });
});

describe("agentAction — in-turn compaction", () => {
  it("keeps one summary, the task and paired tool calls in every request", async () => {
    const summarizer = summaryLLM();
    const { result, llm, events } = await runLong({
      steps: 20,
      systemPrompt: "you fetch",
      compaction: { ...BUDGET, llm: summarizer },
    });

    expect(result.answer).toBe("done");
    expect(summarizer.calls.length).toBeGreaterThan(0);
    expect(events.filter((e) => e.type === "compact")).toHaveLength(summarizer.calls.length);

    for (const call of llm.calls) {
      const sent = call.messages;
      expect(sent[0]).toEqual({ role: "system", content: "you fetch" });
      expect(sent[1]).toEqual({ role: "user", content: "read every page" });
      expect(summaryMessages(sent).length).toBeLessThanOrEqual(1);
      expect(estimateMessageTokens(sent)).toBeLessThanOrEqual(2_000);
      expectPaired(sent);
    }
    // The summary sits right after the task, the newest steps verbatim.
    const last = llm.calls[llm.calls.length - 1]!.messages;
    expect(last[2]).toBe(summaryMessages(last)[0]!);
    expect(last[2]!.content).toContain(`summary ${summarizer.calls.length}`);
    const kept = last.flatMap((m) => (m.role === "tool" ? [m.toolCallId] : []));
    expect(kept.at(-1)).toBe("c19");

    // The transcript the turn produced is never compacted.
    expect(result.turnMessages.filter((m) => m.role === "tool")).toHaveLength(20);
    expect(summaryMessages(result.turnMessages)).toHaveLength(0);
  });

  it("compacts down to the target, so a summary is several steps apart", async () => {
    const summarizer = summaryLLM();
    const { events } = await runLong({ steps: 13, compaction: { ...BUDGET, llm: summarizer } });

    // 13 steps of ~215 tokens against a 2000-token budget: one compaction
    // at the step that crosses it, down to ~1200 tokens.
    expect(summarizer.calls).toHaveLength(1);
    const compacts = events.flatMap((e) => (e.type === "compact" ? [e] : []));
    expect(compacts).toHaveLength(1);
    expect(compacts[0]!.kept).toBeGreaterThan(2 * 2);
    expect(events.some((e) => e.type === "compaction.ineffective")).toBe(false);
  });

  it("reports an over-budget seed once and backs off instead of compacting every step", async () => {
    const summarizer = summaryLLM();
    const { events } = await runLong({
      steps: 12,
      systemPrompt: BIG.repeat(12),
      compaction: { ...BUDGET, llm: summarizer },
    });

    const steps = events.flatMap((e) => (e.type === "compact" ? [e.step] : []));
    // Steps 3, 7 and 11 instead of every step from 3 on.
    expect(steps).toEqual([3, 7, 11]);
    expect(summarizer.calls).toHaveLength(3);
    const ineffective = events.filter((e) => e.type === "compaction.ineffective");
    expect(ineffective).toHaveLength(1);
    expect(ineffective[0]).toMatchObject({ step: 3, maxTokens: 2_000 });
  });

  it("sends the summary request as plain text and folds in the previous summary", async () => {
    const summarizer = summaryLLM();
    await runLong({
      steps: 20,
      compaction: { ...BUDGET, llm: summarizer, prompt: "SUMMARISE" },
    });

    expect(summarizer.calls.length).toBeGreaterThanOrEqual(2);
    for (const call of summarizer.calls) {
      expect(call.tools).toBeUndefined();
      expect(call.maxTokens).toBe(100);
      expect(call.messages.map((m) => m.role)).toEqual(["system", "user"]);
      expect(call.messages[0]!.content).toBe("SUMMARISE");
      expect(call.messages[1]!.content).toContain('<reference-data source="turn-steps">');
    }
    expect(summarizer.calls[0]!.messages[1]!.content).toContain("Tool call fetch [c0]");
    // The second summary folds the first one in instead of stacking on it.
    expect(summarizer.calls[1]!.messages[1]!.content).toContain("summary 1");
    expect(summarizer.calls[1]!.messages[1]!.content).not.toContain("Tool call fetch [c0]");
  });

  it("caps the digest and the summary at summaryMaxTokens * 4 characters", async () => {
    const digest = await runLong({ steps: 40, compaction: { ...BUDGET, summarize: false } });
    const verbose: LLMProvider = {
      chat: async () => ({ content: "fact\n".repeat(500), finishReason: "stop" }),
    };
    const llm = await runLong({ steps: 20, compaction: { ...BUDGET, llm: verbose } });

    for (const run of [digest, llm]) {
      const last = run.llm.calls[run.llm.calls.length - 1]!.messages;
      const fenced = summaryMessages(last)[0]!.content ?? "";
      const body = fenced.slice(fenced.indexOf("## Summary"), fenced.indexOf("</reference-data>"));
      expect(body.length).toBeLessThanOrEqual(400 + 60);
      expect(body).toContain("… (earlier steps omitted)");
    }
  });

  it("retries a rate-limited summary call instead of falling back to the digest", async () => {
    let attempts = 0;
    const flaky: LLMProvider = {
      chat: async () => {
        attempts++;
        if (attempts === 1) throw Object.assign(new Error("429 rate limited"), { status: 429 });
        return { content: "real summary", finishReason: "stop" };
      },
    };
    const llm = fetchingLLM(13);
    const { runner } = freshRunner();
    await runner.run({
      workflow: agentAction({
        name: "retry",
        llm,
        tools: { fetch: fetchTool },
        llmRetry: { baseDelayMs: 0 },
        compaction: { ...BUDGET, llm: flaky },
      }),
      workflowId: "retry-1",
      input: { task: "go" },
    });

    expect(attempts).toBe(2);
    const last = llm.calls[llm.calls.length - 1]!.messages;
    expect(summaryMessages(last)[0]!.content).toContain("real summary");
  });

  it("counts the summary call in usage, events, metrics and the trace", async () => {
    const summarizer = summaryLLM({ inputTokens: 700, outputTokens: 40 });
    const metrics = new InMemoryAgentMetrics();
    const llm = fetchingLLM(13, { inputTokens: 10, outputTokens: 1 });
    const { bus, events } = collectingBus();
    const { runner } = freshRunner();
    const result = await runner.run({
      workflow: agentAction({
        name: "account",
        llm,
        tools: { fetch: fetchTool },
        compaction: { ...BUDGET, llm: summarizer },
        metrics,
        llmProvider: "p",
        llmModel: "m",
        bus,
      }),
      workflowId: "account-1",
      input: { task: "go" },
    });

    // 13 tool steps + the answer at 10 in each, plus the summary.
    expect(result.usage).toMatchObject({ inputTokens: 13 * 10 + 700, outputTokens: 13 + 40 });
    const calls = events.flatMap((e) => (e.type === "llm.call" ? [e] : []));
    expect(calls.filter((e) => e.purpose === "compaction")).toEqual([
      expect.objectContaining({ tokens: { inputTokens: 700, outputTokens: 40 } }),
    ]);
    expect(metrics.counterSamples("llm.tokens.input")).toContainEqual({
      value: 700,
      labels: expect.objectContaining({ provider: "p", model: "m", purpose: "compaction" }),
    });
    expect(result.trace!.spans.filter((s) => s.kind === "compaction")).toEqual([
      expect.objectContaining({ summarized: true, usage: { inputTokens: 700, outputTokens: 40 } }),
    ]);
    // Think spans are all still there.
    expect(result.trace!.spans.filter((s) => s.kind === "think")).toHaveLength(14);
  });

  it("compacts on the provider's reported usage when the estimate is low", async () => {
    const small = tool({
      name: "fetch",
      description: "fetch a page",
      parameters: z.object({ page: z.number() }),
      execute: async () => "ok",
    });
    const run = async (usage?: LLMResponse["usage"]) => {
      const summarizer = summaryLLM();
      const { runner } = freshRunner();
      await runner.run({
        workflow: agentAction({
          name: "usage",
          llm: fetchingLLM(3, usage),
          tools: { fetch: small },
          compaction: { ...BUDGET, llm: summarizer },
        }),
        workflowId: "usage-1",
        input: { task: "go" },
      });
      return summarizer.calls.length;
    };
    expect(await run()).toBe(0);
    expect(await run({ inputTokens: 1_500, outputTokens: 10, cacheReadTokens: 900 })).toBe(1);
  });

  it("compacts with a deterministic digest when summarize is off or the summary fails", async () => {
    const run = async (compaction: TurnCompactionConfig) => {
      const { llm } = await runLong({ steps: 13, compaction });
      return summaryMessages(llm.calls[llm.calls.length - 1]!.messages)[0]!.content;
    };
    const failing: LLMProvider = {
      chat: async () => {
        throw new Error("summary down");
      },
    };

    const off = await run({ ...BUDGET, summarize: false });
    expect(off).toMatch(/- fetch\(\{"page":\d+\}\) → ok: page \d+: xxx/);
    expect(await run({ ...BUDGET, llm: failing })).toBe(off);
  });

  it("replays the compacted list from the journal after a crash", async () => {
    const gate = deferred();
    const summarizer = summaryLLM();
    const sent: Record<"A" | "B", Message[][]> = { A: [], B: [] };
    const workerLLM = (worker: "A" | "B"): LLMProvider => ({
      chat: async (params) => {
        sent[worker].push(params.messages);
        if (worker === "B") return done;
        // Park on the first think after the compaction.
        if (summarizer.calls.length === 1) {
          await gate.promise;
          return { content: "from the crashed worker", finishReason: "stop" };
        }
        return callFetch(sent.A.length - 1);
      },
    });

    const { result, storage } = await crashAndRecover({
      build: (worker) =>
        agentAction({
          name: "replay",
          llm: workerLLM(worker),
          tools: { fetch: fetchTool },
          // Worker B runs without compaction: the journaled plan decides.
          ...(worker === "A" && { compaction: { ...BUDGET, llm: summarizer } }),
        }),
      input: { task: "go" },
      workflowId: "replay-1",
      stalled: () => summarizer.calls.length === 1 && sent.A.length > 0,
      release: () => gate.resolve(),
    });

    expect(result.answer).toBe("done");
    expect(summarizer.calls).toHaveLength(1);
    // B's first live think is A's interrupted one, with the same messages.
    expect(sent.B).toHaveLength(1);
    expect(sent.B[0]).toEqual(sent.A[sent.A.length - 1]!);
    expect(summaryMessages(sent.B[0]!)).toHaveLength(1);

    const journal = await storage.loadJournal({ workflowId: "replay-1", stepName: "agent" });
    expect(journal.filter((e) => e.activityName.startsWith("compact-"))).toHaveLength(1);
  });

  it("rejects an invalid compaction config when the turn is built", () => {
    const build = (compaction: TurnCompactionConfig) => () =>
      agentAction({ name: "bad", llm: fetchingLLM(0), compaction });
    expect(build({ maxTokens: 0 })).toThrow("compaction.maxTokens");
    expect(build({ maxTokens: Number.NaN })).toThrow("compaction.maxTokens");
    expect(build({ maxTokens: 1_000, keepRecentSteps: 1.5 })).toThrow("keepRecentSteps");
    expect(build({ maxTokens: 1_000, keepRecentSteps: 0 })).toThrow("keepRecentSteps");
    expect(build({ maxTokens: 1_000, targetRatio: 1.5 })).toThrow("targetRatio");
    expect(build({ maxTokens: 1_000, summaryMaxTokens: -1 })).toThrow("summaryMaxTokens");
  });

  it("reaches thread turns through LocalAgent, keeping the stored transcript whole", async () => {
    const summarizer = summaryLLM();
    const { runner } = freshRunner();
    const agent = new LocalAgent({
      agent: {
        name: "thread",
        llm: fetchingLLM(13),
        tools: { fetch: fetchTool },
        compaction: { ...BUDGET, llm: summarizer },
      },
      runner,
      namespaceId: "ns",
    });
    const thread = await agent.thread("t-1");
    const out = await thread.send({ task: "go" });

    expect(await out.text).toBe("done");
    expect(summarizer.calls).toHaveLength(1);
    const stored = await thread.messages();
    expect(stored.filter((m) => m.role === "tool")).toHaveLength(13);
    expect(summaryMessages(stored)).toHaveLength(0);
  });
});
