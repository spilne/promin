// ---------------------------------------------------------------------------
// DefaultConsolidator safety: a bad summary never hides messages, the
// latest compaction is the one covering the newest messages, and
// concurrent runs on one thread don't write duplicate episodes.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalAgent } from "../../agent/local-agent.ts";
import { ConsolidationOutputError, DefaultConsolidator } from "../consolidator.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { RateLimitedConsolidator } from "../rate-limited-consolidator.ts";
import type { LLMProvider, LLMResponse } from "../../llm-provider.ts";
import type { Message } from "../../message.ts";

const KEY = { namespaceId: "ns", resourceId: "r", threadId: "t" };

function turns(count: number): Message[] {
  const out: Message[] = [];
  for (let i = 0; i < count; i++) {
    out.push(
      { role: "user", content: `question ${i} SECRET${i}` },
      { role: "assistant", content: `answer ${i}` },
    );
  }
  return out;
}

function replyLLM(response: () => LLMResponse | Promise<LLMResponse>): {
  llm: LLMProvider;
  requests: string[];
} {
  const requests: string[] = [];
  return {
    requests,
    llm: {
      chat: async (params: { messages: Message[] }) => {
        const user = params.messages.find((m) => m.role === "user");
        requests.push(typeof user?.content === "string" ? user.content : "");
        return response();
      },
    },
  };
}

describe("DefaultConsolidator — bad summaries", () => {
  it("an empty summary throws and writes nothing; the messages stay in the prompt", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(6));
    const { llm } = replyLLM(() => ({ content: "", finishReason: "stop" }));

    const err = await new DefaultConsolidator({ store, llm })
      .compactThread(KEY, { keepRecent: 2 })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(ConsolidationOutputError);
    expect((err as ConsolidationOutputError).reason).toBe("empty");
    expect(await store.listThreadEpisodes(KEY)).toEqual([]);
    const ctx = await store.resolveContext(KEY, { maxMessageTokens: 100_000 });
    expect(ctx.messages.map((m) => m.seq)).toEqual(Array.from({ length: 12 }, (_, i) => i + 1));
  });

  it("a summary cut off at the output cap throws and writes nothing", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(6));
    const { llm } = replyLLM(() => ({ content: "partial summ", finishReason: "length" }));

    const err = await new DefaultConsolidator({ store, llm })
      .compactThread(KEY, { keepRecent: 2 })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(ConsolidationOutputError);
    expect((err as ConsolidationOutputError).reason).toBe("truncated");
    expect(await store.listThreadEpisodes(KEY)).toEqual([]);
  });
});

describe("DefaultConsolidator — which compaction is latest", () => {
  it("continues from the compaction covering the newest messages, not the newest row", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(8));
    await store.appendThreadEpisode(KEY, {
      summary: "COVERS-8",
      sourceMessageRange: { fromSeq: 1, toSeq: 8 },
      metadata: { kind: "compact" },
    });
    // A slower run that covered less landed afterwards.
    await store.appendThreadEpisode(KEY, {
      summary: "COVERS-4",
      sourceMessageRange: { fromSeq: 1, toSeq: 4 },
      metadata: { kind: "compact" },
    });

    const ctx = await store.resolveContext(KEY, { maxMessageTokens: 100_000 });
    expect(ctx.reference).toContain("COVERS-8");
    expect(ctx.reference).not.toContain("COVERS-4");
    expect(ctx.messages[0]!.seq).toBe(9);

    const { llm, requests } = replyLLM(() => ({ content: "NEXT", finishReason: "stop" }));
    const ep = await new DefaultConsolidator({ store, llm }).compactThread(KEY, { keepRecent: 2 });
    expect(requests[0]).toContain("COVERS-8");
    expect(requests[0]).not.toContain("SECRET3");
    expect(ep.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 14 });
  });
});

describe("DefaultConsolidator — concurrent runs on one thread", () => {
  it("concurrent compactions in one process share one run", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(6));
    const { llm, requests } = replyLLM(() => ({ content: "S", finishReason: "stop" }));
    const consolidator = new DefaultConsolidator({ store, llm });

    const [a, b] = await Promise.all([
      consolidator.compactThread(KEY, { keepRecent: 2 }),
      consolidator.compactThread(KEY, { keepRecent: 2 }),
    ]);

    expect(a.id).toBe(b.id);
    expect(requests).toHaveLength(1);
    expect(await store.listThreadEpisodes(KEY)).toHaveLength(1);
  });

  it("a compaction that lost the race to another process keeps the winner's episode", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(6));
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    const slow = replyLLM(async () => {
      await gate;
      return { content: "SLOW", finishReason: "stop" };
    });
    const fast = replyLLM(() => ({ content: "FAST", finishReason: "stop" }));

    const slowRun = new DefaultConsolidator({ store, llm: slow.llm }).compactThread(KEY, {
      keepRecent: 2,
    });
    const won = await new DefaultConsolidator({ store, llm: fast.llm }).compactThread(KEY, {
      keepRecent: 2,
    });
    open();
    const lost = await slowRun;

    expect(lost.id).toBe(won.id);
    expect((await store.listThreadEpisodes(KEY)).map((e) => e.summary)).toEqual(["FAST"]);
  });

  it("concurrent distills of a thread write one episode", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendMessages(KEY, turns(2));
    const envelope = JSON.stringify({ summary: "D", outcome: null, salience: 0.5, facts: [] });
    const make = () =>
      new DefaultConsolidator({
        store,
        llm: replyLLM(() => ({ content: envelope, finishReason: "stop" })).llm,
      });

    await Promise.all([make().distillThread(KEY), make().distillThread(KEY)]);

    expect(await store.listResourceEpisodes({ namespaceId: "ns", resourceId: "r" })).toHaveLength(
      1,
    );
  });
});

describe("LocalAgent — distill rate limit wiring", () => {
  it("does not wrap a consolidator that is already rate-limited", async () => {
    const memory = new InMemoryMemoryStore();
    const envelope = JSON.stringify({ summary: "D", outcome: null, salience: 0.5, facts: [] });
    const inner = new DefaultConsolidator({
      store: memory,
      llm: replyLLM(() => ({ content: envelope, finishReason: "stop" })).llm,
    });
    const consolidator = new RateLimitedConsolidator({
      inner,
      memory,
      limits: { windowMs: 60_000, maxDistillsPerWindow: 2 },
    });
    const agent = new LocalAgent({
      agent: { name: "a", llm: replyLLM(() => ({ content: "x", finishReason: "stop" })).llm },
      runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
      memory,
      namespaceId: "ns",
      resourceId: "r",
      consolidator,
    });
    for (const threadId of ["t-1", "t-2"]) {
      await memory.appendMessages({ ...KEY, threadId }, turns(1));
    }

    // Wrapped twice, each distill would take two slots of the same count.
    await agent.distillThread("t-1");
    await agent.distillThread("t-2");
    expect(await memory.listResourceEpisodes({ namespaceId: "ns", resourceId: "r" })).toHaveLength(
      2,
    );
  });
});
