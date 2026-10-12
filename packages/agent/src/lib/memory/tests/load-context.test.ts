// ---------------------------------------------------------------------------
// loadResolvedContext reads only what the prompt can hold: the message tail
// that fits the budget (after the latest compaction), and a bounded number
// of episodes.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";
import { loadResolvedContext } from "../load-context.ts";
import type {
  EpisodeListParams,
  EpisodicRecord,
  MessageRange,
  ScopedKey,
  StoredMessage,
  ThreadKey,
} from "../types.ts";
import type { Message } from "../../message.ts";

/** In-memory store that counts the message and episode rows it hands out. */
class CountingStore extends InMemoryMemoryStore {
  messagesRead = 0;
  episodesRead = 0;

  override async getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]> {
    const rows = await super.getMessages(key, range);
    this.messagesRead += rows.length;
    return rows;
  }

  override async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    const rows = await super.listResourceEpisodes(key, params);
    this.episodesRead += rows.length;
    return rows;
  }
}

const KEY = { namespaceId: "acme", resourceId: "alice", threadId: "long" };

function history(turns: number): Message[] {
  const out: Message[] = [];
  for (let i = 0; i < turns; i++) {
    out.push(
      { role: "user", content: `question ${i}` },
      { role: "assistant", content: `answer ${i}` },
    );
  }
  return out;
}

describe("loadResolvedContext", () => {
  it("reads one page of a long thread's tail, not the whole transcript", async () => {
    const store = new CountingStore();
    await store.appendMessages(KEY, history(2_000));
    store.messagesRead = 0;

    const ctx = await loadResolvedContext({ store, key: KEY, budget: { maxMessageTokens: 40 } });

    expect(ctx.messages.at(-1)!.seq).toBe(4_000);
    expect(store.messagesRead).toBeLessThanOrEqual(100);
  });

  it("walks back page by page when the budget needs more than one", async () => {
    const store = new CountingStore();
    await store.appendMessages(KEY, history(500));
    store.messagesRead = 0;

    const ctx = await loadResolvedContext({
      store,
      key: KEY,
      budget: { maxMessageTokens: 150 * 3 },
    });

    expect(ctx.messages.length).toBeGreaterThan(100);
    expect(store.messagesRead).toBeLessThan(1_000);
  });

  it("never reads the messages a compaction folded", async () => {
    const store = new CountingStore();
    await store.appendMessages(KEY, history(100));
    await store.appendThreadEpisode(KEY, {
      summary: "S",
      sourceMessageRange: { fromSeq: 1, toSeq: 190 },
      metadata: { kind: "compact" },
    });
    store.messagesRead = 0;

    const ctx = await loadResolvedContext({
      store,
      key: KEY,
      budget: { maxMessageTokens: 1_000_000 },
    });

    expect(ctx.messages.map((m) => m.seq)).toEqual([
      191, 192, 193, 194, 195, 196, 197, 198, 199, 200,
    ]);
    expect(store.messagesRead).toBe(10);
  });

  it("reads at most as many episodes as the episode budget can render", async () => {
    const store = new CountingStore();
    for (let i = 0; i < 300; i++) {
      await store.appendResourceEpisode(
        { namespaceId: "acme", resourceId: "alice" },
        { summary: `episode ${i}` },
      );
    }
    await store.appendMessages(KEY, history(1));

    await loadResolvedContext({
      store,
      key: KEY,
      budget: { maxMessageTokens: 1_000, maxEpisodeTokens: 400 },
    });

    expect(store.episodesRead).toBe(50);
  });
});
