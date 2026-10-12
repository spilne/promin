// ---------------------------------------------------------------------------
// ZoryaKnowledgeBases under concurrency: an ingestion never blocks other
// work on its base, renews its claim while it runs, cleans up after itself
// when another replica rewrote the source, and a base created elsewhere
// resolves through the registry without `ensure`.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  FlatTextChunker,
  InMemoryKnowledgeBaseStore,
  InMemoryRetrieverRegistry,
  deletesVersion,
  type DeleteSourceRequest,
  type IngestibleRetriever,
  type KnowledgeChunk,
  type KnowledgeIngestDocument,
  type RetrieveResult,
} from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { ZoryaKnowledgeBases } from "../zorya-knowledge-bases.ts";

const NS = "acme";

function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

async function until(check: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !check(); i++) await new Promise((r) => setTimeout(r, 0));
  expect(check()).toBe(true);
}

/** A durable retriever shared by every replica (one pgvector table). */
class SharedDurableRetriever implements IngestibleRetriever {
  readonly rows = new Map<string, KnowledgeChunk>();
  readonly chunker = new FlatTextChunker({ maxSize: 60, overlap: 0 });
  /** The next `addDocument` waits on this before writing. */
  hold?: Promise<void>;
  adds = 0;

  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    this.adds += 1;
    const hold = this.hold;
    this.hold = undefined;
    if (hold) await hold;
    const chunks = this.chunker.chunk(document);
    for (const chunk of chunks) this.rows.set(chunk.id, chunk);
    return chunks;
  }

  async deleteSource(request: DeleteSourceRequest): Promise<void> {
    for (const [id, chunk] of this.rows) {
      if (chunk.source.id !== request.sourceId) continue;
      if (deletesVersion({ request, version: chunk.source.version })) this.rows.delete(id);
    }
  }

  async retrieve(): Promise<RetrieveResult[]> {
    return [...this.rows.values()].map((chunk) => ({ chunk, score: 1 }));
  }
}

function replica(params: {
  store: InMemoryKnowledgeBaseStore;
  retriever: IngestibleRetriever;
  workerId: string;
  clock?: FakeWallClock;
  staleAfterMs?: number;
}): ZoryaKnowledgeBases {
  return new ZoryaKnowledgeBases({
    store: params.store,
    registry: new InMemoryRetrieverRegistry(),
    retrievers: { durable: true, create: () => params.retriever },
    ingestion: {
      pollIntervalMs: 0,
      workerId: params.workerId,
      ...(params.staleAfterMs !== undefined && { staleAfterMs: params.staleAfterMs }),
    },
    refreshIntervalMs: 0,
    clock: params.clock ?? FakeWallClock.create(1_000),
  });
}

describe("ZoryaKnowledgeBases — concurrency", () => {
  it("an ingestion in flight does not block ensure() or a new ingest on its base", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const retriever = new SharedDurableRetriever();
    const held = gate();
    retriever.hold = held.promise;
    const kb = replica({ store, retriever, workerId: "A" });
    await kb.create({ namespaceId: NS, id: "docs" });
    await kb.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s1", text: "first" } });
    await until(() => retriever.adds === 1);

    let ensured = false;
    let queued = false;
    void kb.ensure({ namespaceId: NS, ids: ["docs"] }).then(() => (ensured = true));
    void kb
      .ingest({ namespaceId: NS, baseId: "docs", source: { id: "s2", text: "second" } })
      .then(() => (queued = true));
    await until(() => ensured && queued);

    held.open();
    await kb.drain();
    await kb.stop();
  });

  it("a rewrite on another replica mid-ingest leaves no stale chunks", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    const retriever = new SharedDurableRetriever();
    const a = replica({ store, retriever, workerId: "A", clock });
    const b = replica({ store, retriever, workerId: "B", clock });
    await a.create({ namespaceId: NS, id: "docs" });
    await b.ensure({ namespaceId: NS, ids: ["docs"] });

    const held = gate();
    retriever.hold = held.promise;
    const oldText = Array.from({ length: 10 }, (_, i) => `old paragraph ${i}`).join("\n\n");
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: oldText } });
    await until(() => retriever.adds === 1);

    // B rewrites the source and ingests it while A is still embedding.
    clock.advance(10);
    await b.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "new text" } });
    await b.drain();
    held.open();
    await a.drain();

    const stored = await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" });
    expect(stored).toMatchObject({ status: "ready", text: "new text", chunkCount: 1 });
    expect([...retriever.rows.values()].map((c) => c.text)).toEqual(["new text"]);
    await a.stop();
    await b.stop();
  });

  it("a long ingestion renews its claim, so another replica never takes it over", async () => {
    const clock = FakeWallClock.create(1_000);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    const retriever = new SharedDurableRetriever();
    const staleAfterMs = 3_000;
    const a = replica({ store, retriever, workerId: "A", clock, staleAfterMs });
    const b = replica({ store, retriever, workerId: "B", clock, staleAfterMs });
    await a.create({ namespaceId: NS, id: "docs" });
    await b.ensure({ namespaceId: NS, ids: ["docs"] });

    const held = gate();
    retriever.hold = held.promise;
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "slow" } });
    await until(() => retriever.adds === 1);

    // Ten stale windows pass while A embeds; its heartbeat keeps the claim fresh.
    for (let i = 0; i < 30; i++) {
      await until(() => clock.pendingCount() > 0);
      clock.advance(1_000);
      await new Promise((r) => setTimeout(r, 0));
    }
    await b.drain();
    expect(retriever.adds).toBe(1);

    held.open();
    await a.drain();
    expect(await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" })).toMatchObject(
      { status: "ready" },
    );
    await a.stop();
    await b.stop();
  });

  it("a base created on another replica resolves through the registry without ensure", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const a = replica({ store, retriever: new SharedDurableRetriever(), workerId: "A" });
    const b = replica({ store, retriever: new SharedDurableRetriever(), workerId: "B" });
    await b.start();
    await a.create({ namespaceId: NS, id: "late" });

    expect(b.registry.get({ id: "late", namespaceId: NS })).toBeNull();
    const row = await b.registry.resolve({ id: "late", namespaceId: NS });
    expect(row?.namespaceId).toBe(NS);
    await a.stop();
    await b.stop();
  });
});
