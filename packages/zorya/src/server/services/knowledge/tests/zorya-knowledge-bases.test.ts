import { describe, expect, it } from "bun:test";
import {
  InMemoryKnowledgeBaseStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  type EmbeddingProvider,
  type IngestibleRetriever,
  type KnowledgeBaseStore,
  type KnowledgeChunk,
  type KnowledgeIngestDocument,
  type RetrieveResult,
} from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeIngestError,
  KnowledgeInputError,
  KnowledgeSourceNotFoundError,
} from "../errors.ts";
import { ZoryaKnowledgeBases, type KnowledgeSourceAdapter } from "../zorya-knowledge-bases.ts";

/** Deterministic bag-of-words embedding over a fixed vocabulary — no network. */
function fakeEmbeddings(params: { fail?: () => boolean } = {}): EmbeddingProvider & {
  calls: number;
} {
  const vocabulary = ["restart", "workers", "heartbeat", "deploy", "rollback", "alpha", "beta"];
  const provider = {
    calls: 0,
    async embed(text: string): Promise<number[]> {
      provider.calls += 1;
      if (params.fail?.()) throw new Error("embedding provider unavailable");
      const lower = text.toLowerCase();
      return vocabulary.map((word) => (lower.includes(word) ? 1 : 0));
    },
  };
  return provider;
}

/** A durable-looking retriever that logs its calls. */
class RecordingRetriever implements IngestibleRetriever {
  ready = false;
  readonly events: string[] = [];
  private chunks: KnowledgeChunk[] = [];

  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    if (!this.ready) throw new Error("retriever_not_ready");
    this.events.push(`add:${document.id}`);
    const chunk: KnowledgeChunk = {
      id: `${document.id}#0`,
      text: document.text,
      index: 0,
      source: { id: document.id },
    };
    this.chunks.push(chunk);
    return [chunk];
  }

  async deleteSource(sourceId: string): Promise<void> {
    this.events.push(`delete:${sourceId}`);
    this.chunks = this.chunks.filter((c) => c.source.id !== sourceId);
  }

  async retrieve(): Promise<RetrieveResult[]> {
    return this.chunks.map((chunk) => ({ chunk, score: 1 }));
  }
}

describe("ZoryaKnowledgeBases — bases", () => {
  it("creates, lists, updates and removes a base, mirroring it in the registry", async () => {
    const clock = FakeWallClock.create(1_000);
    const registry = new InMemoryRetrieverRegistry();
    const kbs = new ZoryaKnowledgeBases({ registry, clock });

    const created = await kbs.create({
      id: "docs",
      description: "Product docs",
      tags: [" internal ", "internal", ""],
    });
    expect(created).toMatchObject({
      id: "docs",
      description: "Product docs",
      tags: ["internal"],
      status: "ready",
      sourceCount: 0,
      chunkCount: 0,
      createdAt: 1_000,
    });
    expect(registry.get("docs")?.description).toBe("Product docs");

    clock.advance(500);
    const updated = await kbs.update({
      id: "docs",
      patch: { description: null, metadata: { owner: "platform" } },
    });
    expect(updated.description).toBeUndefined();
    expect(updated.metadata).toEqual({ owner: "platform" });
    expect(updated.updatedAt).toBe(1_500);
    expect(registry.get("docs")?.description).toBeUndefined();
    expect(registry.get("docs")?.metadata).toEqual({ owner: "platform" });

    expect((await kbs.list()).map((b) => b.id)).toEqual(["docs"]);
    await kbs.remove("docs");
    expect(await kbs.list()).toEqual([]);
    expect(registry.get("docs")).toBeNull();
  });

  it("refuses a taken id, including one a host-wired retriever holds", async () => {
    const registry = new InMemoryRetrieverRegistry([
      { id: "host", retriever: { retrieve: async () => [] } },
    ]);
    const kbs = new ZoryaKnowledgeBases({ registry });
    await kbs.create({ id: "docs" });
    await expect(kbs.create({ id: "docs" })).rejects.toBeInstanceOf(KnowledgeBaseExistsError);
    await expect(kbs.create({ id: "host" })).rejects.toThrow("already registered by the host");
  });

  it("refuses ids recipes could not bind", async () => {
    const kbs = new ZoryaKnowledgeBases();
    for (const id of ["", "_reserved", "has space", "x".repeat(129)]) {
      await expect(kbs.create({ id })).rejects.toBeInstanceOf(KnowledgeInputError);
    }
  });

  it("answers not-found for an unknown base", async () => {
    const kbs = new ZoryaKnowledgeBases();
    expect(await kbs.get("ghost")).toBeNull();
    await expect(kbs.listSources("ghost")).rejects.toBeInstanceOf(KnowledgeBaseNotFoundError);
    await expect(
      kbs.ingest({ baseId: "ghost", source: { id: "s", text: "x" } }),
    ).rejects.toBeInstanceOf(KnowledgeBaseNotFoundError);
  });
});

describe("ZoryaKnowledgeBases — ingestion", () => {
  it("ingests, searches, lists sources without text, and replaces a re-ingested source", async () => {
    const kbs = new ZoryaKnowledgeBases({ embeddings: fakeEmbeddings() });
    await kbs.create({ id: "docs" });
    const source = await kbs.ingest({
      baseId: "docs",
      source: {
        id: "runbook",
        title: "Runbook",
        tags: ["ops"],
        text: "Restart workers when heartbeat lag exceeds the threshold.",
      },
    });
    expect(source).toMatchObject({ id: "runbook", status: "ready", chunkCount: 1 });
    expect(source).not.toHaveProperty("text");

    const hits = await kbs.search({ baseId: "docs", request: { query: "heartbeat" } });
    expect(hits[0]?.chunk.source).toMatchObject({ id: "runbook", title: "Runbook" });

    await kbs.ingest({
      baseId: "docs",
      source: { id: "runbook", title: "Updated runbook", text: "Rollback after a failed deploy." },
    });
    const replaced = await kbs.search({ baseId: "docs", request: { query: "deploy rollback" } });
    expect(replaced).toHaveLength(1);
    expect(replaced[0]?.chunk.source.title).toBe("Updated runbook");
    expect(await kbs.search({ baseId: "docs", request: { query: "heartbeat" } })).toEqual([]);

    const sources = await kbs.listSources("docs");
    expect(sources.map((s) => s.id)).toEqual(["runbook"]);
    expect(sources[0]).not.toHaveProperty("text");
    expect(await kbs.get("docs")).toMatchObject({ sourceCount: 1, chunkCount: 1 });
  });

  it("records a failed ingestion with its reason, drops partial chunks and degrades the base", async () => {
    let failing = false;
    const store = new InMemoryKnowledgeBaseStore();
    const kbs = new ZoryaKnowledgeBases({
      store,
      embeddings: fakeEmbeddings({ fail: () => failing }),
      chunker: { chunk: (d) => [0, 1].map((index) => chunkOf({ document: d, index })) },
    });
    await kbs.create({ id: "docs" });
    await kbs.ingest({ baseId: "docs", source: { id: "ok", text: "alpha" } });

    failing = true;
    const err = await kbs
      .ingest({ baseId: "docs", source: { id: "bad", text: "beta" } })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(KnowledgeIngestError);
    expect((err as Error).message).toContain("embedding provider unavailable");

    const bad = (await kbs.listSources("docs")).find((s) => s.id === "bad");
    expect(bad).toMatchObject({
      status: "failed",
      error: "embedding provider unavailable",
      chunkCount: 0,
    });
    expect(await kbs.get("docs")).toMatchObject({ status: "degraded", chunkCount: 2 });
    // The failure is durable, so the dashboard still shows it after a restart.
    const [row] = await store.loadAll();
    expect(row!.sources.find((s) => s.id === "bad")?.status).toBe("failed");

    failing = false;
    expect(await kbs.search({ baseId: "docs", request: { query: "beta" } })).toEqual([]);
    const retried = await kbs.reindexSource({ baseId: "docs", sourceId: "bad" });
    expect(retried.status).toBe("ready");
    expect(retried).not.toHaveProperty("error");
    expect(await kbs.get("docs")).toMatchObject({ status: "ready", chunkCount: 4 });
  });

  it("validates sources before indexing", async () => {
    const kbs = new ZoryaKnowledgeBases({ maxSourceCharacters: 10 });
    await kbs.create({ id: "docs" });
    for (const source of [
      { id: "  ", text: "x" },
      { id: "s", text: "   " },
      { id: "s", text: "x".repeat(11) },
    ]) {
      await expect(kbs.ingest({ baseId: "docs", source })).rejects.toBeInstanceOf(
        KnowledgeInputError,
      );
    }
    expect(await kbs.listSources("docs")).toEqual([]);
  });

  it("removes a source and its chunks", async () => {
    const kbs = new ZoryaKnowledgeBases();
    await kbs.create({ id: "docs" });
    await kbs.ingest({ baseId: "docs", source: { id: "one", text: "alpha content" } });
    await kbs.removeSource({ baseId: "docs", sourceId: "one" });
    expect(await kbs.listSources("docs")).toEqual([]);
    expect(await kbs.search({ baseId: "docs", request: { query: "alpha" } })).toEqual([]);
    await expect(kbs.removeSource({ baseId: "docs", sourceId: "one" })).rejects.toBeInstanceOf(
      KnowledgeSourceNotFoundError,
    );
  });

  it("pages the chunks of ready sources, optionally for one source", async () => {
    const kbs = new ZoryaKnowledgeBases({
      chunker: { chunk: (d) => [0, 1, 2].map((index) => chunkOf({ document: d, index })) },
    });
    await kbs.create({ id: "docs" });
    await kbs.ingest({ baseId: "docs", source: { id: "b", text: "beta" } });
    await kbs.ingest({ baseId: "docs", source: { id: "a", text: "alpha" } });

    const page = await kbs.chunks({ baseId: "docs", limit: 2, offset: 2 });
    expect(page.total).toBe(6);
    expect(page.chunks.map((c) => c.id)).toEqual(["a#2", "b#0"]);
    const one = await kbs.chunks({ baseId: "docs", sourceId: "b" });
    expect(one.chunks.map((c) => c.id)).toEqual(["b#0", "b#1", "b#2"]);
    await expect(kbs.chunks({ baseId: "docs", sourceId: "ghost" })).rejects.toBeInstanceOf(
      KnowledgeSourceNotFoundError,
    );
  });

  it("runs writes to one base one at a time", async () => {
    const order: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const retriever = new InMemoryRetriever();
    const slow: IngestibleRetriever = {
      retrieve: (r) => retriever.retrieve(r),
      deleteSource: (id) => retriever.deleteSource(id),
      addDocument: async (d) => {
        order.push(`start:${d.id}`);
        if (d.id === "first") await gate;
        order.push(`end:${d.id}`);
        return retriever.addDocument(d);
      },
    };
    const kbs = new ZoryaKnowledgeBases({ retrievers: { durable: true, create: () => slow } });
    await kbs.create({ id: "docs" });
    const first = kbs.ingest({ baseId: "docs", source: { id: "first", text: "alpha" } });
    const second = kbs.ingest({ baseId: "docs", source: { id: "second", text: "beta" } });
    for (let i = 0; i < 20 && order.length === 0; i += 1) await Promise.resolve();
    release();
    await Promise.all([first, second]);
    expect(order).toEqual(["start:first", "end:first", "start:second", "end:second"]);
  });

  it("imports through a registered adapter and refuses unknown kinds", async () => {
    const fixture: KnowledgeSourceAdapter = {
      kind: "fixture",
      load: async ({ config }) => [{ id: String(config), text: "adapter content" }],
    };
    const kbs = new ZoryaKnowledgeBases({ sourceAdapters: [fixture] });
    await kbs.create({ id: "docs" });
    expect(kbs.sourceKinds()).toEqual(["fixture"]);
    const sources = await kbs.importSources({ baseId: "docs", kind: "fixture", config: "src-1" });
    expect(sources.map((s) => s.id)).toEqual(["src-1"]);
    const hits = await kbs.search({ baseId: "docs", request: { query: "adapter content" } });
    expect(hits[0]?.chunk.source.id).toBe("src-1");
    await expect(
      kbs.importSources({ baseId: "docs", kind: "nope", config: {} }),
    ).rejects.toMatchObject({ code: "unknown_source_kind" });
  });
});

describe("ZoryaKnowledgeBases — persistence and start", () => {
  it("rebuilds a non-durable index from the store", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const first = new ZoryaKnowledgeBases({ store });
    await first.create({ id: "docs" });
    await first.ingest({ baseId: "docs", source: { id: "one", text: "alpha content" } });

    const registry = new InMemoryRetrieverRegistry();
    const second = new ZoryaKnowledgeBases({ store, registry });
    await second.start();
    expect((await second.listSources("docs"))[0]?.id).toBe("one");
    expect(registry.get("docs")).not.toBeNull();
    const hits = await second.search({ baseId: "docs", request: { query: "alpha" } });
    expect(hits[0]?.chunk.source.id).toBe("one");
  });

  it("marks a source failed when re-indexing it at start fails, and keeps starting", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const first = new ZoryaKnowledgeBases({ store });
    await first.create({ id: "docs" });
    await first.ingest({ baseId: "docs", source: { id: "one", text: "alpha" } });

    const second = new ZoryaKnowledgeBases({
      store,
      embeddings: fakeEmbeddings({ fail: () => true }),
    });
    await second.start();
    expect((await second.listSources("docs"))[0]).toMatchObject({
      status: "failed",
      error: "embedding provider unavailable",
    });
  });

  it("does not replay a durable retriever and builds it before seed ingestion", async () => {
    const retriever = new RecordingRetriever();
    const registry = new InMemoryRetrieverRegistry();
    const store = new InMemoryKnowledgeBaseStore();
    const config = {
      store,
      registry,
      retrievers: {
        durable: true,
        create: async () => {
          retriever.events.push("setup");
          retriever.ready = true;
          return retriever;
        },
      },
      seed: [{ id: "docs", documents: [{ id: "one", text: "alpha content" }] }],
    };
    const kbs = new ZoryaKnowledgeBases(config);
    await kbs.start();
    expect(retriever.events).toEqual(["setup", "add:one"]);
    expect(registry.get("docs")?.retriever).toBe(retriever);

    // A restart loads the base from the store: no re-seed, no replay.
    retriever.events.length = 0;
    await new ZoryaKnowledgeBases(config).start();
    expect(retriever.events).toEqual(["setup"]);
  });

  it("removing a base drops every source from a durable retriever", async () => {
    const retriever = new RecordingRetriever();
    retriever.ready = true;
    const kbs = new ZoryaKnowledgeBases({ retrievers: { durable: true, create: () => retriever } });
    await kbs.create({ id: "docs" });
    await kbs.ingest({ baseId: "docs", source: { id: "a", text: "alpha" } });
    await kbs.ingest({ baseId: "docs", source: { id: "b", text: "beta" } });
    retriever.events.length = 0;
    await kbs.remove("docs");
    expect(retriever.events.sort()).toEqual(["delete:a", "delete:b"]);
  });

  it("records a failing seed document without failing start", async () => {
    const kbs = new ZoryaKnowledgeBases({
      embeddings: fakeEmbeddings({ fail: () => true }),
      seed: [{ id: "docs", documents: [{ id: "one", text: "alpha" }] }],
    });
    await kbs.start();
    expect(await kbs.get("docs")).toMatchObject({ status: "degraded", sourceCount: 1 });
  });

  it("retries a failed load on the next call", async () => {
    let attempts = 0;
    const inner = new InMemoryKnowledgeBaseStore();
    const flaky: KnowledgeBaseStore = {
      loadAll: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("store down");
        return inner.loadAll();
      },
      saveBase: (b) => inner.saveBase(b),
      deleteBase: (id) => inner.deleteBase(id),
      saveSource: (p) => inner.saveSource(p),
      deleteSource: (p) => inner.deleteSource(p),
    };
    const kbs = new ZoryaKnowledgeBases({ store: flaky });
    await expect(kbs.start()).rejects.toThrow("store down");
    expect(await kbs.list()).toEqual([]);
    expect(attempts).toBe(2);
  });
});

function chunkOf(params: { document: KnowledgeIngestDocument; index: number }): KnowledgeChunk {
  const { document, index } = params;
  return {
    id: `${document.id}#${index}`,
    text: `${document.text} part ${index}`,
    index,
    source: { id: document.id },
  };
}
