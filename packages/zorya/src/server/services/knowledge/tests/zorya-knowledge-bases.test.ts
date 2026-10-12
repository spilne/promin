import { describe, expect, it } from "bun:test";
import {
  InMemoryKnowledgeBaseStore,
  InMemoryRetriever,
  InMemoryRetrieverRegistry,
  deletesVersion,
  type DeleteSourceRequest,
  type EmbeddingProvider,
  type IngestibleRetriever,
  type KnowledgeBaseStore,
  type KnowledgeChunk,
  type KnowledgeIngestDocument,
  type KnowledgeSourceRecord,
  type RetrieveResult,
} from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import {
  KnowledgeBaseExistsError,
  KnowledgeBaseNotFoundError,
  KnowledgeInputError,
  KnowledgeSourceNotFoundError,
} from "../errors.ts";
import {
  ZoryaKnowledgeBases,
  type KnowledgeSourceAdapter,
  type ZoryaKnowledgeBasesConfig,
} from "../zorya-knowledge-bases.ts";

const NS = "acme";

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
      id: `${document.id}@${document.version}#0`,
      text: document.text,
      index: 0,
      source: {
        id: document.id,
        ...(document.version !== undefined && { version: document.version }),
      },
    };
    this.chunks.push(chunk);
    return [chunk];
  }

  async deleteSource(request: DeleteSourceRequest): Promise<void> {
    this.events.push(`delete:${request.sourceId}`);
    this.chunks = this.chunks.filter(
      (c) =>
        c.source.id !== request.sourceId || !deletesVersion({ request, version: c.source.version }),
    );
  }

  async retrieve(): Promise<RetrieveResult[]> {
    return this.chunks.map((chunk) => ({ chunk, score: 1 }));
  }
}

/** A service with its timers on a fake clock that nothing advances. */
function service(config: ZoryaKnowledgeBasesConfig = {}): ZoryaKnowledgeBases {
  return new ZoryaKnowledgeBases({ clock: FakeWallClock.create(1_000), ...config });
}

/** Yield to the event loop until `check` holds, at most `rounds` times. */
async function until(check: () => Promise<boolean>, rounds = 200): Promise<void> {
  for (let i = 0; i < rounds; i += 1) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("condition not reached");
}

function pendingRecord(params: {
  id: string;
  text: string;
  updatedAt?: number;
}): KnowledgeSourceRecord {
  return {
    id: params.id,
    tags: [],
    metadata: {},
    text: params.text,
    status: "pending",
    chunkCount: 0,
    createdAt: 0,
    updatedAt: params.updatedAt ?? 0,
  };
}

describe("ZoryaKnowledgeBases — bases", () => {
  it("creates, lists, updates and removes a base, mirroring it in the registry", async () => {
    const clock = FakeWallClock.create(1_000);
    const registry = new InMemoryRetrieverRegistry();
    const kbs = new ZoryaKnowledgeBases({ registry, clock });

    const created = await kbs.create({
      namespaceId: NS,
      id: "docs",
      description: "Product docs",
      tags: [" internal ", "internal", ""],
    });
    expect(created).toMatchObject({
      namespaceId: NS,
      id: "docs",
      description: "Product docs",
      tags: ["internal"],
      status: "ready",
      sourceCount: 0,
      chunkCount: 0,
      createdAt: 1_000,
    });
    expect(registry.get({ id: "docs", namespaceId: NS })?.description).toBe("Product docs");
    // Not a global row: other namespaces don't see it.
    expect(registry.get({ id: "docs" })).toBeNull();
    expect(registry.get({ id: "docs", namespaceId: "beta" })).toBeNull();

    clock.advance(500);
    const updated = await kbs.update({
      namespaceId: NS,
      id: "docs",
      patch: { description: null, metadata: { owner: "platform" } },
    });
    expect(updated.description).toBeUndefined();
    expect(updated.metadata).toEqual({ owner: "platform" });
    expect(updated.updatedAt).toBe(1_500);
    expect(registry.get({ id: "docs", namespaceId: NS })?.description).toBeUndefined();
    expect(registry.get({ id: "docs", namespaceId: NS })?.metadata).toEqual({ owner: "platform" });

    expect((await kbs.list({ namespaceId: NS })).map((b) => b.id)).toEqual(["docs"]);
    expect(await kbs.list({ namespaceId: "beta" })).toEqual([]);
    await kbs.remove({ namespaceId: NS, id: "docs" });
    expect(await kbs.list({ namespaceId: NS })).toEqual([]);
    expect(registry.get({ id: "docs", namespaceId: NS })).toBeNull();
    await kbs.stop();
  });

  it("keeps the same id in two namespaces apart", async () => {
    const registry = new InMemoryRetrieverRegistry();
    const kbs = service({ registry });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.create({ namespaceId: "beta", id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await kbs.ingest({ namespaceId: "beta", baseId: "docs", source: { id: "s", text: "beta" } });
    await kbs.drain();

    const acme = await kbs.search({ namespaceId: NS, baseId: "docs", request: { query: "alpha" } });
    expect(acme.map((r) => r.chunk.text)).toEqual(["alpha"]);
    expect(
      await kbs.search({ namespaceId: "beta", baseId: "docs", request: { query: "alpha" } }),
    ).toEqual([]);
    expect(registry.get({ id: "docs", namespaceId: NS })?.retriever).not.toBe(
      registry.get({ id: "docs", namespaceId: "beta" })?.retriever,
    );

    await kbs.remove({ namespaceId: NS, id: "docs" });
    expect(await kbs.get({ namespaceId: "beta", id: "docs" })).toMatchObject({ sourceCount: 1 });
    await kbs.stop();
  });

  it("refuses a taken id, including one a host-wired retriever holds", async () => {
    const registry = new InMemoryRetrieverRegistry([
      { id: "host", retriever: { retrieve: async () => [] } },
    ]);
    const kbs = service({ registry });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await expect(kbs.create({ namespaceId: NS, id: "docs" })).rejects.toBeInstanceOf(
      KnowledgeBaseExistsError,
    );
    await expect(kbs.create({ namespaceId: NS, id: "host" })).rejects.toThrow(
      "already registered by the host",
    );
    await kbs.stop();
  });

  it("refuses a taken id another process created", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const a = service({ store });
    const b = service({ store });
    await b.start();
    await a.create({ namespaceId: NS, id: "docs" });
    await expect(b.create({ namespaceId: NS, id: "docs" })).rejects.toBeInstanceOf(
      KnowledgeBaseExistsError,
    );
    await a.stop();
    await b.stop();
  });

  it("refuses ids recipes could not bind and empty namespaces", async () => {
    const kbs = service();
    for (const id of ["", "_reserved", "has space", "x".repeat(129)]) {
      await expect(kbs.create({ namespaceId: NS, id })).rejects.toBeInstanceOf(KnowledgeInputError);
    }
    await expect(kbs.create({ namespaceId: " ", id: "docs" })).rejects.toMatchObject({
      code: "invalid_namespace",
    });
    await kbs.stop();
  });

  it("answers not-found for an unknown base", async () => {
    const kbs = service();
    await kbs.create({ namespaceId: "beta", id: "ghost" });
    expect(await kbs.get({ namespaceId: NS, id: "ghost" })).toBeNull();
    await expect(kbs.listSources({ namespaceId: NS, baseId: "ghost" })).rejects.toBeInstanceOf(
      KnowledgeBaseNotFoundError,
    );
    await expect(
      kbs.ingest({ namespaceId: NS, baseId: "ghost", source: { id: "s", text: "x" } }),
    ).rejects.toBeInstanceOf(KnowledgeBaseNotFoundError);
    await kbs.stop();
  });
});

describe("ZoryaKnowledgeBases — background ingestion", () => {
  it("queues a source, ingests it in the background, and replaces a re-ingested one", async () => {
    const kbs = service({ embeddings: fakeEmbeddings() });
    await kbs.create({ namespaceId: NS, id: "docs" });
    const queued = await kbs.ingest({
      namespaceId: NS,
      baseId: "docs",
      source: {
        id: "runbook",
        title: "Runbook",
        tags: ["ops"],
        text: "Restart workers when heartbeat lag exceeds the threshold.",
      },
    });
    expect(queued).toMatchObject({ id: "runbook", status: "pending", chunkCount: 0 });
    expect(queued).not.toHaveProperty("text");

    await kbs.drain();
    const [ready] = await kbs.listSources({ namespaceId: NS, baseId: "docs" });
    expect(ready).toMatchObject({ id: "runbook", status: "ready", chunkCount: 1 });
    expect(ready).not.toHaveProperty("text");
    const hits = await kbs.search({
      namespaceId: NS,
      baseId: "docs",
      request: { query: "heartbeat" },
    });
    expect(hits[0]?.chunk.source).toMatchObject({ id: "runbook", title: "Runbook" });

    await kbs.ingest({
      namespaceId: NS,
      baseId: "docs",
      source: { id: "runbook", title: "Updated runbook", text: "Rollback after a failed deploy." },
    });
    await kbs.drain();
    const replaced = await kbs.search({
      namespaceId: NS,
      baseId: "docs",
      request: { query: "deploy rollback" },
    });
    expect(replaced).toHaveLength(1);
    expect(replaced[0]?.chunk.source.title).toBe("Updated runbook");
    expect(
      await kbs.search({ namespaceId: NS, baseId: "docs", request: { query: "heartbeat" } }),
    ).toEqual([]);
    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({
      status: "ready",
      sourceCount: 1,
      chunkCount: 1,
    });
    await kbs.stop();
  });

  it("shows a base as ingesting while a source waits, with no chunks for it yet", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const inner = new InMemoryRetriever();
    const gated: IngestibleRetriever = {
      retrieve: (r) => inner.retrieve(r),
      deleteSource: (id) => inner.deleteSource(id),
      addDocument: async (d) => {
        await gate;
        return inner.addDocument(d);
      },
    };
    const kbs = service({ retrievers: { durable: true, create: () => gated } });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });

    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({ status: "ingesting" });
    expect((await kbs.chunks({ namespaceId: NS, baseId: "docs" })).total).toBe(0);
    release();
    await kbs.drain();
    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({ status: "ready" });
    await kbs.stop();
  });

  it("records a failed ingestion with its reason, drops partial chunks and degrades the base", async () => {
    let failing = false;
    const store = new InMemoryKnowledgeBaseStore();
    const kbs = service({
      store,
      embeddings: fakeEmbeddings({ fail: () => failing }),
      chunker: { chunk: (d) => [0, 1].map((index) => chunkOf({ document: d, index })) },
    });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "ok", text: "alpha" } });
    await kbs.drain();

    failing = true;
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "bad", text: "beta" } });
    await kbs.drain();

    const bad = (await kbs.listSources({ namespaceId: NS, baseId: "docs" })).find(
      (s) => s.id === "bad",
    );
    expect(bad).toMatchObject({
      status: "failed",
      error: "embedding provider unavailable",
      chunkCount: 0,
    });
    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({
      status: "degraded",
      chunkCount: 2,
    });
    // The failure is durable, so every process and a restart show it.
    const stored = await store.getBase({ namespaceId: NS, id: "docs" });
    expect(stored!.sources.find((s) => s.id === "bad")?.status).toBe("failed");

    failing = false;
    expect(
      await kbs.search({ namespaceId: NS, baseId: "docs", request: { query: "beta" } }),
    ).toEqual([]);
    const requeued = await kbs.reindexSource({ namespaceId: NS, baseId: "docs", sourceId: "bad" });
    expect(requeued.status).toBe("pending");
    expect(requeued).not.toHaveProperty("error");
    await kbs.drain();
    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({
      status: "ready",
      chunkCount: 4,
    });
    await kbs.stop();
  });

  it("validates sources before queueing", async () => {
    const kbs = service({ maxSourceCharacters: 10 });
    await kbs.create({ namespaceId: NS, id: "docs" });
    for (const source of [
      { id: "  ", text: "x" },
      { id: "s", text: "   " },
      { id: "s", text: "x".repeat(11) },
    ]) {
      await expect(kbs.ingest({ namespaceId: NS, baseId: "docs", source })).rejects.toBeInstanceOf(
        KnowledgeInputError,
      );
    }
    expect(await kbs.listSources({ namespaceId: NS, baseId: "docs" })).toEqual([]);
    await kbs.stop();
  });

  it("removes a source and its chunks", async () => {
    const kbs = service();
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({
      namespaceId: NS,
      baseId: "docs",
      source: { id: "one", text: "alpha content" },
    });
    await kbs.drain();
    await kbs.removeSource({ namespaceId: NS, baseId: "docs", sourceId: "one" });
    expect(await kbs.listSources({ namespaceId: NS, baseId: "docs" })).toEqual([]);
    expect(
      await kbs.search({ namespaceId: NS, baseId: "docs", request: { query: "alpha" } }),
    ).toEqual([]);
    await expect(
      kbs.removeSource({ namespaceId: NS, baseId: "docs", sourceId: "one" }),
    ).rejects.toBeInstanceOf(KnowledgeSourceNotFoundError);
    await kbs.stop();
  });

  it("a source removed while it is being ingested leaves no chunks behind", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const inner = new InMemoryRetriever();
    let started = false;
    const slow: IngestibleRetriever = {
      retrieve: (r) => inner.retrieve(r),
      deleteSource: (id) => inner.deleteSource(id),
      addDocument: async (d) => {
        started = true;
        await gate;
        return inner.addDocument(d);
      },
    };
    const kbs = service({ retrievers: { durable: true, create: () => slow } });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await until(async () => started);
    // The remove waits for the ingestion's lock, then drops the source.
    const removing = kbs.removeSource({ namespaceId: NS, baseId: "docs", sourceId: "s" });
    release();
    await removing;
    await kbs.drain();
    expect(await inner.retrieve({ query: "alpha" })).toEqual([]);
    expect(await kbs.listSources({ namespaceId: NS, baseId: "docs" })).toEqual([]);
    await kbs.stop();
  });

  it("pages the chunks of ready sources, optionally for one source", async () => {
    const kbs = service({
      chunker: { chunk: (d) => [0, 1, 2].map((index) => chunkOf({ document: d, index })) },
    });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "b", text: "beta" } });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "a", text: "alpha" } });
    await kbs.drain();

    const page = await kbs.chunks({ namespaceId: NS, baseId: "docs", limit: 2, offset: 2 });
    expect(page.total).toBe(6);
    expect(page.chunks.map((c) => c.id)).toEqual(["a#2", "b#0"]);
    const one = await kbs.chunks({ namespaceId: NS, baseId: "docs", sourceId: "b" });
    expect(one.chunks.map((c) => c.id)).toEqual(["b#0", "b#1", "b#2"]);
    await expect(
      kbs.chunks({ namespaceId: NS, baseId: "docs", sourceId: "ghost" }),
    ).rejects.toBeInstanceOf(KnowledgeSourceNotFoundError);
    await kbs.stop();
  });

  it("ingests at most `concurrency` sources at once", async () => {
    let running = 0;
    let peak = 0;
    const factory = {
      durable: true,
      create: (): IngestibleRetriever => {
        const inner = new InMemoryRetriever();
        return {
          retrieve: (r) => inner.retrieve(r),
          deleteSource: (id) => inner.deleteSource(id),
          addDocument: async (d) => {
            running += 1;
            peak = Math.max(peak, running);
            await new Promise((resolve) => setTimeout(resolve, 2));
            running -= 1;
            return inner.addDocument(d);
          },
        };
      },
    };
    const kbs = service({ retrievers: factory, ingestion: { concurrency: 2 } });
    for (const id of ["a", "b", "c"]) {
      await kbs.create({ namespaceId: NS, id });
      for (const sourceId of ["1", "2"]) {
        await kbs.ingest({ namespaceId: NS, baseId: id, source: { id: sourceId, text: "alpha" } });
      }
    }
    await kbs.drain();
    expect(peak).toBe(2);
    for (const id of ["a", "b", "c"]) {
      expect(await kbs.get({ namespaceId: NS, id })).toMatchObject({
        status: "ready",
        chunkCount: 2,
      });
    }
    await kbs.stop();
  });

  it("imports through a registered adapter, queueing the documents, and refuses unknown kinds", async () => {
    const fixture: KnowledgeSourceAdapter = {
      kind: "fixture",
      load: async ({ config }) => [{ id: String(config), text: "adapter content" }],
    };
    const kbs = service({ sourceAdapters: [fixture] });
    await kbs.create({ namespaceId: NS, id: "docs" });
    expect(kbs.sourceKinds()).toEqual(["fixture"]);
    const sources = await kbs.importSources({
      namespaceId: NS,
      baseId: "docs",
      kind: "fixture",
      config: "src-1",
    });
    expect(sources.map((s) => [s.id, s.status])).toEqual([["src-1", "pending"]]);
    await kbs.drain();
    const hits = await kbs.search({
      namespaceId: NS,
      baseId: "docs",
      request: { query: "adapter content" },
    });
    expect(hits[0]?.chunk.source.id).toBe("src-1");
    await expect(
      kbs.importSources({ namespaceId: NS, baseId: "docs", kind: "nope", config: {} }),
    ).rejects.toMatchObject({ code: "unknown_source_kind" });
    await kbs.stop();
  });

  it("the poll picks up sources another process queued", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    const kbs = new ZoryaKnowledgeBases({
      store,
      clock,
      ingestion: { pollIntervalMs: 1_000 },
      refreshIntervalMs: 0,
    });
    await kbs.create({ namespaceId: NS, id: "docs" });
    // Another process writes a queued source straight to the shared store.
    await store.saveSource({
      namespaceId: NS,
      baseId: "docs",
      source: pendingRecord({ id: "s", text: "alpha" }),
    });
    const status = async () =>
      (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }))?.status;
    for (let i = 0; i < 20; i += 1) await new Promise((resolve) => setTimeout(resolve, 0));
    expect(await status()).toBe("pending");

    clock.advance(1_000);
    await until(async () => (await status()) === "ready");
    await kbs.stop();
  });

  it("stop waits for ingestions in flight and claims nothing afterwards", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const inner = new InMemoryRetriever();
    let started = false;
    const slow: IngestibleRetriever = {
      retrieve: (r) => inner.retrieve(r),
      deleteSource: (id) => inner.deleteSource(id),
      addDocument: async (d) => {
        started = true;
        await gate;
        return inner.addDocument(d);
      },
    };
    const store = new InMemoryKnowledgeBaseStore();
    const kbs = service({ store, retrievers: { durable: true, create: () => slow } });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await until(async () => started);
    let stopped = false;
    const stopping = kbs.stop().then(() => (stopped = true));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(stopped).toBe(false);
    release();
    await stopping;
    expect(
      (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }))?.status,
    ).toBe("ready");

    await store.saveSource({
      namespaceId: NS,
      baseId: "docs",
      source: pendingRecord({ id: "later", text: "beta" }),
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(
      (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "later" }))?.status,
    ).toBe("pending");
  });
});

describe("ZoryaKnowledgeBases — restarts and several processes", () => {
  it("rebuilds a non-durable index from the store", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const first = service({ store });
    await first.create({ namespaceId: NS, id: "docs" });
    await first.ingest({
      namespaceId: NS,
      baseId: "docs",
      source: { id: "one", text: "alpha content" },
    });
    await first.drain();
    await first.stop();

    const registry = new InMemoryRetrieverRegistry();
    const second = service({ store, registry });
    await second.start();
    expect((await second.listSources({ namespaceId: NS, baseId: "docs" }))[0]?.id).toBe("one");
    expect(registry.get({ id: "docs", namespaceId: NS })).not.toBeNull();
    const hits = await second.search({
      namespaceId: NS,
      baseId: "docs",
      request: { query: "alpha" },
    });
    expect(hits[0]?.chunk.source.id).toBe("one");
    await second.stop();
  });

  it("resumes queued sources after a restart", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const crashed = service({ store, ingestion: { concurrency: 1 } });
    await crashed.create({ namespaceId: NS, id: "docs" });
    // Stopped before its worker claimed anything: the source stays queued.
    await crashed.stop();
    await store.saveSource({
      namespaceId: NS,
      baseId: "docs",
      source: pendingRecord({ id: "s", text: "alpha" }),
    });

    const restarted = service({ store });
    await restarted.drain();
    expect(await restarted.listSources({ namespaceId: NS, baseId: "docs" })).toMatchObject([
      { id: "s", status: "ready", chunkCount: 1 },
    ]);
    await restarted.stop();
  });

  it("a worker restarting under its id resumes its interrupted ingestions at once", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    await store.saveBase({
      namespaceId: NS,
      id: "docs",
      tags: [],
      metadata: {},
      createdAt: 0,
      updatedAt: 0,
    });
    await store.saveSource({
      namespaceId: NS,
      baseId: "docs",
      source: pendingRecord({ id: "s", text: "alpha" }),
    });
    // The worker claimed it, then its process died.
    await store.claimSource({ workerId: "worker-1", staleAfterMs: 60_000 });

    const restarted = service({ store, ingestion: { workerId: "worker-1" } });
    await restarted.drain();
    expect(
      (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }))?.status,
    ).toBe("ready");
    await restarted.stop();
  });

  it("another worker takes over an ingestion orphaned for longer than staleAfterMs", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    await store.saveBase({
      namespaceId: NS,
      id: "docs",
      tags: [],
      metadata: {},
      createdAt: 0,
      updatedAt: 0,
    });
    await store.saveSource({
      namespaceId: NS,
      baseId: "docs",
      source: pendingRecord({ id: "s", text: "alpha" }),
    });
    await store.claimSource({ workerId: "dead", staleAfterMs: 60_000 });

    const kbs = new ZoryaKnowledgeBases({
      store,
      clock,
      ingestion: { staleAfterMs: 60_000, pollIntervalMs: 0 },
      refreshIntervalMs: 0,
    });
    await kbs.drain();
    const status = async () =>
      (await store.getSource({ namespaceId: NS, baseId: "docs", sourceId: "s" }))?.status;
    expect(await status()).toBe("ingesting");

    clock.advance(60_001);
    await kbs.drain();
    expect(await status()).toBe("ready");
    await kbs.stop();
  });

  it("a base created by one process resolves in another without a restart", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    const a = new ZoryaKnowledgeBases({ store, clock });
    const registryB = new InMemoryRetrieverRegistry();
    const b = new ZoryaKnowledgeBases({ store, clock, registry: registryB });
    await b.start();

    await a.create({ namespaceId: NS, id: "docs" });
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await a.drain();

    // A registry miss is re-read from the store on use…
    expect(registryB.get({ id: "docs", namespaceId: NS })).toBeNull();
    await b.ensure({ namespaceId: NS, ids: ["docs"] });
    const resolved = registryB.get({ id: "docs", namespaceId: NS });
    expect(resolved).not.toBeNull();
    expect((await resolved!.retriever.retrieve({ query: "alpha" }))[0]?.chunk.source.id).toBe("s");
    await a.stop();
    await b.stop();
  });

  it("the periodic re-read brings other processes' changes into the registry", async () => {
    const clock = FakeWallClock.create(0);
    const store = new InMemoryKnowledgeBaseStore({ clock });
    const a = new ZoryaKnowledgeBases({ store, clock });
    const registryB = new InMemoryRetrieverRegistry();
    const b = new ZoryaKnowledgeBases({
      store,
      clock,
      registry: registryB,
      refreshIntervalMs: 5_000,
    });
    await b.start();
    const rowB = () => registryB.get({ id: "docs", namespaceId: NS });

    await a.create({ namespaceId: NS, id: "docs" });
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await a.drain();
    expect(rowB()).toBeNull();

    clock.advance(5_000);
    await until(async () => (await rowB()?.retriever.retrieve({ query: "alpha" }))?.length === 1);

    // A new source version reaches B's index too, and a delete drops the row.
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "beta" } });
    await a.drain();
    clock.advance(5_000);
    await until(async () => (await rowB()?.retriever.retrieve({ query: "beta" }))?.length === 1);
    expect(await rowB()!.retriever.retrieve({ query: "alpha" })).toEqual([]);

    await a.remove({ namespaceId: NS, id: "docs" });
    clock.advance(5_000);
    await until(async () => rowB() === null);
    await a.stop();
    await b.stop();
  });

  it("reads show source states another process wrote", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const a = service({ store });
    const b = service({ store });
    await a.create({ namespaceId: NS, id: "docs" });
    await b.start();
    expect(await b.get({ namespaceId: NS, id: "docs" })).toMatchObject({ sourceCount: 0 });
    await a.ingest({ namespaceId: NS, baseId: "docs", source: { id: "s", text: "alpha" } });
    await a.drain();
    expect(await b.listSources({ namespaceId: NS, baseId: "docs" })).toMatchObject([
      { id: "s", status: "ready" },
    ]);
    expect((await b.list({ namespaceId: NS }))[0]).toMatchObject({ sourceCount: 1 });
    await a.stop();
    await b.stop();
  });

  it("shows a source failed here when indexing it locally fails, and keeps starting", async () => {
    const store = new InMemoryKnowledgeBaseStore();
    const first = service({ store });
    await first.create({ namespaceId: NS, id: "docs" });
    await first.ingest({ namespaceId: NS, baseId: "docs", source: { id: "one", text: "alpha" } });
    await first.drain();
    await first.stop();

    const embeddings = fakeEmbeddings({ fail: () => true });
    const second = service({ store, embeddings });
    await second.start();
    expect((await second.listSources({ namespaceId: NS, baseId: "docs" }))[0]).toMatchObject({
      status: "failed",
      error: "embedding provider unavailable",
    });
    // The same version is not retried on every read.
    const calls = embeddings.calls;
    await second.listSources({ namespaceId: NS, baseId: "docs" });
    await second.refresh();
    expect(embeddings.calls).toBe(calls);
    await second.stop();
  });

  it("does not replay a durable retriever and builds it before seed ingestion", async () => {
    const retriever = new RecordingRetriever();
    const registry = new InMemoryRetrieverRegistry();
    const store = new InMemoryKnowledgeBaseStore();
    const config: ZoryaKnowledgeBasesConfig = {
      store,
      registry,
      clock: FakeWallClock.create(0),
      retrievers: {
        durable: true,
        create: async () => {
          retriever.events.push("setup");
          retriever.ready = true;
          return retriever;
        },
      },
      seed: [{ namespaceId: NS, id: "docs", documents: [{ id: "one", text: "alpha content" }] }],
    };
    const kbs = new ZoryaKnowledgeBases(config);
    await kbs.drain();
    // The new version goes in first; then every other version is dropped.
    expect(retriever.events).toEqual(["setup", "add:one", "delete:one"]);
    expect(registry.get({ id: "docs", namespaceId: NS })?.retriever).toBe(retriever);
    await kbs.stop();

    // A restart loads the base from the store: no re-seed, no replay.
    retriever.events.length = 0;
    const restarted = new ZoryaKnowledgeBases(config);
    await restarted.drain();
    expect(retriever.events).toEqual(["setup"]);
    await restarted.stop();
  });

  it("removing a base drops every source from a durable retriever", async () => {
    const retriever = new RecordingRetriever();
    retriever.ready = true;
    const kbs = service({ retrievers: { durable: true, create: () => retriever } });
    await kbs.create({ namespaceId: NS, id: "docs" });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "a", text: "alpha" } });
    await kbs.ingest({ namespaceId: NS, baseId: "docs", source: { id: "b", text: "beta" } });
    await kbs.drain();
    retriever.events.length = 0;
    await kbs.remove({ namespaceId: NS, id: "docs" });
    expect(retriever.events.sort()).toEqual(["delete:a", "delete:b"]);
    await kbs.stop();
  });

  it("records a failing seed document without failing start", async () => {
    const kbs = service({
      embeddings: fakeEmbeddings({ fail: () => true }),
      seed: [{ namespaceId: NS, id: "docs", documents: [{ id: "one", text: "alpha" }] }],
    });
    await kbs.drain();
    expect(await kbs.get({ namespaceId: NS, id: "docs" })).toMatchObject({
      status: "degraded",
      sourceCount: 1,
    });
    await kbs.stop();
  });

  it("retries a failed load on the next call", async () => {
    let attempts = 0;
    const inner = new InMemoryKnowledgeBaseStore();
    const flaky: KnowledgeBaseStore = {
      listBases: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("store down");
        return inner.listBases();
      },
      getBase: (k) => inner.getBase(k),
      getSource: (k) => inner.getSource(k),
      saveBase: (b) => inner.saveBase(b),
      deleteBase: (k) => inner.deleteBase(k),
      saveSource: (p) => inner.saveSource(p),
      deleteSource: (k) => inner.deleteSource(k),
      claimSource: (p) => inner.claimSource(p),
      completeSource: (p) => inner.completeSource(p),
      releaseClaims: (p) => inner.releaseClaims(p),
      heartbeatClaim: (p) => inner.heartbeatClaim(p),
    };
    const kbs = service({ store: flaky });
    await expect(kbs.start()).rejects.toThrow("store down");
    expect(await kbs.list({ namespaceId: NS })).toEqual([]);
    expect(attempts).toBeGreaterThanOrEqual(2);
    await kbs.stop();
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
