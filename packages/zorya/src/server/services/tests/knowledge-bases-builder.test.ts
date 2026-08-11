import { describe, expect, it } from "bun:test";
import {
  InMemoryRetrieverRegistry,
  type KnowledgeChunk,
  type KnowledgeIngestDocument,
  type RetrieveRequest,
  type RetrieveResult,
  type Retriever,
} from "@promin/agent";
import { createZoryaKnowledgeBasesBuilder } from "../knowledge-bases.ts";

describe("ZoryaKnowledgeBasesBuilder", () => {
  it("shares the registry and runs retriever setup before seed ingestion", async () => {
    const events: string[] = [];
    const registry = new InMemoryRetrieverRegistry();
    const retriever = new SetupRequiredRetriever(events);

    const knowledgeBases = createZoryaKnowledgeBasesBuilder()
      .registry(registry)
      .retrieverFactory(() => retriever)
      .retrieverSetup(async (_definition, r) => {
        events.push("setup");
        (r as SetupRequiredRetriever).ready = true;
      })
      .initial({
        id: "docs",
        namespace: "default",
        documents: [{ id: "one", text: "alpha content" }],
      })
      .build();

    await knowledgeBases.ready();

    expect(events).toEqual(["setup", "add:one"]);
    expect(registry.get("docs")?.retriever).toBe(retriever);
    expect(
      (await knowledgeBases.search("default", "docs", { query: "alpha" }))[0]?.chunk.source.id,
    ).toBe("one");
  });
});

class SetupRequiredRetriever implements Retriever {
  ready = false;
  private chunks: KnowledgeChunk[] = [];

  constructor(private readonly events: string[]) {}

  async addDocument(document: KnowledgeIngestDocument): Promise<KnowledgeChunk[]> {
    if (!this.ready) throw new Error("retriever_not_ready");
    this.events.push(`add:${document.id}`);
    const chunk: KnowledgeChunk = {
      id: `${document.id}#0`,
      text: document.text,
      index: 0,
      source: { id: document.id },
      metadata: {},
    };
    this.chunks.push(chunk);
    return [chunk];
  }

  async retrieve(_request: RetrieveRequest): Promise<RetrieveResult[]> {
    return this.chunks.map((chunk) => ({ chunk, score: 1 }));
  }
}
