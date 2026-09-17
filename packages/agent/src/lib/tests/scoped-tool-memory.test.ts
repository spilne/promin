// ---------------------------------------------------------------------------
// createScopedTool / createElevatedTool — declarative scoped-memory
// injection.
// Pinned cases:
//   1. No memory declared → ctx.memory is undefined
//   2. Declared (default scope) → ctx.memory binds to the resource tier
//   3. recordFact + listFacts round-trip at resource scope
//   4. recordEpisode + listEpisodes round-trip
//   5. scope 'namespace' writes land in namespace memory, not resource
//   6. scope 'thread' writes land in thread memory; requires a threadId
//   7. scope 'thread' without a threadId → throws clearly with tool name
//   8. tenant isolation — a tool can't reach another scope's memory
//   9. elevated tools receive ctx.memory too
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { createElevatedTool, createScopedTool } from "../tool.ts";
import { InMemoryMemoryStore } from "../memory/in-memory-memory-store.ts";

const scope = { namespaceId: "acme", resourceId: "alice", threadId: "t-1" };

describe("createScopedTool — declarative scoped memory", () => {
  it("ctx.memory is undefined when no memory declared", async () => {
    const t = createScopedTool({
      name: "noMemory",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => ctx.memory === undefined,
    });
    expect(await t.execute({}, { scope })).toBe(true);
  });

  it("binds ctx.memory to the resource tier by default", async () => {
    const store = new InMemoryMemoryStore();
    const t = createScopedTool({
      name: "remember",
      description: "test",
      parameters: z.object({ text: z.string() }),
      memory: { store },
      execute: async (input, ctx) => {
        await ctx.memory!.recordFact(input.text);
      },
    });
    await t.execute({ text: "alice likes blue" }, { scope });

    // Landed in resource memory for (acme, alice).
    const resourceFacts = await store.listResourceFacts({
      namespaceId: "acme",
      resourceId: "alice",
    });
    expect(resourceFacts.map((f) => f.text)).toEqual(["alice likes blue"]);
  });

  it("recordFact + listFacts round-trip through ctx.memory", async () => {
    const store = new InMemoryMemoryStore();
    const t = createScopedTool({
      name: "factRoundtrip",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "resource" },
      execute: async (_input, ctx) => {
        await ctx.memory!.recordFact("first");
        await ctx.memory!.recordFact("second");
        return (await ctx.memory!.listFacts()).map((f) => f.text);
      },
    });
    expect(await t.execute({}, { scope })).toEqual(["first", "second"]);
  });

  it("recordEpisode + listEpisodes round-trip through ctx.memory", async () => {
    const store = new InMemoryMemoryStore();
    const t = createScopedTool({
      name: "episodeRoundtrip",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "resource" },
      execute: async (_input, ctx) => {
        await ctx.memory!.recordEpisode({ summary: "posted to slack", salience: 0.8 });
        const episodes = await ctx.memory!.listEpisodes();
        return episodes.map((e) => e.summary);
      },
    });
    expect(await t.execute({}, { scope })).toEqual(["posted to slack"]);
  });

  it("scope 'namespace' writes land in namespace memory, not resource", async () => {
    const store = new InMemoryMemoryStore();
    const t = createScopedTool({
      name: "nsMemory",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "namespace" },
      execute: async (_input, ctx) => {
        await ctx.memory!.recordFact("org-wide policy");
      },
    });
    await t.execute({}, { scope });

    expect((await store.listNamespaceFacts("acme")).map((f) => f.text)).toEqual([
      "org-wide policy",
    ]);
    expect(await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" })).toEqual([]);
  });

  it("scope 'thread' writes land in thread memory", async () => {
    const store = new InMemoryMemoryStore();
    await store.createThread({ namespaceId: "acme", threadId: "t-1" });
    const t = createScopedTool({
      name: "threadMemory",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "thread" },
      execute: async (_input, ctx) => {
        await ctx.memory!.recordFact("said in this chat");
      },
    });
    await t.execute({}, { scope });

    expect(
      (await store.listThreadFacts({ namespaceId: "acme", threadId: "t-1" })).map((f) => f.text),
    ).toEqual(["said in this chat"]);
  });

  it("scope 'thread' without a threadId throws clearly", async () => {
    const store = new InMemoryMemoryStore();
    const t = createScopedTool({
      name: "needsThread",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "thread" },
      execute: async (_input, ctx) => ctx.memory!.recordFact("x"),
    });
    // Scope carries ns + resource but no threadId.
    await expect(
      t.execute({}, { scope: { namespaceId: "acme", resourceId: "alice" } }),
    ).rejects.toThrow(/needsThread.*thread.*threadId/s);
  });

  it("a tool's memory cannot reach another resource's facts", async () => {
    const store = new InMemoryMemoryStore();
    await store.appendResourceFact(
      { namespaceId: "acme", resourceId: "bob" },
      "bob's private note",
    );
    const t = createScopedTool({
      name: "isolated",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "resource" },
      execute: async (_input, ctx) => (await ctx.memory!.listFacts()).map((f) => f.text),
    });
    // Invoked as alice — bob's fact must not be visible.
    expect(await t.execute({}, { scope })).toEqual([]);
  });

  it("elevated tools receive ctx.memory too", async () => {
    const store = new InMemoryMemoryStore();
    const t = createElevatedTool({
      name: "elevatedMemory",
      description: "test",
      parameters: z.object({}),
      memory: { store, scope: "resource" },
      execute: async (_input, ctx) => {
        ctx.audit({ action: "test" });
        await ctx.memory!.recordFact("elevated wrote this");
        return ctx.memory !== undefined;
      },
    });
    expect(await t.execute({}, { scope })).toBe(true);
    expect(
      (await store.listResourceFacts({ namespaceId: "acme", resourceId: "alice" })).map(
        (f) => f.text,
      ),
    ).toEqual(["elevated wrote this"]);
  });
});
