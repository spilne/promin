// ---------------------------------------------------------------------------
// Portable `MemoryStore` conformance suite. Modeled on `storageTestSuite`.
//
// Usage:
//   import { memoryStoreTestSuite } from "@promin/agent/testing";
//   memoryStoreTestSuite(() => new InMemoryMemoryStore());
//
// All `MemoryStore` implementations (in-memory, Postgres, others) must
// pass this suite. Every test creates a fresh store via the factory.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { Message } from "../message.ts";
import { PROMPT_CACHE_BOUNDARY, type MemoryStore } from "./types.ts";

const NS = "ns-test";
const ALICE = "alice";
const BOB = "bob";
const T_DEFAULT = "thread-default";

function userMsg(content: string): Message {
  return { role: "user", content };
}

function assistantMsg(content: string): Message {
  return { role: "assistant", content };
}

const looseBudget = { maxMessageTokens: 1_000_000 };

export function memoryStoreTestSuite(factory: () => MemoryStore | Promise<MemoryStore>) {
  async function make(): Promise<MemoryStore> {
    return factory();
  }

  describe("MemoryStore conformance", () => {
    // --- Namespace ----------------------------------------------------
    describe("namespace CRUD", () => {
      it("returns null for missing namespace", async () => {
        const s = await make();
        expect(await s.getNamespace(NS)).toBeNull();
      });

      it("upsert creates and retrieves a namespace", async () => {
        const s = await make();
        const row = await s.upsertNamespace(NS, {
          staticRules: "rules",
          metadata: { tier: "pro" },
        });
        expect(row.namespaceId).toBe(NS);
        expect(row.staticRules).toBe("rules");
        expect(row.metadata.tier).toBe("pro");
        expect(await s.getNamespace(NS)).toMatchObject({ namespaceId: NS, staticRules: "rules" });
      });

      it("upsert patches existing fields without clobbering others", async () => {
        const s = await make();
        await s.upsertNamespace(NS, { staticRules: "v1", metadata: { x: 1 } });
        const after = await s.upsertNamespace(NS, { staticRules: "v2" });
        expect(after.staticRules).toBe("v2");
        expect(after.metadata.x).toBe(1);
      });
    });

    describe("namespace facts", () => {
      it("appendNamespaceFact creates parent row implicitly", async () => {
        const s = await make();
        const f = await s.appendNamespaceFact(NS, "default currency is USD");
        expect(f.id.length).toBeGreaterThan(0);
        expect(f.text).toBe("default currency is USD");
        expect(await s.getNamespace(NS)).not.toBeNull();
      });

      it("listNamespaceFacts returns facts in append order", async () => {
        const s = await make();
        await s.appendNamespaceFact(NS, "a");
        await s.appendNamespaceFact(NS, "b");
        await s.appendNamespaceFact(NS, "c");
        const facts = await s.listNamespaceFacts(NS);
        expect(facts.map((f) => f.text)).toEqual(["a", "b", "c"]);
      });

      it("deleteNamespaceFact removes only the targeted entry", async () => {
        const s = await make();
        const a = await s.appendNamespaceFact(NS, "a");
        await s.appendNamespaceFact(NS, "b");
        await s.deleteNamespaceFact(NS, a.id);
        const facts = await s.listNamespaceFacts(NS);
        expect(facts.map((f) => f.text)).toEqual(["b"]);
      });
    });

    // --- Resource -----------------------------------------------------
    describe("resource CRUD", () => {
      it("returns null for missing resource", async () => {
        const s = await make();
        expect(await s.getResource({ namespaceId: NS, resourceId: ALICE })).toBeNull();
      });

      it("upsert creates and retrieves a resource", async () => {
        const s = await make();
        const row = await s.upsertResource(
          { namespaceId: NS, resourceId: ALICE },
          { staticRules: "alice prefers terse" },
        );
        expect(row.resourceId).toBe(ALICE);
        expect(row.staticRules).toBe("alice prefers terse");
      });

      it("isolates resources within a namespace", async () => {
        const s = await make();
        await s.upsertResource({ namespaceId: NS, resourceId: ALICE }, { staticRules: "ALICE" });
        await s.upsertResource({ namespaceId: NS, resourceId: BOB }, { staticRules: "BOB" });
        const a = await s.getResource({ namespaceId: NS, resourceId: ALICE });
        const b = await s.getResource({ namespaceId: NS, resourceId: BOB });
        expect(a?.staticRules).toBe("ALICE");
        expect(b?.staticRules).toBe("BOB");
      });
    });

    describe("resource facts", () => {
      it("appendResourceFact creates parent row implicitly", async () => {
        const s = await make();
        await s.appendResourceFact({ namespaceId: NS, resourceId: ALICE }, "loves dogs");
        expect(await s.getResource({ namespaceId: NS, resourceId: ALICE })).not.toBeNull();
      });

      it("isolates facts per resource", async () => {
        const s = await make();
        await s.appendResourceFact({ namespaceId: NS, resourceId: ALICE }, "alice-fact");
        await s.appendResourceFact({ namespaceId: NS, resourceId: BOB }, "bob-fact");
        const aFacts = await s.listResourceFacts({ namespaceId: NS, resourceId: ALICE });
        const bFacts = await s.listResourceFacts({ namespaceId: NS, resourceId: BOB });
        expect(aFacts.map((f) => f.text)).toEqual(["alice-fact"]);
        expect(bFacts.map((f) => f.text)).toEqual(["bob-fact"]);
      });
    });

    // --- Thread -------------------------------------------------------
    describe("thread CRUD", () => {
      it("createThread returns the new row", async () => {
        const s = await make();
        const row = await s.createThread(
          { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT },
          { metadata: { topic: "billing" } },
        );
        expect(row.threadId).toBe(T_DEFAULT);
        expect(row.resourceId).toBe(ALICE);
        expect(row.metadata.topic).toBe("billing");
      });

      it("createThread on duplicate id throws", async () => {
        const s = await make();
        await s.createThread({ namespaceId: NS, threadId: T_DEFAULT });
        await expect(s.createThread({ namespaceId: NS, threadId: T_DEFAULT })).rejects.toThrow();
      });

      it("getThread returns null when absent", async () => {
        const s = await make();
        expect(await s.getThread({ namespaceId: NS, threadId: "nope" })).toBeNull();
      });

      it("setThreadWorking persists", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.setThreadWorking(key, "scratch");
        expect((await s.getThread(key))?.workingMemory).toBe("scratch");
      });

      it("setThreadInheritFromParent persists", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.setThreadInheritFromParent(key, false);
        expect((await s.getThread(key))?.inheritFromParent).toBe(false);
      });

      it("deleteThread removes thread, messages, and facts", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendMessages(key, [userMsg("hi")]);
        await s.appendThreadFact(key, "scratch fact");
        await s.deleteThread(key);
        expect(await s.getThread(key)).toBeNull();
        expect(await s.getMessages(key)).toEqual([]);
        expect(await s.listThreadFacts(key)).toEqual([]);
      });
    });

    describe("thread messages", () => {
      it("appendMessages assigns monotonic seq numbers + createdAt", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        const stored = await s.appendMessages(key, [userMsg("a"), assistantMsg("b")]);
        expect(stored.map((m) => m.seq)).toEqual([1, 2]);
        expect(stored.every((m) => typeof m.createdAt === "number")).toBe(true);
      });

      it("appendMessages auto-creates the thread", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: "auto-thread" };
        await s.appendMessages(key, [userMsg("a")]);
        expect(await s.getThread(key)).not.toBeNull();
      });

      it("getMessages returns oldest-first by default", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendMessages(key, [userMsg("a"), userMsg("b"), userMsg("c")]);
        const out = await s.getMessages(key);
        expect(out.map((m) => m.content)).toEqual(["a", "b", "c"]);
      });

      it("getMessages respects fromSeq + toSeq + limit + order", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendMessages(key, [userMsg("a"), userMsg("b"), userMsg("c"), userMsg("d")]);
        const slice = await s.getMessages(key, { fromSeq: 2, toSeq: 3 });
        expect(slice.map((m) => m.content)).toEqual(["b", "c"]);

        const desc = await s.getMessages(key, { order: "desc", limit: 2 });
        expect(desc.map((m) => m.content)).toEqual(["d", "c"]);
      });
    });

    describe("listThreads", () => {
      it("filters by resourceId", async () => {
        const s = await make();
        await s.createThread({ namespaceId: NS, resourceId: ALICE, threadId: "a-1" });
        await s.createThread({ namespaceId: NS, resourceId: ALICE, threadId: "a-2" });
        await s.createThread({ namespaceId: NS, resourceId: BOB, threadId: "b-1" });
        const aliceThreads = await s.listThreads({ namespaceId: NS, resourceId: ALICE });
        expect(aliceThreads.map((t) => t.threadId).sort()).toEqual(["a-1", "a-2"]);
      });

      it("filters by metadata subset", async () => {
        const s = await make();
        await s.createThread(
          { namespaceId: NS, threadId: "billing-1" },
          { metadata: { topic: "billing" } },
        );
        await s.createThread(
          { namespaceId: NS, threadId: "support-1" },
          { metadata: { topic: "support" } },
        );
        const billing = await s.listThreads({
          namespaceId: NS,
          metadataFilter: { topic: "billing" },
        });
        expect(billing.map((t) => t.threadId)).toEqual(["billing-1"]);
      });

      it("includes messageCount and lastActiveAt", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendMessages(key, [userMsg("a"), userMsg("b")]);
        const [t] = await s.listThreads({ namespaceId: NS });
        expect(t.messageCount).toBe(2);
        expect(typeof t.lastActiveAt).toBe("number");
      });
    });

    // --- Cascade severance --------------------------------------------
    describe("inheritFromParent severance", () => {
      it("namespace contributions appear when inherit is true", async () => {
        const s = await make();
        await s.upsertNamespace(NS, { staticRules: "NAMESPACE-RULES" });
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key);
        const ctx = await s.resolveContext(key, looseBudget);
        expect(ctx.systemPrompt).toContain("NAMESPACE-RULES");
      });

      it("resource.inheritFromParent=false cuts namespace from the cascade", async () => {
        const s = await make();
        await s.upsertNamespace(NS, { staticRules: "NAMESPACE-RULES" });
        await s.upsertResource(
          { namespaceId: NS, resourceId: ALICE },
          { staticRules: "RESOURCE-RULES", inheritFromParent: false },
        );
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key);
        const ctx = await s.resolveContext(key, looseBudget);
        expect(ctx.systemPrompt).not.toContain("NAMESPACE-RULES");
        expect(ctx.systemPrompt).toContain("RESOURCE-RULES");
      });

      it("thread.inheritFromParent=false cuts both namespace and resource", async () => {
        const s = await make();
        await s.upsertNamespace(NS, { staticRules: "NS" });
        await s.upsertResource({ namespaceId: NS, resourceId: ALICE }, { staticRules: "RES" });
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key, { inheritFromParent: false, workingMemory: "THREAD-ONLY" });
        const ctx = await s.resolveContext(key, looseBudget);
        expect(ctx.systemPrompt).not.toContain("NS");
        expect(ctx.systemPrompt).not.toContain("RES");
        expect(ctx.systemPrompt).toContain("THREAD-ONLY");
      });
    });

    // --- Cascade assembly --------------------------------------------
    describe("resolveContext", () => {
      it("includes facts from all enabled layers, dated", async () => {
        const s = await make();
        await s.appendNamespaceFact(NS, "ns-fact");
        await s.appendResourceFact({ namespaceId: NS, resourceId: ALICE }, "res-fact");
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendThreadFact(key, "thread-fact");
        const ctx = await s.resolveContext(key, looseBudget);
        expect(ctx.systemPrompt).toContain("ns-fact");
        expect(ctx.systemPrompt).toContain("res-fact");
        expect(ctx.systemPrompt).toContain("thread-fact");
      });

      it("emits the cache boundary marker between namespace and resource", async () => {
        const s = await make();
        await s.upsertNamespace(NS, { staticRules: "NS" });
        await s.upsertResource({ namespaceId: NS, resourceId: ALICE }, { staticRules: "RES" });
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key);
        const ctx = await s.resolveContext(key, looseBudget);

        const idxNs = ctx.systemPrompt.indexOf("NS");
        const idxBoundary = ctx.systemPrompt.indexOf(PROMPT_CACHE_BOUNDARY);
        const idxRes = ctx.systemPrompt.indexOf("RES");
        expect(idxNs).toBeGreaterThan(-1);
        expect(idxBoundary).toBeGreaterThan(idxNs);
        expect(idxRes).toBeGreaterThan(idxBoundary);
      });

      it("trims oldest messages first to fit the budget", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        const big = "a".repeat(40); // ~10 tokens via default chars/4 estimator
        await s.appendMessages(key, [userMsg(big), userMsg(big), userMsg(big)]);
        const ctx = await s.resolveContext(key, { maxMessageTokens: 15 });
        // Only the newest message survives.
        expect(ctx.messages.map((m) => m.seq)).toEqual([3]);
      });
    });

    // --- Episodes (L2) -----------------------------------------------
    describe("episodic records (L2)", () => {
      it("appendResourceEpisode assigns id, createdAt, defaults salience=0.5", async () => {
        const s = await make();
        const ep = await s.appendResourceEpisode(
          { namespaceId: NS, resourceId: ALICE },
          { summary: "first episode" },
        );
        expect(ep.id.length).toBeGreaterThan(0);
        expect(typeof ep.createdAt).toBe("number");
        expect(ep.salience).toBe(0.5);
      });

      it("clamps salience to [0,1]", async () => {
        const s = await make();
        const a = await s.appendResourceEpisode(
          { namespaceId: NS, resourceId: ALICE },
          { summary: "high", salience: 5 },
        );
        const b = await s.appendResourceEpisode(
          { namespaceId: NS, resourceId: ALICE },
          { summary: "low", salience: -3 },
        );
        expect(a.salience).toBe(1);
        expect(b.salience).toBe(0);
      });

      it("listResourceEpisodes orders by salienceDesc by default", async () => {
        const s = await make();
        const key = { namespaceId: NS, resourceId: ALICE };
        await s.appendResourceEpisode(key, { summary: "lo", salience: 0.2 });
        await s.appendResourceEpisode(key, { summary: "hi", salience: 0.9 });
        await s.appendResourceEpisode(key, { summary: "mid", salience: 0.5 });
        const out = await s.listResourceEpisodes(key);
        expect(out.map((e) => e.summary)).toEqual(["hi", "mid", "lo"]);
      });

      it("respects minSalience filter and limit", async () => {
        const s = await make();
        const key = { namespaceId: NS, resourceId: ALICE };
        await s.appendResourceEpisode(key, { summary: "a", salience: 0.1 });
        await s.appendResourceEpisode(key, { summary: "b", salience: 0.5 });
        await s.appendResourceEpisode(key, { summary: "c", salience: 0.9 });
        const filtered = await s.listResourceEpisodes(key, { minSalience: 0.4 });
        expect(filtered.map((e) => e.summary)).toEqual(["c", "b"]);
        const capped = await s.listResourceEpisodes(key, { limit: 1 });
        expect(capped.map((e) => e.summary)).toEqual(["c"]);
      });

      it("supports per-thread and per-namespace episodes", async () => {
        const s = await make();
        const tkey = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(tkey);
        await s.appendThreadEpisode(tkey, { summary: "thread-rollup", salience: 0.7 });
        await s.appendNamespaceEpisode(NS, { summary: "org-event", salience: 0.4 });
        expect((await s.listThreadEpisodes(tkey)).map((e) => e.summary)).toEqual(["thread-rollup"]);
        expect((await s.listNamespaceEpisodes(NS)).map((e) => e.summary)).toEqual(["org-event"]);
      });

      it("persists sourceThreadId + sourceMessageRange + outcome", async () => {
        const s = await make();
        const ep = await s.appendResourceEpisode(
          { namespaceId: NS, resourceId: ALICE },
          {
            summary: "rolled up",
            outcome: "deal closed",
            sourceThreadId: "billing-2026-q1",
            sourceMessageRange: { fromSeq: 1, toSeq: 42 },
          },
        );
        expect(ep.outcome).toBe("deal closed");
        expect(ep.sourceThreadId).toBe("billing-2026-q1");
        expect(ep.sourceMessageRange).toEqual({ fromSeq: 1, toSeq: 42 });
      });

      it("deletes only the targeted episode", async () => {
        const s = await make();
        const key = { namespaceId: NS, resourceId: ALICE };
        const a = await s.appendResourceEpisode(key, { summary: "a" });
        await s.appendResourceEpisode(key, { summary: "b" });
        await s.deleteResourceEpisode(key, a.id);
        const remaining = await s.listResourceEpisodes(key);
        expect(remaining.map((e) => e.summary)).toEqual(["b"]);
      });

      it("deleteThread cascades to thread episodes", async () => {
        const s = await make();
        const key = { namespaceId: NS, threadId: T_DEFAULT };
        await s.createThread(key);
        await s.appendThreadEpisode(key, { summary: "rollup" });
        await s.deleteThread(key);
        expect(await s.listThreadEpisodes(key)).toEqual([]);
      });
    });

    describe("episodes in resolveContext", () => {
      it("does NOT inject episodes when maxEpisodeTokens is unset", async () => {
        const s = await make();
        await s.appendResourceEpisode(
          { namespaceId: NS, resourceId: ALICE },
          { summary: "EPISODIC-MEMORY", salience: 1 },
        );
        const key = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(key);
        const ctx = await s.resolveContext(key, { maxMessageTokens: 1000 });
        expect(ctx.systemPrompt).not.toContain("EPISODIC-MEMORY");
      });

      it("injects top-salience episodes when maxEpisodeTokens > 0", async () => {
        const s = await make();
        const rkey = { namespaceId: NS, resourceId: ALICE };
        await s.appendResourceEpisode(rkey, { summary: "MID", salience: 0.5 });
        await s.appendResourceEpisode(rkey, { summary: "TOP", salience: 0.9 });
        const tkey = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(tkey);
        const ctx = await s.resolveContext(tkey, {
          maxMessageTokens: 1000,
          maxEpisodeTokens: 1000,
        });
        const idxTop = ctx.systemPrompt.indexOf("TOP");
        const idxMid = ctx.systemPrompt.indexOf("MID");
        expect(idxTop).toBeGreaterThan(-1);
        expect(idxMid).toBeGreaterThan(idxTop);
      });

      it("trims lowest-salience episodes when over the budget", async () => {
        const s = await make();
        const rkey = { namespaceId: NS, resourceId: ALICE };
        await s.appendResourceEpisode(rkey, { summary: "a".repeat(40), salience: 0.9 });
        await s.appendResourceEpisode(rkey, { summary: "b".repeat(40), salience: 0.5 });
        const tkey = { namespaceId: NS, resourceId: ALICE, threadId: T_DEFAULT };
        await s.createThread(tkey);
        // Budget fits one episode (~10 tokens) but not two.
        const ctx = await s.resolveContext(tkey, {
          maxMessageTokens: 1000,
          maxEpisodeTokens: 12,
        });
        // Higher-salience episode survives.
        expect(ctx.systemPrompt).toContain("a".repeat(40));
        expect(ctx.systemPrompt).not.toContain("b".repeat(40));
      });
    });

    // --- Tenant isolation --------------------------------------------
    describe("tenant isolation", () => {
      it("namespaces do not leak facts or threads across tenants", async () => {
        const s = await make();
        await s.appendNamespaceFact("tenant-a", "secret-a");
        await s.appendNamespaceFact("tenant-b", "secret-b");
        await s.createThread({ namespaceId: "tenant-a", threadId: "t" });
        await s.createThread({ namespaceId: "tenant-b", threadId: "t" });

        const aFacts = await s.listNamespaceFacts("tenant-a");
        const bFacts = await s.listNamespaceFacts("tenant-b");
        expect(aFacts.map((f) => f.text)).toEqual(["secret-a"]);
        expect(bFacts.map((f) => f.text)).toEqual(["secret-b"]);

        const aThreads = await s.listThreads({ namespaceId: "tenant-a" });
        const bThreads = await s.listThreads({ namespaceId: "tenant-b" });
        expect(aThreads.map((t) => t.threadId)).toEqual(["t"]);
        expect(bThreads.map((t) => t.threadId)).toEqual(["t"]);
      });
    });
  });
}
