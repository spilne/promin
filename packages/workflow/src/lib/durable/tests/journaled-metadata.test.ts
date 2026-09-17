// ---------------------------------------------------------------------------
// ctx.metadata — live-writable workflow metadata surfaced to the dashboard.
//
// Exercises the in-memory storage path. The Postgres / Redis / SQLite
// implementations are covered by their respective package test suites.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeEach } from "bun:test";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { runJournaledStep } from "../journaled-step.ts";
import type { JournaledContext } from "../journaled-step.ts";

describe("ctx.metadata — live-writable surface", () => {
  let storage: InMemoryWorkflowStorage;

  beforeEach(async () => {
    storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({
      workflowId: "wf-meta",
      workflowName: "test",
      input: {},
      metadata: { tenant: "acme" },
    });
  });

  it("get() returns the snapshot loaded at body start", async () => {
    let observed: Record<string, unknown> | undefined;
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      observed = ctx.metadata.get();
      return "ok";
    };

    await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-meta",
      stepName: "body",
      storage,
      workflowStorage: storage,
      body,
    });
    expect(observed).toEqual({ tenant: "acme" });
  });

  it("set(key, value) merges into storage and is visible to subsequent get()", async () => {
    let observed: Record<string, unknown> | undefined;
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      ctx.metadata.set("progress", "1/3");
      observed = ctx.metadata.get();
      return "ok";
    };

    await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-meta",
      stepName: "body",
      storage,
      workflowStorage: storage,
      body,
    });
    expect(observed).toEqual({ tenant: "acme", progress: "1/3" });

    // Storage reflects the write — give the fire-and-forget storage call a
    // microtask to land. In-memory storage resolves synchronously so this
    // is belt-and-braces.
    await Promise.resolve();
    const wf = await storage.loadWorkflow("wf-meta");
    expect(wf?.metadata).toEqual({ tenant: "acme", progress: "1/3" });
  });

  it("merge(patch) applies all keys at once", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      ctx.metadata.merge({ progress: "2/3", phase: "scoring" });
      return ctx.metadata.get();
    };

    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-meta",
      stepName: "body",
      storage,
      workflowStorage: storage,
      body,
    });
    expect(result).toEqual({ tenant: "acme", progress: "2/3", phase: "scoring" });

    await Promise.resolve();
    const wf = await storage.loadWorkflow("wf-meta");
    expect(wf?.metadata).toEqual({ tenant: "acme", progress: "2/3", phase: "scoring" });
  });

  it("set(key, null) removes the key from both snapshot and storage", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      ctx.metadata.set("tenant", null);
      ctx.metadata.set("progress", "3/3");
      return ctx.metadata.get();
    };

    const result = await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-meta",
      stepName: "body",
      storage,
      workflowStorage: storage,
      body,
    });
    expect(result).toEqual({ progress: "3/3" });

    await Promise.resolve();
    const wf = await storage.loadWorkflow("wf-meta");
    expect(wf?.metadata).toEqual({ progress: "3/3" });
  });

  it("get() returns a copy — mutations don't bleed into the snapshot", async () => {
    const body = function* (ctx: JournaledContext<unknown, unknown>) {
      const a = ctx.metadata.get();
      (a as Record<string, unknown>).rogue = "leaked";
      return ctx.metadata.get();
    };

    const result = (await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "wf-meta",
      stepName: "body",
      storage,
      workflowStorage: storage,
      body,
    })) as Record<string, unknown>;
    expect(result.rogue).toBeUndefined();
    expect(result).toEqual({ tenant: "acme" });
  });
});
