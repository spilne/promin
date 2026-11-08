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
import { succeed } from "@spilne/perfect-core";
import { workflow } from "../workflow-builder.ts";
import { createWorkflowRunner } from "../workflow-runner.ts";
import { FenceTokenMismatchError, StorageError } from "../durable-pipeline-error.ts";

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

describe("ctx.metadata — seeded by the runner", () => {
  /** In-memory storage that counts `loadWorkflow` calls. */
  class CountingStorage extends InMemoryWorkflowStorage {
    loads = 0;
    override async loadWorkflow(workflowId: string) {
      this.loads++;
      return super.loadWorkflow(workflowId);
    }
  }

  it("a journaled step under the runner reads the run's metadata without loading the run", async () => {
    const storage = new CountingStorage();
    let observed: Record<string, unknown> | undefined;
    const wf = workflow<number>({ name: "meta-seeded" })
      .step("a", ({ input }) => succeed(input))
      .journaled("j", function* (ctx) {
        observed = ctx.metadata.get();
        return 1;
      })
      .build();
    await storage.createWorkflow({
      workflowId: "seeded-1",
      workflowName: wf.name,
      input: 1,
      metadata: { tenant: "acme" },
    });
    const runner = createWorkflowRunner({ storage });
    storage.loads = 0;

    await runner.run({ workflow: wf, workflowId: "seeded-1", input: 1 });

    expect(observed).toEqual({ tenant: "acme" });
    // The one load is the run's state taken with the lock.
    expect(storage.loads).toBe(1);
  });

  it("a later journaled step sees what an earlier one wrote", async () => {
    const storage = new InMemoryWorkflowStorage();
    let observed: Record<string, unknown> | undefined;
    const wf = workflow<number>({ name: "meta-chain" })
      .journaled("first", function* (ctx) {
        ctx.metadata.merge({ progress: "1/2", drop: null });
        return 1;
      })
      .journaled("second", function* (ctx) {
        observed = ctx.metadata.get();
        return 2;
      })
      .build();
    await storage.createWorkflow({
      workflowId: "chain-1",
      workflowName: wf.name,
      input: 1,
      metadata: { tenant: "acme", drop: true },
    });

    await createWorkflowRunner({ storage }).run({ workflow: wf, workflowId: "chain-1", input: 1 });

    expect(observed).toEqual({ tenant: "acme", progress: "1/2" });
    expect((await storage.loadWorkflow("chain-1"))!.metadata).toEqual({
      tenant: "acme",
      progress: "1/2",
    });
  });

  it("the runner's copy is replaced on a write, never mutated", async () => {
    const storage = new InMemoryWorkflowStorage();
    await storage.createWorkflow({ workflowId: "ref-1", workflowName: "t", input: {} });
    const original = { tenant: "acme" };
    const ref = { current: original as Record<string, unknown> | undefined };

    await runJournaledStep({
      input: {},
      prev: {},
      workflowId: "ref-1",
      stepName: "body",
      storage,
      workflowStorage: storage,
      workflowMetadata: ref,
      body: function* (ctx: JournaledContext<unknown, unknown>) {
        ctx.metadata.set("progress", 1);
        return "ok";
      },
    });

    expect(original).toEqual({ tenant: "acme" });
    expect(ref.current).toEqual({ tenant: "acme", progress: 1 });
  });
});

describe("ctx.metadata — a failed write is not silent", () => {
  class FlakyMetadataStorage extends InMemoryWorkflowStorage {
    failWith: Error | undefined;
    override async setWorkflowMetadata(
      params: Parameters<InMemoryWorkflowStorage["setWorkflowMetadata"]>[0],
    ): Promise<void> {
      if (this.failWith) throw this.failWith;
      return super.setWorkflowMetadata(params);
    }
  }

  const build = () =>
    workflow<number>({ name: "meta-fail" })
      .journaled("j", function* (ctx) {
        ctx.metadata.set("progress", "1/2");
        return 1;
      })
      .build();

  it("a storage error is reported to onMetadataWriteError and the step carries on", async () => {
    const storage = new FlakyMetadataStorage();
    storage.failWith = new StorageError({ message: "disk full" });
    const reports: { stepName: string; keys: readonly string[]; error: unknown }[] = [];
    const runner = createWorkflowRunner({
      storage,
      hooks: { onMetadataWriteError: (e) => void reports.push(e) },
    });

    const result = await runner.run({ workflow: build(), workflowId: "mf-1", input: 1 });

    expect(result).toBe(1);
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      stepName: "j",
      keys: ["progress"],
      error: storage.failWith,
    });
  });

  it("a write that lost the lock's fence fails the step with FenceTokenMismatchError", async () => {
    const storage = new FlakyMetadataStorage();
    storage.failWith = new FenceTokenMismatchError({
      workflowId: "mf-2",
      expected: "2",
      provided: "1",
      message: "stale",
    });
    const runner = createWorkflowRunner({ storage });

    const { data, error } = await runner.runSafe({
      workflow: build(),
      workflowId: "mf-2",
      input: 1,
    });

    expect(data).toBeNull();
    expect((error as { _tag?: string })._tag).toBe("FenceTokenMismatchError");
    expect((await storage.loadWorkflow("mf-2"))!.status).not.toBe("completed");
  });
});
