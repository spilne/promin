import { it, expect } from "bun:test";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { withPostgres, uniqueName } from "./infra.ts";
import { PgStateBackend, PgStepQueue } from "@promin/postgres";
import type { DrizzleDb } from "@promin/postgres";

// ---------------------------------------------------------------------------
// Helper — create drizzle db from postgres ctx
// ---------------------------------------------------------------------------

function createDb(ctx: { url: string }): { db: DrizzleDb; close: () => Promise<void> } {
  const sql = postgres(ctx.url);
  const db = drizzle(sql) as DrizzleDb;
  return { db, close: () => sql.end() };
}

// ---------------------------------------------------------------------------
// PgStateBackend — state checkpoint/restore with real Postgres
// ---------------------------------------------------------------------------

withPostgres("PgStateBackend — persistent topology state", (ctx) => {
  it("put/get/delete with ensureTable from schema", async () => {
    const { db, close } = createDb(ctx);
    const state = new PgStateBackend({ db, table: uniqueName("state").replace(/-/g, "_") });
    await state.ensureTable();

    await state.put("key1", { counter: 42 });
    await state.put("key2", { counter: 99 });

    expect(await state.get("key1")).toEqual({ counter: 42 });
    expect(await state.get("key2")).toEqual({ counter: 99 });

    await state.delete("key1");
    expect(await state.get("key1")).toBeUndefined();

    await close();
  });

  it("checkpoint and restore survives across instances", async () => {
    const { db, close } = createDb(ctx);
    const table = uniqueName("cptest").replace(/-/g, "_");

    // Instance 1: write and checkpoint
    const s1 = new PgStateBackend({ db, table });
    await s1.ensureTable();
    await s1.put("window:0", [{ key: "u1", count: 5 }]);
    await s1.put("dedupe:0", ["seen-1", "seen-2"]);
    await s1.checkpoint({ name: "cp-1" });

    // Simulate crash — clear live state
    await s1.clear();
    expect(await s1.get("window:0")).toBeUndefined();

    // Instance 2: restore from checkpoint
    const s2 = new PgStateBackend({ db, table });
    await s2.restore({ name: "cp-1" });

    expect(await s2.get("window:0")).toEqual([{ key: "u1", count: 5 }]);
    expect(await s2.get("dedupe:0")).toEqual(["seen-1", "seen-2"]);

    await close();
  });

  it("multiple checkpoints — latest wins", async () => {
    const { db, close } = createDb(ctx);
    const table = uniqueName("multicp").replace(/-/g, "_");
    const state = new PgStateBackend({ db, table });
    await state.ensureTable();

    await state.put("v", 1);
    await state.checkpoint({ name: "cp" });

    await state.put("v", 2);
    await state.checkpoint({ name: "cp" }); // overwrites

    await state.clear();
    await state.restore({ name: "cp" });

    expect(await state.get("v")).toBe(2); // latest checkpoint

    await close();
  });
});

// ---------------------------------------------------------------------------
// PgStepQueue — SKIP LOCKED distributed task queue
// ---------------------------------------------------------------------------

withPostgres("PgStepQueue — distributed step dispatch", (ctx) => {
  it("enqueue and claim a step task", async () => {
    const { db, close } = createDb(ctx);
    const queue = new PgStepQueue({ db, workerId: "worker-1" });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-1",
      stepName: "process",
      needs: ["default"],
      input: { data: "hello" },
      prevResults: {},
    });

    const tasks = await queue.claim({ capabilities: ["default"], limit: 1 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]!.stepName).toBe("process");
    expect(tasks[0]!.input).toEqual({ data: "hello" });

    await close();
  });

  it("SKIP LOCKED prevents double-claim", async () => {
    const { db, close } = createDb(ctx);
    const q1 = new PgStepQueue({ db, workerId: "w1" });
    const q2 = new PgStepQueue({ db, workerId: "w2" });
    await q1.ensureTable();

    await q1.enqueue({
      workflowId: "wf-2",
      stepName: "step-a",
      needs: ["default"],
      input: {},
      prevResults: {},
    });

    // Both workers try to claim — only one should succeed
    const [t1, t2] = await Promise.all([
      q1.claim({ capabilities: ["default"], limit: 1 }),
      q2.claim({ capabilities: ["default"], limit: 1 }),
    ]);

    const claimed = [...t1, ...t2];
    expect(claimed.length).toBe(1); // exactly one

    await close();
  });

  it("higher priority tasks are claimed first", async () => {
    const { db, close } = createDb(ctx);
    const queue = new PgStepQueue({ db, workerId: "w1" });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-lo",
      stepName: "low",
      needs: ["default"],
      input: {},
      prevResults: {},
      priority: 1,
    });
    await queue.enqueue({
      workflowId: "wf-hi",
      stepName: "high",
      needs: ["default"],
      input: {},
      prevResults: {},
      priority: 10,
    });

    const first = await queue.claim({ capabilities: ["default"], limit: 1 });
    expect(first[0]!.stepName).toBe("high"); // higher priority first

    const second = await queue.claim({ capabilities: ["default"], limit: 1 });
    expect(second[0]!.stepName).toBe("low");

    await close();
  });

  it("complete marks task as done", async () => {
    const { db, close } = createDb(ctx);
    const queue = new PgStepQueue({ db, workerId: "w1" });
    await queue.ensureTable();

    await queue.enqueue({
      workflowId: "wf-done",
      stepName: "s1",
      needs: ["default"],
      input: {},
      prevResults: {},
    });

    const tasks = await queue.claim({ capabilities: ["default"], limit: 1 });
    await queue.complete({
      taskId: tasks[0]!.id,
      result: { output: "done" },
      durationMs: 42,
    });

    // No more tasks to claim
    const next = await queue.claim({ capabilities: ["default"], limit: 1 });
    expect(next).toHaveLength(0);

    await close();
  });
});
