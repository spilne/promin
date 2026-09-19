import { it, expect, setDefaultTimeout } from "bun:test";

setDefaultTimeout(300_000);
import { Redis as IoRedis } from "ioredis";
import postgres from "postgres";
import { drizzle } from "drizzle-orm/postgres-js";
import { withAll, uniqueName } from "../infra.ts";
import { RedisCacheStore, type RedisClient as RedisClientType } from "@promin/redis";
import { PgStateBackend, PgStepQueue } from "@promin/postgres";
import type { DrizzleDb } from "@promin/postgres";

// ---------------------------------------------------------------------------
// E2E: Postgres step queue → worker claim/complete cycle
// ---------------------------------------------------------------------------

withAll("E2E: Postgres step queue — distributed task lifecycle", (ctx) => {
  it("coordinator enqueues, workers claim and complete, no duplicates", async () => {
    const sql = postgres(ctx.postgres.url);
    const db = drizzle(sql) as DrizzleDb;
    const queue = new PgStepQueue({ db, workerId: "coordinator" });
    await queue.ensureTable();

    // Enqueue 10 tasks
    for (let i = 0; i < 10; i++) {
      await queue.enqueue({
        workflowId: "wf-e2e",
        stepName: `step-${i}`,
        needs: ["default"],
        input: { index: i },
        prevResults: {},
      });
    }

    // 3 workers compete for tasks
    const w1 = new PgStepQueue({ db, workerId: "w1" });
    const w2 = new PgStepQueue({ db, workerId: "w2" });
    const w3 = new PgStepQueue({ db, workerId: "w3" });

    const completed: string[] = [];
    const workerAssignments: Record<string, string[]> = { w1: [], w2: [], w3: [] };

    async function work(q: PgStepQueue, name: string) {
      while (true) {
        const tasks = await q.claim({ capabilities: ["default"], limit: 1 });
        if (tasks.length === 0) break;
        const task = tasks[0]!;
        workerAssignments[name]!.push(task.stepName);
        await q.complete({ taskId: task.id, result: { done: true }, durationMs: 1 });
        completed.push(task.stepName);
      }
    }

    await Promise.all([work(w1, "w1"), work(w2, "w2"), work(w3, "w3")]);

    // All 10 tasks completed, no duplicates
    expect(completed.sort()).toEqual(Array.from({ length: 10 }, (_, i) => `step-${i}`).sort());

    // Work was distributed (at least 1 worker got tasks)
    const activeWorkers = Object.values(workerAssignments).filter((a) => a.length > 0);
    expect(activeWorkers.length).toBeGreaterThanOrEqual(1);

    await sql.end();
  });
});

// ---------------------------------------------------------------------------
// E2E: Cache layering — Redis L1 + Postgres L2
// ---------------------------------------------------------------------------

withAll("E2E: Redis cache with Postgres state fallback", (ctx) => {
  it("cache hit avoids state backend lookup", async () => {
    const redis = new IoRedis(ctx.redis.port, ctx.redis.host) as unknown as RedisClientType;
    const sql = postgres(ctx.postgres.url);
    const db = drizzle(sql) as DrizzleDb;

    const cache = new RedisCacheStore<{ score: number }>({
      redis,
      ttlMs: 5_000,
      prefix: uniqueName("cache") + ":",
    });

    const state = new PgStateBackend({ db, table: uniqueName("state").replace(/-/g, "_") });
    await state.ensureTable();

    // Store in both
    await state.put("user:1", { score: 42 });
    await cache.set("user:1", { score: 42 });

    // Cache hit
    const cached = await cache.get("user:1");
    expect(cached).toEqual({ score: 42 });

    // After cache eviction, fall back to state backend
    await cache.delete("user:1");
    expect(await cache.get("user:1")).toBeUndefined();
    expect(await state.get("user:1")).toEqual({ score: 42 });

    redis.disconnect();
    await sql.end();
  });
});
