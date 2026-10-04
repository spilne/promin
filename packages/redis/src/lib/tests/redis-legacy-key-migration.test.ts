import { expect, it } from "bun:test";
import { RedisStepQueue } from "../redis-step-queue.ts";
import { RedisSchedulerStorage } from "../redis-scheduler-storage.ts";
import { RedisStateMachineStorage } from "../redis-state-machine-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// `migrateLegacyKeys()` of the stores whose keys moved under a hash tag:
// data written in the untagged layout is readable after the migration.
// The legacy layout is produced by writing with this version and renaming
// every key back to its untagged name.
// ---------------------------------------------------------------------------

async function renameAll(params: {
  redis: RedisStoreClient;
  pattern: string;
  legacyName: (key: string) => string | null;
}): Promise<number> {
  let renamed = 0;
  for (const key of await params.redis.keys(params.pattern)) {
    const to = params.legacyName(key);
    if (to === null) await params.redis.del(key);
    else {
      await params.redis.eval("return redis.call('RENAME', KEYS[1], KEYS[2])", 2, key, to);
      renamed++;
    }
  }
  return renamed;
}

redisDescribe("migrateLegacyKeys", (ctx) => {
  it("RedisStepQueue: tasks, sets and counters move under the queue's tag", async () => {
    const redis = ctx.client();
    const prefix = uniquePrefix("sq");
    const q = new RedisStepQueue({ redis, prefix });
    const input = { workflowId: "wf", input: {}, prevResults: {} };
    const pending = await q.enqueue({ ...input, stepName: "pending" });
    await q.enqueue({ ...input, stepName: "running" });
    const [running] = await q.claim({ workerId: "w", limit: 1, stepNames: ["running"] });

    const legacy = await renameAll({
      redis,
      pattern: `{${prefix}}:*`,
      legacyName: (key) => `${prefix}:${key.slice(prefix.length + 3)}`,
    });
    expect(await q.get(pending)).toBeUndefined();

    expect(await q.migrateLegacyKeys()).toEqual({ keys: legacy });
    expect((await q.get(pending))?.status).toBe("pending");
    expect(await q.enqueue({ ...input, stepName: "pending" })).toBe(pending);
    expect(
      await q.complete({
        taskId: running!.id,
        claimToken: running!.claimToken,
        result: 1,
        durationMs: 1,
      }),
    ).toBe(true);
    // The id counter carried over: a new task gets a fresh id.
    expect(Number(await q.enqueue({ ...input, stepName: "new" }))).toBe(3);
    expect(await q.migrateLegacyKeys()).toEqual({ keys: 0 });
  });

  it("RedisSchedulerStorage: schedules, due sets and lease epochs move; namespaces register", async () => {
    const redis = ctx.client();
    const prefix = uniquePrefix("sched");
    const s = new RedisSchedulerStorage({ redis, prefix });
    await s.upsertSchedule({ id: "a", intervalMs: 1_000, namespace: "tenant:1" });
    await s.upsertSchedule({ id: "b", intervalMs: 1_000 });
    const first = await s.tryAcquireLeader({ key: "poll", instanceId: "i", ttlMs: 30_000 });
    await s.releaseLeader({ lease: first! });

    const legacy = await renameAll({
      redis,
      pattern: `{${prefix}}:*`,
      // The namespace registry did not exist in the untagged layout.
      legacyName: (key) =>
        key.endsWith(":namespaces") ? null : `${prefix}:${key.slice(prefix.length + 3)}`,
    });
    expect(await s.listSchedules()).toEqual([]);

    expect(await s.migrateLegacyKeys()).toEqual({ keys: legacy });
    expect((await s.listSchedules()).map((c) => c.id)).toEqual(["a", "b"]);
    const due = await s.findDueAcross({ now: new Date(Date.now() + 60_000), limit: 10 });
    expect(due.map((d) => d.id).sort()).toEqual(["a", "b"]);
    expect(due.find((d) => d.id === "a")?.namespace).toBe("tenant:1");
    // The epoch kept counting from where it was.
    const next = await s.tryAcquireLeader({ key: "poll", instanceId: "i", ttlMs: 30_000 });
    expect(next!.epoch).toBe(first!.epoch + 1);
  });

  it("RedisStateMachineStorage: each machine's keys move under its tag", async () => {
    const redis = ctx.client();
    const prefix = uniquePrefix("sm");
    const s = new RedisStateMachineStorage({ redis, prefix });
    await s.create({ id: "m:1", name: "n", initial: "a", context: { k: 1 } });
    await s.transition({
      id: "m:1",
      from: "a",
      to: "b",
      expectedRevision: 0,
      event: "go",
      context: {},
    });
    const token = await s.tryLock({ id: "m:1", durationMs: 30_000 });

    const legacy = await renameAll({
      redis,
      pattern: `${prefix}:{sm:*`,
      legacyName: (key) => {
        const [, id, kind] = /^.*?:\{sm:(.*)\}:(machine|events|lock)$/.exec(key)!;
        return `${prefix}:${kind}:${id}`;
      },
    });
    expect(legacy).toBe(3);
    expect(await s.load("m:1")).toBeNull();

    expect(await s.migrateLegacyKeys()).toEqual({ keys: 3 });
    expect((await s.load("m:1"))?.current).toBe("b");
    expect((await s.loadEvents("m:1")).map((e) => e.event)).toEqual(["go"]);
    expect(await s.tryLock({ id: "m:1", durationMs: 1_000 })).toBeNull();
    expect(await s.extendLock({ id: "m:1", token: token!, durationMs: 1_000 })).toBe(true);
    expect(await s.migrateLegacyKeys()).toEqual({ keys: 0 });
  });
});
