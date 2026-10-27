import { describe, expect, it } from "bun:test";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Redis-specific behaviour of the hash-tagged key layout: the cross-workflow
// index converging after a lag, the sleep schedule cleaning itself, purge
// finding untracked streams, fence tokens across purge, resetSteps on the
// journal, and the migration from the untagged layout.
// ---------------------------------------------------------------------------

const pttl = (client: RedisStoreClient, key: string): Promise<number> =>
  (client as unknown as { pttl(k: string): Promise<number> }).pttl(key);

redisDescribe("RedisWorkflowStorage key layout", (redis) => {
  const setup = (opts?: { completedTtlMs?: number }) => {
    const prefix = uniquePrefix("lay");
    const client = redis.client();
    const storage = new RedisWorkflowStorage({
      redis: client,
      prefix,
      ...(opts?.completedTtlMs ? { retention: { completedTtlMs: opts.completedTtlMs } } : {}),
    });
    return { prefix, client, storage };
  };

  describe("cross-workflow index", () => {
    it("a reader repairs an index entry that lags the workflow hash", async () => {
      const { prefix, client, storage } = setup();
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      // A writer that crashed between its workflow script and the index sync.
      await client.eval(
        `redis.call('HSET', KEYS[1], 'status', 'completed', 'completedAt', ARGV[1])
         return redis.call('HINCRBY', KEYS[1], 'iv', 1)`,
        1,
        `${prefix}:{wf:w}`,
        new Date().toISOString(),
      );
      expect(await storage.countWorkflows({ status: "pending" })).toBe(1);

      expect(await storage.listWorkflows({ status: "pending" })).toEqual([]);
      expect(await storage.countWorkflows({ status: "pending" })).toBe(0);
      expect(
        (await storage.listWorkflows({ status: "completed" })).map((w) => w.workflowId),
      ).toEqual(["w"]);
    });

    it("a retried create indexes a row whose first create crashed before indexing", async () => {
      const { prefix, client, storage } = setup();
      const now = new Date().toISOString();
      await client.hset(`${prefix}:{wf:w}`, {
        id: "w",
        workflowName: "n",
        status: "pending",
        run: "1",
        input: "1",
        createdAt: now,
        updatedAt: now,
        iv: "1",
        streamsTracked: "1",
      });
      expect(await storage.countWorkflows()).toBe(0);
      const again = await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      expect(again.created).toBe(false);
      expect(await storage.countWorkflows({ name: "n" })).toBe(1);
    });

    it("unfiltered pages come from the ordering sorted sets, NULLs last", async () => {
      const { storage } = setup();
      for (const id of ["a", "b", "c"]) {
        await storage.createWorkflow({ workflowId: id, workflowName: "n", input: 1 });
      }
      await storage.saveStepResult({
        workflowId: "b",
        stepName: "s",
        result: 1,
        durationMs: 1,
        startedAt: new Date(),
      });
      const page = await storage.listWorkflows({ orderBy: "startedAt", limit: 2 });
      expect(page.map((w) => w.workflowId)[0]).toBe("b");
      expect(page).toHaveLength(2);
      const rest = await storage.listWorkflows({ orderBy: "startedAt", offset: 1 });
      expect(rest.map((w) => w.workflowId).sort()).toEqual(["a", "c"]);
    });
  });

  describe("sleep schedule", () => {
    it("findDueSleeps drops a member whose journal entry is gone", async () => {
      const { prefix, client, storage } = setup();
      await client.zadd(`${prefix}:{idx}:sleeps`, 1, "ghost::s::0|");
      expect(await storage.findDueSleeps({ now: new Date(), limit: 10 })).toEqual([]);
      expect(await client.zcard(`${prefix}:{idx}:sleeps`)).toBe(0);
    });

    it("a replayed pending sleep is scheduled again", async () => {
      const { prefix, client, storage } = setup();
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      const sleep = {
        workflowId: "w",
        stepName: "s",
        activityIndex: 0,
        activityName: "nap",
        stepType: "sleep" as const,
        wakeAt: new Date(1_000),
      };
      await storage.appendPendingEntry(sleep);
      // Its first writer crashed before scheduling it.
      await client.zrem(`${prefix}:{idx}:sleeps`, "w::s::0|");
      await storage.appendPendingEntry(sleep);
      const due = await storage.findDueSleeps({ now: new Date(), limit: 10 });
      expect(due.map((d) => d.workflowId)).toEqual(["w"]);
    });
  });

  describe("purge", () => {
    it("removes streams appended before stream ids were tracked", async () => {
      const { prefix, client, storage } = setup();
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      await client.rpush(`${prefix}:{wf:w}:streams:legacy`, JSON.stringify({ payload: 1 }));
      await client.hdel(`${prefix}:{wf:w}`, "streamsTracked");
      await storage.completeWorkflow({ workflowId: "w", result: 1 });

      expect(await storage.purgeCompleted({ olderThanMs: -60_000, limit: 10 })).toBe(1);
      expect(Number(await client.exists(`${prefix}:{wf:w}:streams:legacy`))).toBe(0);
      expect(await storage.countWorkflows()).toBe(0);
    });

    it("fence tokens keep growing across a purge and re-create", async () => {
      const { storage } = setup();
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      const first = await storage.tryLock({ workflowId: "w", lockDurationMs: 30_000 });
      await storage.releaseLock({ workflowId: "w", guard: { fenceToken: first.token! } });
      await storage.completeWorkflow({ workflowId: "w", result: 1 });
      await storage.purgeCompleted({ olderThanMs: -60_000, limit: 10 });
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      const second = await storage.tryLock({ workflowId: "w", lockDurationMs: 30_000 });
      expect(BigInt(second.token!) > BigInt(first.token!)).toBe(true);
    });
  });

  describe("resetSteps", () => {
    it("drops the reset steps' journal and sleeps and lifts the retention TTL", async () => {
      const { prefix, client, storage } = setup({ completedTtlMs: 60_000 });
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      for (const stepName of ["keep", "redo"]) {
        await storage.saveStepResult({
          workflowId: "w",
          stepName,
          result: 1,
          durationMs: 1,
          startedAt: new Date(),
        });
      }
      await storage.appendPendingEntry({
        workflowId: "w",
        stepName: "redo",
        activityIndex: 0,
        activityName: "nap",
        stepType: "sleep",
        wakeAt: new Date(1_000),
      });
      await storage.completeWorkflow({ workflowId: "w", result: 1 });
      expect(await pttl(client, `${prefix}:{wf:w}`)).toBeGreaterThan(0);

      await storage.resetSteps({ workflowId: "w", stepNames: ["redo"] });

      expect(await storage.loadJournal({ workflowId: "w", stepName: "redo" })).toEqual([]);
      expect(await client.zcard(`${prefix}:{idx}:sleeps`)).toBe(0);
      expect(await pttl(client, `${prefix}:{wf:w}`)).toBe(-1);
      expect(await pttl(client, `${prefix}:{wf:w}:steps:1`)).toBe(-1);
      expect(await storage.countWorkflows({ status: "running" })).toBe(1);
      expect(await storage.countWorkflows({ status: "completed" })).toBe(0);
    });
  });

  describe("migrateLegacyKeys", () => {
    it("moves untagged keys under the hash tags and rebuilds the indexes", async () => {
      const { prefix: p, client, storage } = setup();
      const now = new Date().toISOString();
      const wakeAt = Date.now() - 1_000;
      // The untagged layout of earlier versions: a suspended parent with a
      // step, a pending journal sleep, a signal, an untracked stream and a
      // live lock; a pending child whose id extends the parent's.
      await client.hset(`${p}:old`, {
        id: "old",
        workflowName: "w",
        status: "suspended",
        run: "1",
        input: "{}",
        createdAt: now,
        updatedAt: now,
        startedAt: now,
      });
      await client.hset(`${p}:old:steps:1`, {
        s: JSON.stringify({
          stepName: "s",
          run: 1,
          status: "sleeping",
          dependsOn: [],
          stepType: "single",
          attempt: 1,
          wakeAt: new Date(wakeAt).toISOString(),
        }),
      });
      await client.sadd(`${p}:old:journal:steps`, "s");
      await client.zadd(`${p}:old:journal:s:idx`, 0, "0|");
      await client.hset(`${p}:old:journal:s:entry:0|`, {
        activityName: "nap",
        stepType: "sleep",
        phase: "pending",
        wakeAt: String(wakeAt),
        branchPath: "",
        createdAt: now,
      });
      await client.zadd(`${p}:sleeps`, wakeAt, "old::s::0|");
      await client.hset(`${p}:old:signals`, {
        go: JSON.stringify({ signalName: "go", payload: 1, deliveredAt: now }),
      });
      await client.rpush(
        `${p}:old:streams:out`,
        JSON.stringify({ payload: 1, appendedBy: "workflow", appendedAt: now }),
      );
      await client.hset(`${p}:lock:old`, { lockedBy: "x", token: "7" });
      await client.pexpire(`${p}:lock:old`, 60_000);
      await client.hset(`${p}:old:kid`, {
        id: "old:kid",
        workflowName: "kid",
        status: "pending",
        run: "1",
        input: "{}",
        createdAt: now,
        updatedAt: now,
        parentWorkflowId: "old",
        namespace: "ns",
      });
      await client.hset(`${p}:old:kid:steps:1`, {
        a: JSON.stringify({ stepName: "a", run: 1, status: "completed", dependsOn: [] }),
      });
      await client.sadd(`${p}:idx:status:suspended`, "old");
      await client.sadd(`${p}:idx:status:pending`, "old:kid");
      await client.sadd(`${p}:idx:name:w`, "old");
      await client.sadd(`${p}:idx:children:old`, "old:kid");
      await client.sadd(`${p}:idx:distinct:names`, "w", "kid", "purged-long-ago");
      await client.set(`${p}:lock-fence-counter`, "7");
      await client.set(`${p}:signal_token:tk`, "old");

      const result = await storage.migrateLegacyKeys();
      expect(result.workflows).toBe(2);

      const old = (await storage.loadWorkflow("old"))!;
      expect(old.status).toBe("suspended");
      expect(old.steps["s"]?.status).toBe("sleeping");
      const kid = (await storage.loadWorkflow("old:kid"))!;
      expect(kid.parentWorkflowId).toBe("old");
      expect(kid.steps["a"]?.status).toBe("completed");

      expect((await storage.listWorkflows({ parentId: "old" })).map((w) => w.workflowId)).toEqual([
        "old:kid",
      ]);
      expect(await storage.countWorkflows({ namespace: "ns" })).toBe(1);
      expect(await storage.countWorkflows({ status: "suspended" })).toBe(1);
      expect(await storage.countWorkflows()).toBe(2);
      expect(await storage.distinctWorkflowNames()).toEqual(["kid", "purged-long-ago", "w"]);
      expect(await storage.loadJournal({ workflowId: "old", stepName: "s" })).toHaveLength(1);
      expect(
        (await storage.findDueSleeps({ now: new Date(), limit: 10 })).map((d) => d.workflowId),
      ).toEqual(["old"]);
      expect(await storage.loadSignals("old")).toHaveLength(1);
      expect(await storage.readStreamChunks({ workflowId: "old", streamId: "out" })).toHaveLength(
        1,
      );
      expect((await storage.tryLock({ workflowId: "old", lockDurationMs: 30_000 })).acquired).toBe(
        false,
      );
      expect(
        (await storage.listDueTimers({ now: new Date(), limit: 10 })).map((w) => w.workflowId),
      ).toEqual(["old"]);

      // The legacy keys are gone, the untagged lookups stay.
      for (const key of [
        `${p}:old`,
        `${p}:old:steps:1`,
        `${p}:old:kid`,
        `${p}:lock:old`,
        `${p}:sleeps`,
        `${p}:idx:status:suspended`,
        `${p}:idx:children:old`,
        `${p}:idx:distinct:names`,
        `${p}:lock-fence-counter`,
      ]) {
        expect([key, Number(await client.exists(key))]).toEqual([key, 0]);
      }
      expect(await client.get(`${p}:signal_token:tk`)).toBe("old");

      // The backfilled stream is purged with its workflow.
      await storage.completeWorkflow({ workflowId: "old", result: 1 });
      expect(await storage.purgeCompleted({ olderThanMs: -60_000, limit: 10 })).toBe(1);
      expect(Number(await client.exists(`${p}:{wf:old}:streams:out`))).toBe(0);

      // Nothing left to move.
      expect((await storage.migrateLegacyKeys()).keys).toBe(0);
    });
  });
});
