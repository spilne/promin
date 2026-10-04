import { describe, expect, it } from "bun:test";
import { RedisWorkflowStorage } from "../redis-workflow-storage.ts";
import type { RedisStoreClient } from "../redis-client.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// Redis-specific behaviour of the hash-tagged key layout: the cross-workflow
// index converging after a lag, the sleep schedule cleaning itself, purge
// removing streams, fence tokens across purge and resetSteps on the
// journal.
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
    it("removes the workflow's streams", async () => {
      const { prefix, client, storage } = setup();
      await storage.createWorkflow({ workflowId: "w", workflowName: "n", input: 1 });
      await storage.appendStreamChunk({
        workflowId: "w",
        streamId: "out",
        payload: 1,
        appendedBy: "workflow",
      });
      expect(Number(await client.exists(`${prefix}:{wf:w}:streams:out`))).toBe(1);
      await storage.completeWorkflow({ workflowId: "w", result: 1 });

      expect(await storage.purgeCompleted({ olderThanMs: -60_000, limit: 10 })).toBe(1);
      expect(Number(await client.exists(`${prefix}:{wf:w}:streams:out`))).toBe(0);
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
});
