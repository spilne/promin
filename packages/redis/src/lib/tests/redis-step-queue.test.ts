import { describe, expect, it } from "bun:test";
import { stepQueueTestSuite } from "@promin/workflow/testing";
import { RedisStepQueue } from "../redis-step-queue.ts";
import { redisDescribe, uniquePrefix, type RedisTestContext } from "./redis-test-utils.ts";

redisDescribe("RedisStepQueue conformance", (redis) => {
  stepQueueTestSuite(
    () => new RedisStepQueue({ redis: redis.client(), prefix: uniquePrefix("sq") }),
  );
});

redisDescribe("RedisStepQueue concurrency keys", (redis) => {
  const keyed = (i: number, limit = 1) => ({
    workflowId: `wf-${i}`,
    stepName: "send",
    input: {},
    prevResults: {},
    concurrencyKey: "tenant",
    concurrencyScope: "send",
    concurrencyLimit: limit,
  });

  function queues(ctx: RedisTestContext, count: number): RedisStepQueue[] {
    const prefix = uniquePrefix("sq");
    return Array.from(
      { length: count },
      (_, i) => new RedisStepQueue({ redis: ctx.client(), prefix, workerId: `w-${i}` }),
    );
  }

  it("round-trips the concurrency fields on claimed tasks", async () => {
    const [q] = queues(redis, 1);
    await q!.enqueue(keyed(0, 3));
    const [task] = await q!.claim({ limit: 1 });
    expect(task).toMatchObject({
      concurrencyKey: "tenant",
      concurrencyScope: "send",
      concurrencyLimit: 3,
    });
  });

  it("releases the slot when a task fails", async () => {
    const [q] = queues(redis, 1);
    await q!.enqueue(keyed(0));
    await q!.enqueue(keyed(1));
    const [first] = await q!.claim({ limit: 10 });
    expect(await q!.claim({ limit: 10 })).toHaveLength(0);

    await q!.fail({ taskId: first!.id, claimToken: first!.claimToken, error: "x", durationMs: 1 });
    expect(await q!.claim({ limit: 10 })).toHaveLength(1);
  });

  it("releases the slot when a stuck task is requeued, and the task can be reclaimed", async () => {
    const [q] = queues(redis, 1);
    await q!.enqueue(keyed(0));
    await q!.enqueue(keyed(1));
    const [first] = await q!.claim({ limit: 10 });

    expect(await q!.requeueStuck({ claimedBy: "w-0" })).toBe(1);
    const again = await q!.claim({ limit: 10 });
    expect(again).toHaveLength(1);
    expect(again[0]!.id).toBe(first!.id);
  });

  it("releases the slot when the claim filter rejects a task", async () => {
    const [q] = queues(redis, 1);
    await q!.enqueue(keyed(0));
    expect(await q!.claim({ limit: 10, filter: () => false })).toHaveLength(0);
    expect(await q!.claim({ limit: 10 })).toHaveLength(1);
  });

  it("a stale claim token can neither complete nor requeue the reclaimed task", async () => {
    const [q] = queues(redis, 1);
    await q!.enqueue(keyed(0));
    const [stale] = await q!.claim({ limit: 1 });
    await q!.requeueStuck({ claimedBy: "w-0" });
    const [fresh] = await q!.claim({ limit: 1 });

    expect(
      await q!.complete({ taskId: stale!.id, claimToken: "stale", result: 1, durationMs: 1 }),
    ).toBe(false);
    // The slot is still held by the fresh claim.
    await q!.enqueue(keyed(1));
    expect(await q!.claim({ limit: 10 })).toHaveLength(0);
    expect(
      await q!.complete({
        taskId: fresh!.id,
        claimToken: fresh!.claimToken,
        result: 1,
        durationMs: 1,
      }),
    ).toBe(true);
    expect(await q!.claim({ limit: 10 })).toHaveLength(1);
  });

  it("concurrent claimers on separate connections never exceed the limit", async () => {
    const qs = queues(redis, 8);
    for (let i = 0; i < 20; i++) await qs[0]!.enqueue(keyed(i, 3));

    const results = await Promise.all(qs.map((q) => q.claim({ limit: 5 })));
    expect(results.flat()).toHaveLength(3);
  });

  it("a blocked backlog larger than one scan page does not starve other keys", async () => {
    const [q] = queues(redis, 1);
    // 40 high-priority tasks on one key (limit 1) ahead of an unrelated task.
    for (let i = 0; i < 40; i++) await q!.enqueue({ ...keyed(i), priority: 9 });
    await q!.enqueue({
      workflowId: "other",
      stepName: "send",
      input: {},
      prevResults: {},
      priority: 1,
    });

    const claimed = await q!.claim({ limit: 2 });
    expect(claimed.map((t) => t.workflowId).sort()).toEqual(["other", "wf-0"]);
  });

  describe("claimScanLimit", () => {
    it("bounds how far one claim looks past blocked tasks", async () => {
      const prefix = uniquePrefix("sq");
      const q = new RedisStepQueue({ redis: redis.client(), prefix, claimScanLimit: 10 });
      for (let i = 0; i < 20; i++) await q.enqueue({ ...keyed(i), priority: 9 });
      await q.enqueue({
        workflowId: "other",
        stepName: "send",
        input: {},
        prevResults: {},
        priority: 1,
      });

      expect((await q.claim({ limit: 5 })).map((t) => t.workflowId)).toEqual(["wf-0"]);
    });
  });
});
