import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { FakeWallClock } from "@promin/workflow";
import type { StepQueueEnqueueParams } from "@promin/workflow/distributed";
import { SqliteStepQueue } from "../sqlite-step-queue.ts";

// ---------------------------------------------------------------------------
// Claim cost must follow the lanes a claim opens, not the pending count: a
// deep backlog held back by a concurrency key or a step group is never read.
// ---------------------------------------------------------------------------

const BACKLOG = 20_000;

function open(params: { clock?: FakeWallClock } = {}) {
  const db = new Database(":memory:");
  return { db, queue: SqliteStepQueue.make({ db, ...params }) };
}

async function enqueueMany(params: {
  db: Database;
  queue: SqliteStepQueue;
  count: number;
  make: (i: number) => StepQueueEnqueueParams;
}): Promise<void> {
  params.db.run("BEGIN");
  for (let i = 0; i < params.count; i++) await params.queue.enqueue(params.make(i));
  params.db.run("COMMIT");
}

/** Median wall time of a claim that is released straight away, so it repeats. */
async function medianClaimMs(queue: SqliteStepQueue, runs = 9): Promise<number> {
  const times: number[] = [];
  for (let i = 0; i < runs; i++) {
    const start = performance.now();
    const got = await queue.claim({ workerId: "w", limit: 5 });
    times.push(performance.now() - start);
    for (const t of got) await queue.release({ taskId: t.id, claimToken: t.claimToken! });
  }
  return times.sort((a, b) => a - b)[Math.floor(runs / 2)]!;
}

/** The same free work with and without a throttled backlog ahead of it. */
async function claimCostWithBacklog(params: {
  held: (i: number) => StepQueueEnqueueParams;
  fillCap: (queue: SqliteStepQueue) => Promise<void>;
}) {
  const free = (i: number): StepQueueEnqueueParams => ({
    workflowId: `free-${i}`,
    stepName: "s",
    input: {},
    priority: 1,
  });
  const bare = open();
  await enqueueMany({ ...bare, count: 5, make: free });
  const baseline = await medianClaimMs(bare.queue);

  const deep = open();
  await enqueueMany({ ...deep, count: BACKLOG, make: params.held });
  await enqueueMany({ ...deep, count: 5, make: free });
  await params.fillCap(deep.queue);
  const claimed = await deep.queue.claim({ workerId: "w", limit: 5 });
  expect(claimed.map((t) => t.workflowId).sort()).toEqual([
    "free-0",
    "free-1",
    "free-2",
    "free-3",
    "free-4",
  ]);
  for (const t of claimed) await deep.queue.release({ taskId: t.id, claimToken: t.claimToken! });
  return { baseline, withBacklog: await medianClaimMs(deep.queue) };
}

describe("SqliteStepQueue claim at scale", () => {
  it("a deep backlog behind a full concurrency key costs a claim nothing", async () => {
    const { baseline, withBacklog } = await claimCostWithBacklog({
      held: (i) => ({
        workflowId: `held-${i}`,
        stepName: "s",
        input: {},
        priority: 9,
        concurrencyKey: "hot",
        concurrencyScope: "s",
        concurrencyLimit: 1,
      }),
      // One running task fills the key's single slot.
      fillCap: async (queue) => {
        const [first] = await queue.claim({ workerId: "other", limit: 1 });
        expect(first!.workflowId).toBe("held-0");
      },
    });
    expect(withBacklog).toBeLessThan(baseline * 5 + 8);
  });

  it("a deep backlog behind a full step group costs a claim nothing", async () => {
    const { baseline, withBacklog } = await claimCostWithBacklog({
      held: (i) => ({
        workflowId: `held-${i}`,
        stepName: "s",
        input: {},
        priority: 9,
        groupName: "openai",
        groupMaxConcurrent: 1,
        fairnessKey: `tenant-${i % 200}`,
      }),
      fillCap: async (queue) => {
        const [first] = await queue.claim({ workerId: "other", limit: 1 });
        expect(first!.groupName).toBe("openai");
      },
    });
    expect(withBacklog).toBeLessThan(baseline * 5 + 8);
  });

  it("a group that has used its starts is skipped however much history it has", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { db, queue } = open({ clock });
    const group = { groupName: "rate", groupMaxStarts: 2, groupWindowMs: 60_000 } as const;
    // 20k settled starts of the group, all long out of its window.
    await enqueueMany({
      db,
      queue,
      count: BACKLOG,
      make: (i) => ({ workflowId: `old-${i}`, stepName: "s", input: {}, ...group }),
    });
    const longAgo = clock.currentTimeMs() - 3_600_000;
    db.query(
      `UPDATE promin_step_tasks SET status = 'completed', claimed_at = ?, completed_at = ?`,
    ).run(longAgo, longAgo);
    await enqueueMany({
      db,
      queue,
      count: 3,
      make: (i) => ({ workflowId: `new-${i}`, stepName: "s", input: {}, ...group }),
    });

    const start = performance.now();
    const claimed = await queue.claim({ workerId: "w", limit: 5 });
    const elapsed = performance.now() - start;
    // Two starts fit the window; the settled history is not read.
    expect(claimed.map((t) => t.workflowId)).toEqual(["new-0", "new-1"]);
    expect(elapsed).toBeLessThan(8);
  });

  it("fairness holds beyond any scan limit: a quiet key is served ahead of a 20k-deep noisy one", async () => {
    const { db, queue } = open();
    const g = { groupName: "fair", groupMaxConcurrent: 1000 };
    await enqueueMany({
      db,
      queue,
      count: BACKLOG,
      make: (i) => ({
        workflowId: `noisy-${i}`,
        stepName: "s",
        input: {},
        ...g,
        fairnessKey: "noisy",
      }),
    });
    await enqueueMany({
      db,
      queue,
      count: 3,
      make: (i) => ({
        workflowId: `quiet-${i}`,
        stepName: "s",
        input: {},
        ...g,
        fairnessKey: "quiet",
      }),
    });

    const claimed = await queue.claim({ workerId: "w", limit: 4 });
    expect(claimed.map((t) => t.workflowId)).toEqual(["noisy-0", "quiet-0", "noisy-1", "quiet-1"]);
  });

  it("many concurrency keys: each key's head is claimed in order, capped per key", async () => {
    const { db, queue } = open();
    await enqueueMany({
      db,
      queue,
      count: 3000,
      make: (i) => ({
        workflowId: `w-${i}`,
        stepName: "s",
        input: {},
        concurrencyKey: `k-${i % 1000}`,
        concurrencyScope: "s",
        concurrencyLimit: 1,
      }),
    });
    // 1000 keys, limit 1 each: one claim of 2000 takes exactly one task per key, the oldest.
    const claimed = await queue.claim({ workerId: "w", limit: 2000 });
    expect(claimed).toHaveLength(1000);
    expect(claimed.map((t) => t.workflowId)).toEqual(
      Array.from({ length: 1000 }, (_, i) => `w-${i}`),
    );
  });

  it("a grouped task whose concurrency key is full does not hide the group's other tasks, and runs once the key frees", async () => {
    const { queue } = open();
    const both = (workflowId: string, key: string): StepQueueEnqueueParams => ({
      workflowId,
      stepName: "s",
      input: {},
      groupName: "g",
      groupMaxConcurrent: 5,
      concurrencyKey: key,
      concurrencyScope: "s",
      concurrencyLimit: 1,
    });
    await queue.enqueue(both("a", "k1"));
    await queue.enqueue(both("b", "k1"));
    await queue.enqueue(both("c", "k2"));

    const first = await queue.claim({ workerId: "w", limit: 3 });
    expect(first.map((t) => t.workflowId)).toEqual(["a", "c"]);
    await queue.complete({
      taskId: first[0]!.id,
      claimToken: first[0]!.claimToken!,
      result: 1,
      durationMs: 1,
    });
    expect((await queue.claim({ workerId: "w", limit: 3 })).map((t) => t.workflowId)).toEqual([
      "b",
    ]);
  });

  it("reads a task an older release inserted after the upgrade", async () => {
    const { db, queue } = open();
    // An older release has no `lane` column to write; the column is generated.
    db.run(
      `INSERT INTO promin_step_tasks (id, workflow_id, step_name, input, created_at, active_key,
         concurrency_key, concurrency_scope, concurrency_limit)
       VALUES ('legacy', 'wf-legacy', 's', '{}', 1, 'wf-legacy::s', 'k', 's', 1)`,
    );
    const claimed = await queue.claim({ workerId: "w", limit: 5 });
    expect(claimed.map((t) => t.id)).toEqual(["legacy"]);
  });
});
