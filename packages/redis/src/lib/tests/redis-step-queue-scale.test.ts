import { expect, it } from "bun:test";
import { RedisStepQueue } from "../redis-step-queue.ts";
import type { StepQueueEnqueueParams } from "@promin/workflow/distributed";
import { redisDescribe, uniquePrefix, type RedisTestContext } from "./redis-test-utils.ts";

// ---------------------------------------------------------------------------
// A claim reads the heads of the lanes that are open, not the backlog behind
// a full concurrency key or group. Cost is counted in Redis commands (the
// server counts the ones a script issues too), which does not depend on how
// loaded the machine is.
// ---------------------------------------------------------------------------

interface InfoClient {
  info(section: string): Promise<string>;
}

function setup(redis: RedisTestContext) {
  const client = redis.client();
  const info = client as unknown as InfoClient;
  const commands = async (): Promise<number> =>
    Number(/total_commands_processed:(\d+)/.exec(await info.info("stats"))![1]);
  return { client, commands, prefix: uniquePrefix("sq") };
}

async function enqueueMany(params: {
  queue: RedisStepQueue;
  count: number;
  make: (i: number) => StepQueueEnqueueParams;
}): Promise<void> {
  for (let i = 0; i < params.count; i += 250) {
    await Promise.all(
      Array.from({ length: Math.min(250, params.count - i) }, (_, j) =>
        params.queue.enqueue(params.make(i + j)),
      ),
    );
  }
}

const free = (i: number): StepQueueEnqueueParams => ({
  workflowId: `free-${i}`,
  stepName: "s",
  input: {},
  priority: 1,
});
const FREE_IDS = ["free-0", "free-1", "free-2", "free-3", "free-4"];

redisDescribe("RedisStepQueue claim at scale", (redis) => {
  /** Commands one claim of the free tasks costs behind `backlog` held-back ones. */
  async function claimCost(params: {
    backlog: number;
    held: (i: number) => StepQueueEnqueueParams;
  }): Promise<number> {
    const { client, commands, prefix } = setup(redis);
    const queue = new RedisStepQueue({ redis: client, prefix });
    await enqueueMany({ queue, count: params.backlog, make: params.held });
    await enqueueMany({ queue, count: 5, make: free });
    // One running task fills the key's / group's single slot.
    expect(await queue.claim({ workerId: "other", limit: 1 })).toHaveLength(1);

    const before = await commands();
    const claimed = await queue.claim({ workerId: "w", limit: 5 });
    const cost = (await commands()) - before - 1; // the INFO read itself
    expect(claimed.map((t) => t.workflowId).sort()).toEqual(FREE_IDS);
    return cost;
  }

  const held = (extra: Partial<StepQueueEnqueueParams>) => (i: number) => ({
    workflowId: `held-${i}`,
    stepName: "s",
    input: {},
    priority: 9,
    ...extra,
  });

  it("a backlog behind a full concurrency key is never read", async () => {
    const key = {
      concurrencyKey: "hot",
      concurrencyScope: "s",
      concurrencyLimit: 1,
    };
    const shallow = await claimCost({ backlog: 20, held: held(key) });
    const deep = await claimCost({ backlog: 3000, held: held(key) });
    expect(deep).toBeLessThanOrEqual(shallow + 10);
  });

  it("a backlog behind a full step group is never read", async () => {
    const group = (i: number) => ({
      groupName: "openai",
      groupMaxConcurrent: 1,
      fairnessKey: `tenant-${i % 50}`,
    });
    const make = (i: number) => ({ ...held({})(i), ...group(i) });
    const shallow = await claimCost({ backlog: 20, held: make });
    const deep = await claimCost({ backlog: 3000, held: make });
    expect(deep).toBeLessThanOrEqual(shallow + 10);
  });

  it("a group parked on its rate window is never read until the window frees a slot", async () => {
    const make = (i: number) => ({
      ...held({})(i),
      groupName: "rate",
      groupMaxStarts: 1,
      groupWindowMs: 3_600_000,
    });
    const shallow = await claimCost({ backlog: 20, held: make });
    const deep = await claimCost({ backlog: 3000, held: make });
    expect(deep).toBeLessThanOrEqual(shallow + 10);
  });

  it("fairness is exact past the old scan limit: a quiet key is served ahead of a 3000-deep noisy one", async () => {
    const { client, prefix } = setup(redis);
    const queue = new RedisStepQueue({ redis: client, prefix, claimScanLimit: 1000 });
    const g = { groupName: "fair", groupMaxConcurrent: 1000 };
    await enqueueMany({
      queue,
      count: 3000,
      make: (i) => ({
        workflowId: `noisy-${i}`,
        stepName: "s",
        input: {},
        ...g,
        fairnessKey: "noisy",
      }),
    });
    await enqueueMany({
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

  it("a deep priority-9 backlog on one key no longer hides work behind it, however many keys", async () => {
    const { client, prefix } = setup(redis);
    const queue = new RedisStepQueue({ redis: client, prefix });
    const keyed = (i: number, key: string): StepQueueEnqueueParams => ({
      workflowId: `${key}-${i}`,
      stepName: "s",
      input: {},
      concurrencyKey: key,
      concurrencyScope: "s",
      concurrencyLimit: 1,
    });
    await enqueueMany({ queue, count: 1500, make: (i) => ({ ...keyed(i, "hot"), priority: 9 }) });
    await enqueueMany({ queue, count: 600, make: (i) => keyed(0, `k${i}`) });

    // The hot key takes one slot; every other key one more.
    const claimed = await queue.claim({ workerId: "w", limit: 700 });
    expect(claimed).toHaveLength(601);
    expect(claimed[0]!.workflowId).toBe("hot-0");
    expect(await queue.claim({ workerId: "w", limit: 700 })).toHaveLength(0);
  });

  it("a grouped task whose concurrency key is full does not hide the group's other tasks, and runs once the key frees", async () => {
    const { client, prefix } = setup(redis);
    const queue = new RedisStepQueue({ redis: client, prefix });
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

  it("takes a task deeper in a lane when its head is another step, in priority order across lanes", async () => {
    const { client, prefix } = setup(redis);
    const queue = new RedisStepQueue({ redis: client, prefix });
    const key = { concurrencyKey: "k", concurrencyScope: "s", concurrencyLimit: 5 };
    await queue.enqueue({
      workflowId: "foreign",
      stepName: "other",
      input: {},
      priority: 9,
      ...key,
    });
    await queue.enqueue({ workflowId: "deep", stepName: "mine", input: {}, priority: 3, ...key });
    await queue.enqueue({ workflowId: "plain", stepName: "mine", input: {}, priority: 5 });

    const claimed = await queue.claim({ workerId: "w", limit: 2, stepNames: ["mine"] });
    expect(claimed.map((t) => t.workflowId)).toEqual(["plain", "deep"]);
  });
});

redisDescribe("RedisStepQueue upgrade from the pre-lane layout", (redis) => {
  /** Drop every key the lanes added: what a queue written by an older release looks like. */
  async function toLegacyLayout(params: {
    client: ReturnType<RedisTestContext["client"]>;
    prefix: string;
  }) {
    await params.client.eval(
      `for i = 1, #ARGV do for _, k in ipairs(redis.call('KEYS', ARGV[i])) do redis.call('DEL', k) end end return 1`,
      1,
      `{${params.prefix}}:counter`,
      `{${params.prefix}}:lane:*`,
      `{${params.prefix}}:ready`,
      `{${params.prefix}}:rmem`,
      `{${params.prefix}}:gready:*`,
      `{${params.prefix}}:gmem:*`,
      `{${params.prefix}}:gtimers`,
      `{${params.prefix}}:lanes:v2`,
    );
  }

  it("indexes a queue written before the lanes on its first claim, and claims as an untouched twin does", async () => {
    const workload = async (q: RedisStepQueue) => {
      const g = { groupName: "g", groupMaxConcurrent: 3 };
      for (let i = 0; i < 4; i++) {
        await q.enqueue({
          workflowId: `a-${i}`,
          stepName: "s",
          input: {},
          ...g,
          fairnessKey: "a",
          priority: i === 3 ? 9 : 5,
        });
      }
      for (let i = 0; i < 2; i++) {
        await q.enqueue({ workflowId: `b-${i}`, stepName: "s", input: {}, ...g, fairnessKey: "b" });
      }
      for (let i = 0; i < 3; i++) {
        await q.enqueue({
          workflowId: `k-${i}`,
          stepName: "s",
          input: {},
          concurrencyKey: "k",
          concurrencyScope: "s",
          concurrencyLimit: 1,
        });
      }
      for (let i = 0; i < 3; i++)
        await q.enqueue({ workflowId: `p-${i}`, stepName: "s", input: {} });
    };
    const drain = async (q: RedisStepQueue) => {
      const order: string[] = [];
      for (let round = 0; round < 6; round++) {
        const claimed = await q.claim({ workerId: "w", limit: 3 });
        order.push(claimed.map((t) => t.workflowId).join(","));
        for (const t of claimed) {
          await q.complete({ taskId: t.id, claimToken: t.claimToken!, result: 1, durationMs: 1 });
        }
      }
      return order;
    };

    const client = redis.client();
    const twinPrefix = uniquePrefix("twin");
    const legacyPrefix = uniquePrefix("legacy");
    const twin = new RedisStepQueue({ redis: client, prefix: twinPrefix });
    const legacy = new RedisStepQueue({ redis: client, prefix: legacyPrefix });
    await workload(twin);
    await workload(legacy);
    // One task already running when the lanes were introduced.
    const [running] = await legacy.claim({ workerId: "old", limit: 1 });
    const [twinRunning] = await twin.claim({ workerId: "old", limit: 1 });
    expect(running!.workflowId).toBe(twinRunning!.workflowId);
    // `pending`, the task hashes and the running sets stay, as the old layout had them.
    await toLegacyLayout({ client, prefix: legacyPrefix });

    expect(await drain(legacy)).toEqual(await drain(twin));
    // Indexed once: the marker is set and a second queue instance claims normally.
    const again = new RedisStepQueue({ redis: client, prefix: legacyPrefix });
    await again.enqueue({ workflowId: "late", stepName: "s", input: {} });
    expect((await again.claim({ workerId: "w", limit: 1 })).map((t) => t.workflowId)).toEqual([
      "late",
    ]);
  });
});
