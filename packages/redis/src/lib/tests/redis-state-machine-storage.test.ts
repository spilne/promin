import { expect, it } from "bun:test";
import { FakeWallClock, stateMachine } from "@promin/workflow";
import type { RedisStoreClient } from "../redis-client.ts";
import { RedisStateMachineStorage } from "../redis-state-machine-storage.ts";
import { redisDescribe, uniquePrefix } from "./redis-test-utils.ts";

async function pttl(redis: RedisStoreClient, key: string): Promise<number> {
  return (await redis.eval("return redis.call('PTTL', KEYS[1])", 1, key)) as number;
}

redisDescribe("RedisStateMachineStorage", (ctx) => {
  function make(config?: { terminalTtlMs?: number; activeTtlMs?: number }) {
    const redis = ctx.client();
    const prefix = uniquePrefix("sm");
    const clock = FakeWallClock.create(Date.parse("2026-01-01T00:00:00.000Z"));
    const storage = new RedisStateMachineStorage({ redis, prefix, clock, ...config });
    return { redis, prefix, clock, storage };
  }

  it("create + load round-trips every field", async () => {
    const { storage } = make();
    await storage.create({
      id: "m1",
      name: "order",
      type: "order-type",
      namespace: "tenant-a",
      initial: "draft",
      context: { items: ["a"] },
      version: "v2",
      metadata: { owner: "u1" },
    });

    expect(await storage.load("m1")).toEqual({
      id: "m1",
      name: "order",
      type: "order-type",
      namespace: "tenant-a",
      current: "draft",
      context: { items: ["a"] },
      version: "v2",
      metadata: { owner: "u1" },
      revision: 0,
      createdAt: new Date("2026-01-01T00:00:00.000Z"),
      updatedAt: new Date("2026-01-01T00:00:00.000Z"),
    });
  });

  it("load returns null for an unknown machine", async () => {
    const { storage } = make();
    expect(await storage.load("missing")).toBeNull();
  });

  it("transition updates the snapshot and appends an event with eventData", async () => {
    const { storage, clock } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: { n: 0 } });
    clock.advance(1_000);

    await storage.transition({
      id: "m1",
      from: "a",
      to: "b",
      expectedRevision: 0,
      event: "go",
      context: { n: 1 },
      eventData: { reason: "test" },
      metadata: { actor: "u1" },
    });

    const state = await storage.load("m1");
    expect(state?.current).toBe("b");
    expect(state?.context).toEqual({ n: 1 });
    expect(state?.updatedAt).toEqual(new Date("2026-01-01T00:00:01.000Z"));

    const events = await storage.loadEvents("m1");
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      event: "go",
      from: "a",
      to: "b",
      context: { n: 1 },
      eventData: { reason: "test" },
      metadata: { actor: "u1" },
      createdAt: new Date("2026-01-01T00:00:01.000Z"),
    });
  });

  it("transition rejects a stale `from` without writing anything", async () => {
    const { storage } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });

    await expect(
      storage.transition({
        id: "m1",
        from: "x",
        to: "b",
        expectedRevision: 0,
        event: "go",
        context: {},
      }),
    ).rejects.toThrow('Machine m1 is in state "a" at revision 0, not "x" at revision 0');
    expect((await storage.load("m1"))?.current).toBe("a");
    expect(await storage.loadEvents("m1")).toEqual([]);
  });

  it("transition on an unknown machine throws", async () => {
    const { storage } = make();
    await expect(
      storage.transition({
        id: "nope",
        from: "a",
        to: "b",
        expectedRevision: 0,
        event: "go",
        context: {},
      }),
    ).rejects.toThrow("Machine nope not found");
  });

  it("concurrent transitions from the same state — exactly one wins", async () => {
    const { storage, prefix } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });
    const others = Array.from(
      { length: 5 },
      () => new RedisStateMachineStorage({ redis: ctx.client(), prefix }),
    );

    const results = await Promise.allSettled(
      others.map((s, i) =>
        s.transition({
          id: "m1",
          from: "a",
          to: `b${i}`,
          expectedRevision: 0,
          event: "go",
          context: { i },
        }),
      ),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(await storage.loadEvents("m1")).toHaveLength(1);
  });

  it("loadEvents pages with limit + offset", async () => {
    const { storage } = make();
    await storage.create({ id: "m1", name: "n", initial: "s0", context: {} });
    for (let i = 0; i < 5; i++) {
      await storage.transition({
        id: "m1",
        from: `s${i}`,
        to: `s${i + 1}`,
        expectedRevision: i,
        event: `e${i}`,
        context: {},
      });
    }

    expect((await storage.loadEvents("m1", { limit: 2 })).map((e) => e.event)).toEqual([
      "e0",
      "e1",
    ]);
    expect((await storage.loadEvents("m1", { limit: 2, offset: 3 })).map((e) => e.event)).toEqual([
      "e3",
      "e4",
    ]);
    expect((await storage.loadEvents("m1", { offset: 4 })).map((e) => e.event)).toEqual(["e4"]);
    expect(await storage.loadEvents("m1", { limit: 0 })).toEqual([]);
  });

  it("re-creating a machine resets its history", async () => {
    const { storage } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });
    await storage.transition({
      id: "m1",
      from: "a",
      to: "b",
      expectedRevision: 0,
      event: "go",
      context: {},
    });
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });
    expect(await storage.loadEvents("m1")).toEqual([]);
    expect((await storage.load("m1"))?.revision).toBe(0);
  });

  it("a machine written without a revision field takes its history length as revision", async () => {
    const { storage, redis, prefix } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });
    await storage.transition({
      id: "m1",
      from: "a",
      to: "a",
      expectedRevision: 0,
      event: "t",
      context: {},
    });
    await storage.transition({
      id: "m1",
      from: "a",
      to: "a",
      expectedRevision: 1,
      event: "t",
      context: {},
    });
    // Snapshot shape from before revisions were stored.
    await redis.eval("return redis.call('HDEL', KEYS[1], 'revision')", 1, `${prefix}:machine:m1`);

    expect((await storage.load("m1"))?.revision).toBe(2);
    await expect(
      storage.transition({
        id: "m1",
        from: "a",
        to: "a",
        expectedRevision: 0,
        event: "t",
        context: {},
      }),
    ).rejects.toThrow("at revision 2");
    await storage.transition({
      id: "m1",
      from: "a",
      to: "a",
      expectedRevision: 2,
      event: "t",
      context: {},
    });
    expect((await storage.load("m1"))?.revision).toBe(3);
  });

  it("applies the active TTL while running and the terminal TTL once terminal", async () => {
    const { storage, redis, prefix } = make({ activeTtlMs: 60_000, terminalTtlMs: 5_000 });
    storage.registerTerminalStates(["done"]);
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });

    expect(await pttl(redis, `${prefix}:machine:m1`)).toBeGreaterThan(5_000);

    await storage.transition({
      id: "m1",
      from: "a",
      to: "b",
      expectedRevision: 0,
      event: "go",
      context: {},
    });
    expect(await pttl(redis, `${prefix}:machine:m1`)).toBeGreaterThan(5_000);
    expect(await pttl(redis, `${prefix}:events:m1`)).toBeGreaterThan(5_000);

    await storage.transition({
      id: "m1",
      from: "b",
      to: "done",
      expectedRevision: 1,
      event: "finish",
      context: {},
    });
    expect(await pttl(redis, `${prefix}:machine:m1`)).toBeLessThanOrEqual(5_000);
    expect(await pttl(redis, `${prefix}:events:m1`)).toBeLessThanOrEqual(5_000);
  });

  it("leaves keys without expiry when no TTLs are configured", async () => {
    const { storage, redis, prefix } = make();
    await storage.create({ id: "m1", name: "n", initial: "a", context: {} });
    await storage.transition({
      id: "m1",
      from: "a",
      to: "b",
      expectedRevision: 0,
      event: "go",
      context: {},
    });
    expect(await pttl(redis, `${prefix}:machine:m1`)).toBe(-1);
    expect(await pttl(redis, `${prefix}:events:m1`)).toBe(-1);
  });

  it("tryLock is exclusive until released", async () => {
    const { storage, prefix } = make();
    const other = new RedisStateMachineStorage({ redis: ctx.client(), prefix });

    const token = await storage.tryLock({ id: "m1", durationMs: 10_000 });
    expect(token).not.toBeNull();
    expect(await other.tryLock({ id: "m1", durationMs: 10_000 })).toBeNull();
    await storage.releaseLock({ id: "m1", token: token! });
    expect(await other.tryLock({ id: "m1", durationMs: 10_000 })).not.toBeNull();
  });

  it("releaseLock never frees a lock another holder acquired after expiry", async () => {
    const { storage, prefix } = make();
    const other = new RedisStateMachineStorage({ redis: ctx.client(), prefix });

    const stale = await storage.tryLock({ id: "m1", durationMs: 20 });
    expect(stale).not.toBeNull();
    await new Promise((r) => setTimeout(r, 50));
    expect(await other.tryLock({ id: "m1", durationMs: 10_000 })).not.toBeNull();

    await storage.releaseLock({ id: "m1", token: stale! });
    expect(await storage.extendLock({ id: "m1", token: stale!, durationMs: 10_000 })).toBe(false);
    expect(await storage.tryLock({ id: "m1", durationMs: 10_000 })).toBeNull();
  });

  it("extendLock resets the key's PX expiry", async () => {
    const { storage, redis, prefix } = make();
    const token = await storage.tryLock({ id: "m1", durationMs: 1_000 });
    expect(await pttl(redis, `${prefix}:lock:m1`)).toBeLessThanOrEqual(1_000);
    expect(await storage.extendLock({ id: "m1", token: token!, durationMs: 60_000 })).toBe(true);
    expect(await pttl(redis, `${prefix}:lock:m1`)).toBeGreaterThan(1_000);
  });

  it("drives a stateMachine end to end", async () => {
    type Light = {
      red: { context: { count: number }; transitions: { next: "green" } };
      green: { context: { count: number }; transitions: { stop: "off" } };
      off: { context: { count: number }; transitions: {} };
    };
    const { storage } = make();
    const light = stateMachine<Light>({ name: "light", storage })
      .state("red")
      .state("green")
      .state("off", { terminal: true })
      .on("next", {
        from: "red",
        to: "green",
        action: (c: { count: number }) => ({ count: c.count + 1 }),
      })
      .on("stop", {
        from: "green",
        to: "off",
        action: (c: { count: number }) => ({ count: c.count + 1 }),
      })
      .initial("red")
      .build();

    await light.start({ id: "l1", context: { count: 0 } });
    await light.send({ id: "l1", event: "next" });
    await light.send({ id: "l1", event: "stop" });

    expect(await light.getState("l1")).toEqual({ current: "off", context: { count: 2 } });
    expect((await light.getHistory("l1")).map((e) => e.event)).toEqual(["next", "stop"]);
  });
});
