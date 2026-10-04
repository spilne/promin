// ---------------------------------------------------------------------------
// Postgres scheduler leadership — lease rows on the server clock, not session
// advisory locks. The portable lease/fencing cases run in the conformance
// suite; these cover what is specific to a pooled Postgres client.
// ---------------------------------------------------------------------------

import { it, expect } from "bun:test";
import { sql } from "drizzle-orm";
import { schedulePartition, schedulerLeaderKey } from "@promin/workflow/scheduler";
import { postgresDescribe } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { createDurableScheduler } from "../durable-scheduler.ts";
import { PgSchedulerStorage } from "../pg-scheduler-storage.ts";

postgresDescribe("Postgres scheduler leadership", { migrate }, (pg) => {
  const reset = () =>
    pg.db.execute(sql`TRUNCATE TABLE wf_schedules, wf_schedule_ticks, wf_leader_leases CASCADE`);

  it("the leader keeps its lease on every pooled connection under concurrent load", async () => {
    await reset();
    const storage = new PgSchedulerStorage({ db: pg.db });
    const key = schedulerLeaderKey({});
    const first = await storage.tryAcquireLeader({ key, instanceId: "A", ttlMs: 30_000 });

    const results = await Promise.all([
      ...Array.from({ length: 20 }, () => pg.sql`SELECT pg_sleep(0.01)`.then(() => undefined)),
      ...Array.from({ length: 10 }, () =>
        storage.tryAcquireLeader({ key, instanceId: "A", ttlMs: 30_000 }),
      ),
    ]);

    const leases = results.filter((r) => r !== undefined);
    expect(leases).toHaveLength(10);
    expect(leases.every((l) => l?.epoch === first!.epoch)).toBe(true);
    expect(await storage.tryAcquireLeader({ key, instanceId: "B", ttlMs: 30_000 })).toBeNull();
  });

  it("a stopped scheduler hands leadership over at once instead of after the TTL", async () => {
    await reset();
    const a = createDurableScheduler({
      db: pg.db,
      instanceId: "A",
      pollIntervalMs: 50,
      leaderLockTtlMs: 60_000,
    });
    const b = createDurableScheduler({
      db: pg.db,
      instanceId: "B",
      pollIntervalMs: 50,
      leaderLockTtlMs: 60_000,
    });
    await a.register({ id: "first", intervalMs: 60_000 });
    await a.register({ id: "second", intervalMs: 60_000 });

    const fromA = await a.stream().take(1).toArray().run();
    expect(fromA.map((t) => t.scheduleId)).toHaveLength(1);

    // A's lease would last a minute; B leads right away because A released it.
    const fromB = await b.stream().take(2).toArray().run();
    expect(fromB.map((t) => t.scheduleId).sort()).toEqual(["first", "second"]);
  }, 10_000);

  it("partitioned schedulers each lead their own partition and fire concurrently", async () => {
    await reset();
    const workers = [0, 1].map((index) =>
      createDurableScheduler({
        db: pg.db,
        instanceId: `worker-${index}`,
        pollIntervalMs: 50,
        partition: { index, count: 2 },
      }),
    );
    const ids = Array.from({ length: 12 }, (_, i) => `pg-part-${i}`);
    for (const id of ids) await workers[0]!.register({ id, intervalMs: 60_000 });
    const expected = [0, 1].map((index) =>
      ids.filter((id) => schedulePartition({ id, count: 2 }) === index).sort(),
    );

    const fired = await Promise.all(
      workers.map((w, i) => w.stream().take(expected[i]!.length).toArray().run()),
    );

    expect(fired.map((ticks) => ticks.map((t) => t.scheduleId).sort())).toEqual(expected);
  }, 10_000);
});
