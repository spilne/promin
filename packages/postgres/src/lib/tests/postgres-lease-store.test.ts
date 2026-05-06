// ---------------------------------------------------------------------------
// PostgresLeaseStore — runs the @promin/agent leaseStoreTestSuite
// against a real PG container, plus contention specifics that only
// matter when multiple connections race.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { leaseStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../migrate.ts";
import { PostgresLeaseStore } from "../postgres-lease-store.ts";
import { PostgresTestContainer } from "../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_thread_lease`;
});

leaseStoreTestSuite((getNow) => new PostgresLeaseStore({ db: pg.db, now: getNow }));

describe("PostgresLeaseStore — Postgres-specific", () => {
  it("two stores against the same db see each other's writes", async () => {
    const a = new PostgresLeaseStore({ db: pg.db });
    const b = new PostgresLeaseStore({ db: pg.db });
    const ra = await a.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      ttlMs: 60_000,
    });
    expect(ra.acquired).toBe(true);
    const rb = await b.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-B",
      ttlMs: 60_000,
    });
    expect(rb.acquired).toBe(false);
    if (!rb.acquired) {
      expect(rb.currentLease.ownerId).toBe("worker-A");
    }
  });

  it("under high concurrency, exactly one of N acquires wins", async () => {
    const stores = Array.from({ length: 10 }, () => new PostgresLeaseStore({ db: pg.db }));
    const results = await Promise.all(
      stores.map((s, i) =>
        s.acquire({
          key: { namespaceId: "acme", threadId: "t-race" },
          ownerId: `worker-${i}`,
          ttlMs: 60_000,
        }),
      ),
    );
    const winners = results.filter((r) => r.acquired);
    expect(winners.length).toBe(1);
    const losers = results.filter((r) => !r.acquired);
    expect(losers.length).toBe(9);
    // All losers should agree on who won.
    const winnerOwner = winners[0]!.acquired ? winners[0]!.lease.ownerId : "";
    for (const l of losers) {
      if (!l.acquired) {
        expect(l.currentLease.ownerId).toBe(winnerOwner);
      }
    }
  });

  it("release is bounded to the holder's leaseId — won't drop a stolen lease", async () => {
    const s = new PostgresLeaseStore({ db: pg.db });
    let now = 1_000_000;
    const clocked = new PostgresLeaseStore({ db: pg.db, now: () => now });

    // Worker A acquires with short TTL via the clock-injected store.
    const a = await clocked.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-A",
      ttlMs: 1_000,
    });
    if (!a.acquired) throw new Error("setup");

    // Time advances; worker B steals.
    now += 5_000;
    const b = await clocked.acquire({
      key: { namespaceId: "acme", threadId: "t-1" },
      ownerId: "worker-B",
      ttlMs: 60_000,
    });
    if (!b.acquired) throw new Error("setup steal");

    // Worker A's stale release should NOT remove B's lease.
    await s.release({ leaseId: a.lease.leaseId });
    const cur = await s.get({ namespaceId: "acme", threadId: "t-1" });
    expect(cur?.ownerId).toBe("worker-B");
  });
});
