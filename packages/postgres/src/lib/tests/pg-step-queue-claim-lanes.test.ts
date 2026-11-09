import { describe, it, expect, beforeAll, afterAll, setDefaultTimeout } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { PostgresTestContainer } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PgStepQueue } from "../pg-step-queue.ts";

setDefaultTimeout(120_000);

// ---------------------------------------------------------------------------
// The lane claim must pick exactly what the original scan picks (the scan
// stays available as `claimStrategy: "scan"` for this comparison), and its
// cost must not grow with a throttled backlog.
// ---------------------------------------------------------------------------

const pg = new PostgresTestContainer();
beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 120_000);
afterAll(async () => {
  await pg.stop();
});

const NOW = new Date("2026-03-01T12:00:00.000Z");

/** Small deterministic PRNG so a failing seed reproduces. */
function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

async function seedRandom(seed: number, opts: { mixedPriority: boolean }): Promise<void> {
  const r = rng(seed);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(r() * xs.length)]!;
  await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
  const n = 60 + Math.floor(r() * 120);
  for (let i = 0; i < n; i++) {
    const keyed = r() < 0.5;
    const grouped = r() < 0.4;
    const running = r() < 0.15;
    const key = keyed ? pick(["a", "b", "c", "d"]) : null;
    const group = grouped ? pick(["g1", "g2"]) : null;
    await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, priority, needs,
        concurrency_key, concurrency_scope, concurrency_limit,
        group_name, group_max_concurrent, group_max_starts, group_window_ms, fairness_key,
        status, claimed_at, created_at)
      VALUES (${`wf-${i}`}, 's', ${opts.mixedPriority ? pick([3, 5, 5, 7]) : 5},
        ${pick(["{}", "{}", "{gpu}"])}::text[],
        ${key}, ${keyed ? "sc" : null}, ${keyed ? (key === "a" ? 1 : 2) : null},
        ${group}, ${grouped ? (group === "g1" ? 2 : 3) : null},
        ${grouped && group === "g2" ? 4 : null}, ${grouped && group === "g2" ? 60_000 : null},
        ${grouped ? pick(["f1", "f2", "f3"]) : null},
        ${running ? "running" : "pending"}, ${running ? new Date(NOW.getTime() - 1000).toISOString() : null}::timestamptz,
        ${new Date(NOW.getTime() - (n - i) * 1000 + Math.floor(r() * 3) * 500).toISOString()}::timestamptz)`;
  }
}

async function drain(strategy: "scan" | "lanes", capabilities: string[]): Promise<string[][]> {
  const q = new PgStepQueue({
    db: pg.db,
    claimStrategy: strategy,
    clock: FakeWallClock.create(NOW),
  });
  const rounds: string[][] = [];
  for (let i = 0; i < 6; i++) {
    const got = await q.claim({ workerId: `w${i}`, limit: 1 + (i % 4) * 3, capabilities });
    rounds.push(got.map((t) => t.workflowId));
  }
  return rounds;
}

describe("PgStepQueue claim strategies", () => {
  it("lanes claim what the scan claims: keys, groups, fairness, capabilities", async () => {
    for (let seed = 1; seed <= 25; seed++) {
      for (const capabilities of [[], ["gpu"]]) {
        await seedRandom(seed, { mixedPriority: false });
        const scan = await drain("scan", capabilities);
        await seedRandom(seed, { mixedPriority: false });
        const lanes = await drain("lanes", capabilities);
        expect({ seed, capabilities, claimed: lanes }).toEqual({
          seed,
          capabilities,
          claimed: scan,
        });
      }
    }
  });

  it("lanes claim what the scan claims with mixed priorities", async () => {
    for (let seed = 100; seed <= 125; seed++) {
      await seedRandom(seed, { mixedPriority: true });
      const scan = await drain("scan", ["gpu"]);
      await seedRandom(seed, { mixedPriority: true });
      const lanes = await drain("lanes", ["gpu"]);
      expect({ seed, claimed: lanes }).toEqual({ seed, claimed: scan });
    }
  });

  it("lanes claim what the scan claims when throttled rows clog the head of the queue", async () => {
    const seedClogged = async () => {
      await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, status, claimed_at)
        VALUES ('run', 's', 'hot', 'sc', 1, 'running', now())`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, priority, created_at)
        SELECT 'hot-' || g, 's', 'hot', 'sc', 1, 7, '2026-01-01'::timestamptz + g * interval '1 second' FROM generate_series(1, 700) g`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, priority, created_at)
        SELECT 'k-' || g, 's', 'k' || (g % 3), 'sc', 2, 5 + (g % 2), '2026-02-01'::timestamptz + g * interval '1 second' FROM generate_series(1, 40) g`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, priority, created_at)
        SELECT 'u-' || g, 's', 4 + (g % 3), '2026-02-02'::timestamptz + g * interval '1 second' FROM generate_series(1, 20) g`;
    };
    await seedClogged();
    const scan = await drain("scan", []);
    await seedClogged();
    const lanes = await drain("lanes", []);
    expect(lanes.flat().length).toBeGreaterThan(10);
    expect(lanes).toEqual(scan);
  });

  it("a throttled backlog of one key or group does not slow claims for others", async () => {
    const BACKLOG = 40_000;
    const seedHot = async (n: number) => {
      await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, group_name, group_max_concurrent, fairness_key, status, claimed_at, created_at)
        VALUES ('run-g', 's', 'g', 1, 'f0', 'running', now(), now() - interval '2 days')`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, status, claimed_at, created_at)
        VALUES ('run-k', 's', 'hot', 'sc', 1, 'running', now(), now() - interval '2 days')`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, group_name, group_max_concurrent, fairness_key, created_at)
        SELECT 'g-' || g, 's', 'g', 1, 'f' || (g % 4), now() - interval '1 day' + g * interval '1 millisecond'
        FROM generate_series(1, ${n}::int) g`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name, concurrency_key, concurrency_scope, concurrency_limit, created_at)
        SELECT 'k-' || g, 's', 'hot', 'sc', 1, now() - interval '1 day' + g * interval '1 millisecond'
        FROM generate_series(1, ${n}::int) g`;
      await pg.sql`INSERT INTO wf_step_queue (workflow_id, step_name)
        SELECT 'o-' || g, 's' FROM generate_series(1, 500) g`;
      await pg.sql`ANALYZE wf_step_queue`;
    };
    const median = async (strategy: "scan" | "lanes"): Promise<number> => {
      const q = new PgStepQueue({ db: pg.db, claimStrategy: strategy });
      const samples: number[] = [];
      for (let i = 0; i < 5; i++) {
        const t = performance.now();
        const got = await q.claim({ workerId: "w", limit: 10 });
        samples.push(performance.now() - t);
        expect(got.length).toBe(10);
        expect(got.every((x) => x.workflowId.startsWith("o-"))).toBe(true);
      }
      return samples.sort((a, b) => a - b)[2]!;
    };

    await seedHot(10);
    const small = await median("lanes");
    await seedHot(BACKLOG);
    const big = await median("lanes");
    await seedHot(BACKLOG);
    const scan = await median("scan");
    console.log(
      `[claim] lanes small=${small.toFixed(1)}ms 2x${BACKLOG}=${big.toFixed(1)}ms scan=${scan.toFixed(1)}ms`,
    );
    // Independent of backlog depth (generous: shared CI hosts are noisy) and
    // far below the scan, which pays for every throttled row.
    expect(big).toBeLessThan(small * 4 + 40);
    expect(big * 4).toBeLessThan(scan);
  });
});
