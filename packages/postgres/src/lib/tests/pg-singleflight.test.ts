import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { PostgresTestContainer } from "../test-utils.ts";
import { singleflightTestSuite } from "@promin/core/testing";
import { PgSingleflight } from "../pg-singleflight.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
}, 60_000);

afterAll(async () => {
  await pg.stop();
});

// ---------------------------------------------------------------------------
// Conformance suite
// ---------------------------------------------------------------------------

let setupDone = false;

singleflightTestSuite(async () => {
  if (!setupDone) {
    const sf = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });
    // @ts-ignore
    await sf._ensureReady();
    setupDone = true;
  }
  await pg.sql`TRUNCATE promin_singleflight`;
  return new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });
});

// ---------------------------------------------------------------------------
// Postgres-specific tests
// ---------------------------------------------------------------------------

describe("PgSingleflight — Postgres-specific", () => {
  beforeEach(async () => {
    await pg.sql`TRUNCATE promin_singleflight`;
  });

  it("multiple processes share a single flight via DB coordination", async () => {
    // Simulate two separate PgSingleflight instances (separate processes)
    const sf1 = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });
    const sf2 = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });

    let executions = 0;

    // sf1 starts the flight
    const p1 = sf1.doAsync("shared-key", async () => {
      executions++;
      await new Promise<void>((r) => setTimeout(r, 80));
      return "done";
    });

    // Give sf1 time to insert the row
    await new Promise<void>((r) => setTimeout(r, 20));

    // sf2 joins as loser and waits
    const p2 = sf2.doAsync("shared-key", async () => {
      executions++;
      return "done";
    });

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1).toBe("done");
    expect(r2).toBe("done");
    // Only one execution should have happened (sf1 is winner, sf2 polls for result)
    expect(executions).toBe(1);
  });

  it("error from winner propagates to loser instance", async () => {
    const sf1 = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });
    const sf2 = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });

    let executions = 0;

    const p1 = sf1.doAsync("err-key", async () => {
      executions++;
      await new Promise<void>((r) => setTimeout(r, 60));
      throw new Error("distributed failure");
    });

    await new Promise<void>((r) => setTimeout(r, 20));

    const p2 = sf2.doAsync("err-key", async () => {
      executions++;
      return "never";
    });

    const [r1, r2] = await Promise.allSettled([p1, p2]);
    expect(r1.status).toBe("rejected");
    expect(r2.status).toBe("rejected");
    if (r1.status === "rejected") expect(r1.reason.message).toBe("distributed failure");
    if (r2.status === "rejected") expect(r2.reason.message).toBe("distributed failure");
    expect(executions).toBe(1);
  });

  it("new flight can start after previous completes", async () => {
    const sf = new PgSingleflight({ db: pg.db, pollIntervalMs: 10 });

    const r1 = await sf.doAsync("seq-key", async () => "first");
    // Give cleanup a moment
    await new Promise<void>((r) => setTimeout(r, 1100));
    const r2 = await sf.doAsync("seq-key", async () => "second");

    expect(r1).toBe("first");
    expect(r2).toBe("second");
  });
});
