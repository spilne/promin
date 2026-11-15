import { describe, it, expect, beforeAll, afterAll, setDefaultTimeout } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { createWorker, MapStepRegistry } from "@promin/workflow/distributed";
import { succeed } from "@spilne/perfect-core";
import { PostgresTestContainer } from "../test-utils.ts";
import { migrate } from "../migrate.ts";
import { PgStepQueue } from "../pg-step-queue.ts";
import { PostgresWorkflowStorage } from "../postgres-workflow-storage.ts";
import { PgWakeups, postgresJsListen, type PgListenConnect } from "../pg-wakeups.ts";

setDefaultTimeout(60_000);

// ---------------------------------------------------------------------------
// LISTEN/NOTIFY wakeups over a real Postgres. Wakes are hints: the worker
// below polls once a minute and still claims within a moment of an enqueue.
// ---------------------------------------------------------------------------

const pg = new PostgresTestContainer();
beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 120_000);
afterAll(async () => {
  await pg.stop();
});

async function eventually(check: () => boolean | Promise<boolean>, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  expect(await check()).toBe(true);
}

const listenerConnections = async () =>
  (
    await pg.sql`SELECT pid FROM pg_stat_activity WHERE application_name = 'promin-wakeups-test'`
  ).map((r) => Number(r.pid));

function wakeups(extra?: { minReconnectMs?: number; onError?: (e: unknown) => void }) {
  return new PgWakeups({
    db: pg.db,
    connect: postgresJsListen({
      url: pg.url,
      options: { connection: { application_name: "promin-wakeups-test" } },
    }),
    minReconnectMs: 50,
    maxReconnectMs: 200,
    ...extra,
  });
}

describe("PgWakeups", () => {
  it("delivers a notify from another connection to the matching topic, with its key", async () => {
    const listener = wakeups();
    const sender = wakeups();
    const seen: Array<[string, string | undefined]> = [];
    const off = listener.onWake({ topic: "results", handler: (k) => seen.push(["results", k]) });
    const offSteps = listener.onWake({ topic: "steps", handler: (k) => seen.push(["steps", k]) });
    await eventually(async () => (await listenerConnections()).length === 1);

    await sender.notify({ topic: "results", key: "42" });
    await sender.notify({ topic: "steps" });
    await eventually(() => seen.length === 2);
    expect(seen).toEqual([
      ["results", "42"],
      ["steps", undefined],
    ]);

    off();
    offSteps();
    // The last unsubscribe closes the dedicated connection.
    await eventually(async () => (await listenerConnections()).length === 0);
    await listener.close();
  });

  it("reconnects with backoff after the connection is killed, and wakes subscribers once", async () => {
    const errors: unknown[] = [];
    const w = wakeups({ onError: (e) => errors.push(e) });
    let wakes = 0;
    const keys: Array<string | undefined> = [];
    w.onWake({
      topic: "signals",
      handler: (k) => {
        wakes++;
        keys.push(k);
      },
    });
    await eventually(async () => (await listenerConnections()).length === 1);
    const [pid] = await listenerConnections();

    await pg.sql`SELECT pg_terminate_backend(${pid!})`;
    // Reconnected on a new backend; the reconnect itself is the one wake.
    await eventually(async () => {
      const now = await listenerConnections();
      return now.length === 1 && now[0] !== pid && wakes === 1;
    });
    expect(keys).toEqual([undefined]);
    expect(errors.length).toBeGreaterThan(0);

    await w.notify({ topic: "signals", key: "wf-1" });
    await eventually(() => wakes === 2);
    expect(keys[1]).toBe("wf-1");
    await w.close();
    await eventually(async () => (await listenerConnections()).length === 0);
  });

  it("retries a failing connect with capped exponential backoff", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const attemptsAt: number[] = [];
    const connect: PgListenConnect = async () => {
      attemptsAt.push(clock.currentTimeMs());
      if (attemptsAt.length < 5) throw new Error("db down");
      return { close: async () => {} };
    };
    const w = new PgWakeups({
      db: pg.db,
      connect,
      clock,
      random: () => 1, // no jitter shave: delay = base * 1
      minReconnectMs: 100,
      maxReconnectMs: 500,
      onError: () => {},
    });
    w.onWake({ topic: "steps", handler: () => {} });
    for (let i = 0; i < 40 && attemptsAt.length < 5; i++) {
      await new Promise((r) => setImmediate(r));
      clock.advance(50);
    }
    const t0 = attemptsAt[0]!;
    const gaps = attemptsAt.slice(1).map((t, i) => t - attemptsAt[i]!);
    // 100, 200, 400, 500 (capped), each within one 50 ms clock step.
    expect(gaps.map((g) => Math.round(g / 50) * 50)).toEqual([100, 200, 400, 500]);
    expect(t0).toBeGreaterThanOrEqual(0);
    await w.close();
  });

  it("wakes a worker that polls once a minute as soon as a task is enqueued", async () => {
    const claimedBy = async (
      queueWakeups: PgWakeups | undefined,
      workerWakeups: PgWakeups | undefined,
    ) => {
      await pg.sql`TRUNCATE wf_step_queue RESTART IDENTITY`;
      const queue = new PgStepQueue({ db: pg.db, ...(queueWakeups && { wakeups: queueWakeups }) });
      const registry = new MapStepRegistry();
      registry.register({ stepName: "s", handler: () => succeed("done") });
      const worker = createWorker({
        stepQueue: queue,
        registry,
        capabilities: [],
        pollIntervalMs: 60_000,
        ...(workerWakeups && { wakeups: workerWakeups }),
      });
      void worker.start();
      // The first claim runs at start; the next is a minute away.
      await new Promise((r) => setTimeout(r, 300));
      const id = await queue.enqueue({ workflowId: "w", stepName: "s", input: {} });
      const settled = async () => (await queue.get(id))?.status === "completed";
      let ok = false;
      try {
        await eventually(settled, 1_500);
        ok = true;
      } catch {
        ok = false;
      }
      await worker.stop();
      return ok;
    };

    const shared = wakeups();
    expect(await claimedBy(shared, shared)).toBe(true);
    await shared.close();
    // Same setup without the wake channel waits for the minute.
    expect(await claimedBy(undefined, undefined)).toBe(false);
  });

  it("signals delivered through the storage wake a listener, keyed by workflow id", async () => {
    const w = wakeups();
    const storage = await PostgresWorkflowStorage.create({ db: pg.db, wakeups: w });
    await pg.sql`INSERT INTO wf_workflows (workflow_id, workflow_name, input) VALUES ('sig-1', 'w', '{}'::jsonb)`;
    const keys: Array<string | undefined> = [];
    w.onWake({ topic: "signals", handler: (k) => keys.push(k) });
    await eventually(async () => (await listenerConnections()).length === 1);
    await storage.deliverSignal({ workflowId: "sig-1", signalName: "go", payload: { ok: true } });
    await eventually(() => keys.includes("sig-1"));
    await w.close();
  });
});
