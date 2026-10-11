// ---------------------------------------------------------------------------
// RunRetention: config validation, the deferred first sweep, batched
// deletion of ended runs past their age, leader election across instances,
// and the ZoryaServer wiring. One FakeWallClock drives everything.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, InMemoryWorkflowStorage } from "@promin/workflow";
import { InMemoryLeaderLeases } from "@promin/workflow/scheduler";
import { ZoryaServer } from "../../../server.ts";
import type { Logger } from "../../../server-config.ts";
import { QueuedWorkflows } from "../queued-workflows.ts";
import {
  RunRetention,
  validateRunRetentionConfig,
  type RunRetentionConfig,
  type RunRetentionReport,
} from "../run-retention.ts";
import type { ZoryaCluster } from "../cluster.ts";

const HOUR = 60 * 60 * 1000;
const quietLogger: Logger = { log() {}, warn() {}, error() {} };

async function flush(): Promise<void> {
  for (let i = 0; i < 20; i++) await new Promise<void>((r) => setImmediate(r));
}

/** Storage that counts `purgeCompleted` calls. */
class CountingStorage extends InMemoryWorkflowStorage {
  purgeCalls = 0;
  override async purgeCompleted(
    params: Parameters<InMemoryWorkflowStorage["purgeCompleted"]>[0],
  ): Promise<number> {
    this.purgeCalls += 1;
    return super.purgeCompleted(params);
  }
}

async function seedEnded(params: {
  storage: InMemoryWorkflowStorage;
  ids: ReadonlyArray<string>;
}): Promise<void> {
  for (const [i, workflowId] of params.ids.entries()) {
    await params.storage.createWorkflow({ workflowId, workflowName: "wf", input: {} });
    if (i % 2 === 0) await params.storage.completeWorkflow({ workflowId, result: "ok" });
    else await params.storage.failWorkflow({ workflowId, error: "boom" });
  }
}

function makeRetention(params: {
  storage: InMemoryWorkflowStorage;
  clock: FakeWallClock;
  config: RunRetentionConfig;
  cluster?: ZoryaCluster;
}): { retention: RunRetention; reports: RunRetentionReport[]; errors: unknown[] } {
  const reports: RunRetentionReport[] = [];
  const errors: unknown[] = [];
  const retention = new RunRetention({
    storage: params.storage,
    config: params.config,
    cluster: params.cluster,
    clock: params.clock,
    onSwept: (report) => reports.push(report),
    onError: (error) => errors.push(error),
  });
  return { retention, reports, errors };
}

describe("RunRetention", () => {
  it("validates its config", () => {
    expect(() => validateRunRetentionConfig({ maxAgeMs: 0 })).toThrow(
      "maxAgeMs must be a positive finite number",
    );
    expect(() => validateRunRetentionConfig({ maxAgeMs: Number.POSITIVE_INFINITY })).toThrow(
      "maxAgeMs must be a positive finite number",
    );
    expect(() => validateRunRetentionConfig({ maxAgeMs: HOUR, intervalMs: -1 })).toThrow(
      "intervalMs must be a positive finite number",
    );
    expect(() => validateRunRetentionConfig({ maxAgeMs: HOUR, batchSize: 1.5 })).toThrow(
      "batchSize must be a positive integer",
    );
    expect(() => validateRunRetentionConfig({ maxAgeMs: HOUR, intervalMs: 100 })).not.toThrow();
  });

  it("sweeps one interval after start, never at boot, and stops sweeping on stop", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new CountingStorage({ clock });
    const { retention, reports } = makeRetention({
      storage,
      clock,
      config: { maxAgeMs: HOUR, intervalMs: 1_000 },
    });

    await retention.start();
    await flush();
    expect(storage.purgeCalls).toBe(0);

    clock.advance(1_000);
    await flush();
    expect(storage.purgeCalls).toBe(1);
    expect(reports).toEqual([{ deleted: 0 }]);

    await retention.stop();
    clock.advance(10_000);
    await flush();
    expect(storage.purgeCalls).toBe(1);
  });

  it("deletes ended runs past their age in batches, keeping recent and live runs", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new CountingStorage({ clock });
    const old = ["old-1", "old-2", "old-3", "old-4", "old-5"];
    await seedEnded({ storage, ids: old });
    await storage.createWorkflow({ workflowId: "live", workflowName: "wf", input: {} });
    clock.advance(2 * HOUR);
    await seedEnded({ storage, ids: ["recent"] });

    const { retention, reports } = makeRetention({
      storage,
      clock,
      config: { maxAgeMs: HOUR, intervalMs: 1_000, batchSize: 2 },
    });
    await retention.start();
    await flush();
    clock.advance(1_000);
    await flush();
    await retention.stop();

    expect(reports).toEqual([{ deleted: 5 }]);
    // Two full batches, then a short one ends the sweep.
    expect(storage.purgeCalls).toBe(3);
    for (const id of old) expect(await storage.loadWorkflow(id)).toBeNull();
    expect(await storage.loadWorkflow("recent")).not.toBeNull();
    expect(await storage.loadWorkflow("live")).not.toBeNull();
  });

  it("sweeps on one instance of a cluster only", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new CountingStorage({ clock });
    const leases = new InMemoryLeaderLeases({ clock });
    await seedEnded({ storage, ids: ["a", "b", "c"] });
    clock.advance(2 * HOUR);

    const config = { maxAgeMs: HOUR, intervalMs: 1_000 };
    const one = makeRetention({ storage, clock, config, cluster: { instanceId: "one", leases } });
    const two = makeRetention({ storage, clock, config, cluster: { instanceId: "two", leases } });
    await one.retention.start();
    await flush();
    await two.retention.start();
    await flush();

    expect(await two.retention.purge()).toEqual({ deleted: 0, skipped: "not_leader" });
    for (let i = 0; i < 3; i++) {
      clock.advance(1_000);
      await flush();
    }
    expect(one.reports.reduce((n, r) => n + r.deleted, 0)).toBe(3);
    expect(two.reports).toEqual([]);

    // The leader stops and releases its lease: the peer takes over.
    await one.retention.stop();
    await seedEnded({ storage, ids: ["d"] });
    clock.advance(2 * HOUR);
    await flush();
    expect(await two.retention.purge()).toEqual({ deleted: 1 });
    await two.retention.stop();
  });

  it("runs as a ZoryaServer background service", async () => {
    const clock = FakeWallClock.create(0);
    const storage = new CountingStorage({ clock });
    await seedEnded({ storage, ids: ["x"] });
    clock.advance(2 * HOUR);
    const server = new ZoryaServer({
      workflows: new QueuedWorkflows({
        storage,
        clock,
        maintenance: false,
        wakeups: { sleepScanIntervalMs: 0, signalScanIntervalMs: 0 },
      }),
      insecure: true,
      logger: quietLogger,
      clock,
      retention: { maxAgeMs: HOUR, intervalMs: 1_000 },
    });
    expect(server.retention).toBeDefined();

    server.start();
    await server.ready();
    expect(server.serviceStatuses().map((s) => s.name)).toContain("retention");
    await flush();
    expect(storage.purgeCalls).toBe(0);
    clock.advance(1_000);
    await flush();
    expect(server.retention?.lastSweep).toEqual({ deleted: 1 });
    expect(await storage.loadWorkflow("x")).toBeNull();
    await server.stop();
  });

  it("refuses an invalid config at server construction", () => {
    expect(
      () =>
        new ZoryaServer({
          workflows: new QueuedWorkflows({ storage: new InMemoryWorkflowStorage() }),
          insecure: true,
          logger: quietLogger,
          retention: { maxAgeMs: -1 },
        }),
    ).toThrow("maxAgeMs");
  });
});
