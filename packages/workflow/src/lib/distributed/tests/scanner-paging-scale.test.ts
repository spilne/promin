// ---------------------------------------------------------------------------
// Scanners and recovery over a backlog of 100 000 runs: every storage read is
// one bounded page after a keyset cursor, no run is handed out twice or
// skipped, and one sweep ends after its budget instead of draining everything.
// The storage is a virtual id space (nothing is materialized), so what the
// test measures is the scanner's own reads and memory, not a backend's.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { createSleepScanner } from "../sleep-scanner.ts";
import { createSignalScanner } from "../signal-scanner.ts";
import { recoverWorkflows, RecoveryStrategy } from "../../durable/runner/recovery.ts";
import { FakeWallClock } from "../../shared/wall-clock.ts";
import type { LeaderElection } from "../leader-election.ts";
import type {
  ListDueTimersParams,
  ListOrphanedRunsParams,
  ListSignalWakeupsParams,
  OrphanedRun,
  WorkflowStorage,
  WorkflowWakeup,
} from "../../durable/workflow-storage.ts";
import type { WorkflowVersionRegistry } from "../../durable/workflow-version-registry.ts";
import type { Workflow } from "../../durable/durable-pipeline.ts";

const TOTAL = 100_000;
const PAGE = 128;
const BUDGET = 1_000;

const idOf = (n: number): string => `run-${String(n).padStart(6, "0")}`;
const indexOf = (id: string): number => Number(id.slice(4));

/** The rows `[after + 1, TOTAL)` of the virtual id space, at most `limit`. */
function virtualPage<T>(params: {
  limit: number;
  afterWorkflowId?: string;
  row: (id: string) => T;
  stats: { calls: number; largestPage: number; largestLimit: number };
}): T[] {
  const start = params.afterWorkflowId === undefined ? 0 : indexOf(params.afterWorkflowId) + 1;
  const end = Math.min(TOTAL, start + params.limit);
  const rows: T[] = [];
  for (let n = start; n < end; n++) rows.push(params.row(idOf(n)));
  params.stats.calls++;
  params.stats.largestPage = Math.max(params.stats.largestPage, rows.length);
  params.stats.largestLimit = Math.max(params.stats.largestLimit, params.limit);
  return rows;
}

const newStats = () => ({ calls: 0, largestPage: 0, largestLimit: 0 });

const wakeupOf = (id: string): WorkflowWakeup => ({
  workflowId: id,
  workflowName: "scale",
  input: null,
  stepName: "nap",
  reason: "sleep",
});

/** A leader election that records how many runs had been handed out at each acquire. */
function recordingLeader(handed: () => number): LeaderElection & { acquiredAt: number[] } {
  const acquiredAt: number[] = [];
  return {
    acquiredAt,
    async tryAcquire() {
      acquiredAt.push(handed());
      return true;
    },
    async release() {},
  };
}

async function until(done: () => boolean): Promise<void> {
  for (let i = 0; i < 200_000 && !done(); i++) await new Promise<void>((r) => setImmediate(r));
  expect(done()).toBe(true);
}

describe("scanners over 100 000 due runs", () => {
  it("the sleep scanner pages with a keyset cursor and ends each sweep at its budget", async () => {
    const stats = newStats();
    const handed: string[] = [];
    const storage = {
      listDueTimers: async (p: ListDueTimersParams) => virtualPage({ ...p, row: wakeupOf, stats }),
    } as unknown as WorkflowStorage;
    const leader = recordingLeader(() => handed.length);
    const scanner = createSleepScanner({
      storage,
      clock: FakeWallClock.create("2026-01-01T00:00:00Z"),
      wake: async (run) => {
        handed.push(run.workflowId);
      },
      pageSize: PAGE,
      maxRunsPerSweep: BUDGET,
      leaderElection: leader,
    });
    const running = scanner.start();
    await until(() => handed.length === TOTAL);
    await scanner.stop();
    await running;

    // Every run exactly once, in id order.
    expect(handed.length).toBe(TOTAL);
    expect(handed.every((id, n) => id === idOf(n))).toBe(true);
    // Reads were pages of at most PAGE rows after a cursor.
    expect(stats.largestPage).toBeLessThanOrEqual(PAGE);
    expect(stats.largestLimit).toBe(PAGE);
    expect(stats.calls).toBeLessThanOrEqual(Math.ceil(TOTAL / PAGE) + TOTAL / BUDGET + 2);
    // Each sweep ended at its budget: leadership was re-checked at every
    // BUDGET boundary, a new sweep continuing after the last run handed out.
    for (let boundary = BUDGET; boundary < TOTAL; boundary += BUDGET) {
      expect(leader.acquiredAt).toContain(boundary);
    }
  }, 60_000);

  it("the signal scanner honours the same budget", async () => {
    const stats = newStats();
    const handed: string[] = [];
    const storage = {
      listSignalWakeups: async (p: ListSignalWakeupsParams) =>
        virtualPage({ ...p, row: wakeupOf, stats }),
    } as unknown as WorkflowStorage;
    const leader = recordingLeader(() => handed.length);
    const scanner = createSignalScanner({
      storage,
      clock: FakeWallClock.create("2026-01-01T00:00:00Z"),
      wake: async (run) => {
        handed.push(run.workflowId);
      },
      pageSize: PAGE,
      maxRunsPerSweep: BUDGET,
      leaderElection: leader,
    });
    const running = scanner.start();
    await until(() => handed.length === TOTAL);
    await scanner.stop();
    await running;

    expect(handed.every((id, n) => id === idOf(n))).toBe(true);
    expect(stats.largestPage).toBeLessThanOrEqual(PAGE);
    expect(leader.acquiredAt).toContain(BUDGET);
  }, 60_000);
});

describe("resumeRecent over 100 000 orphaned runs", () => {
  it("reads one page at a time, as the pool drains", async () => {
    const stats = newStats();
    let listed = 0;
    let started = 0;
    let backlog = 0;
    let largestBacklog = 0;
    let inFlight = 0;
    let peakInFlight = 0;
    const concurrent = 8;
    const storage = {
      listOrphanedRuns: async (p: ListOrphanedRunsParams) => {
        const rows = virtualPage<OrphanedRun>({
          limit: p.limit,
          afterWorkflowId: p.afterWorkflowId,
          stats,
          row: (id) => ({
            workflowId: id,
            workflowName: "scale",
            status: "running",
            input: null,
          }),
        });
        listed += rows.length;
        backlog = listed - started;
        largestBacklog = Math.max(largestBacklog, backlog);
        return rows;
      },
    } as unknown as WorkflowStorage;
    const definition = {} as Workflow<unknown, unknown>;
    const registry = { resolve: async () => definition } as unknown as WorkflowVersionRegistry;

    const result = await recoverWorkflows({
      strategy: RecoveryStrategy.builder().resumeRecent({ concurrent }).build(),
      storage,
      registry,
      clock: FakeWallClock.create("2026-01-01T00:00:00Z"),
      resume: async () => {
        started++;
        inFlight++;
        peakInFlight = Math.max(peakInFlight, inFlight);
        await Promise.resolve();
        inFlight--;
      },
    });
    await result.settled;

    expect(result.resumed).toBe(TOTAL);
    expect(started).toBe(TOTAL);
    expect(peakInFlight).toBeLessThanOrEqual(concurrent);
    expect(stats.largestPage).toBeLessThanOrEqual(200);
    // The listing never ran far ahead of the resumes: at most a couple of
    // pages were waiting, not the 100 000-run backlog.
    expect(largestBacklog).toBeLessThanOrEqual(2 * 200 + concurrent);
  }, 60_000);
});
