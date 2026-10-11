// ---------------------------------------------------------------------------
// RunRetention — deletes ended runs (completed, failed, tripwire) once they
// are older than a configured age, so run history does not grow without
// bound.
//
// It is one more leader-elected maintenance loop (`LeaderLoop`): in a
// cluster only the instance holding the `zorya/runs/retention` lease
// sweeps. Each sweep deletes in bounded batches through
// `WorkflowStorage.purgeCompleted`, which removes a run together with its
// steps, signals, run history, journal, step attempts, signal tokens and
// streams. Batches repeat while full, renewing the leadership between them,
// so a backlog drains in one sweep without one long delete.
//
// The age cutoff is relative (`olderThanMs`): the storage computes it with
// the same clock that stamped `completedAt`.
//
// The first sweep runs one interval after start, not at boot, so it never
// competes with startup recovery.
// ---------------------------------------------------------------------------

import type { WallClock, WorkflowStorage } from "@promin/workflow";
import { leaderElectionFor, type ZoryaCluster } from "./cluster.ts";
import { LeaderLoop } from "./run-maintenance.ts";

/** Retention of ended runs (`ZoryaServerConfig.retention`). */
export interface RunRetentionConfig {
  /** Delete completed, failed and tripwired runs that ended more than this long ago (ms). */
  readonly maxAgeMs: number;
  /** Delay between sweeps (ms). Default: one hour. */
  readonly intervalMs?: number;
  /** Most runs one delete batch removes. Default: 500. */
  readonly batchSize?: number;
}

/** What one retention sweep did. */
export interface RunRetentionReport {
  /** Runs deleted. */
  readonly deleted: number;
  /** Set when another instance holds the retention leadership (or this one is stopping). */
  readonly skipped?: "not_leader";
}

export const DEFAULT_RETENTION_INTERVAL_MS = 60 * 60 * 1000;
export const DEFAULT_RETENTION_BATCH_SIZE = 500;

/** Throw when a retention config cannot work. */
export function validateRunRetentionConfig(config: RunRetentionConfig): void {
  if (!Number.isFinite(config.maxAgeMs) || config.maxAgeMs <= 0) {
    throw new Error("ZoryaServer retention.maxAgeMs must be a positive finite number");
  }
  if (
    config.intervalMs !== undefined &&
    (!Number.isFinite(config.intervalMs) || config.intervalMs <= 0)
  ) {
    throw new Error("ZoryaServer retention.intervalMs must be a positive finite number");
  }
  if (
    config.batchSize !== undefined &&
    (!Number.isInteger(config.batchSize) || config.batchSize <= 0)
  ) {
    throw new Error("ZoryaServer retention.batchSize must be a positive integer");
  }
}

export class RunRetention {
  readonly name = "retention";
  private readonly storage: WorkflowStorage;
  private readonly maxAgeMs: number;
  private readonly batchSize: number;
  private readonly loop: LeaderLoop;
  private readonly onSwept: (report: RunRetentionReport) => void;
  /** Skips the loop's first iteration, which runs at start. */
  private armed = false;
  /** The last sweep's outcome (tests, diagnostics). */
  lastSweep?: RunRetentionReport;

  constructor(params: {
    readonly storage: WorkflowStorage;
    readonly config: RunRetentionConfig;
    readonly cluster: ZoryaCluster | undefined;
    readonly clock: WallClock;
    readonly onSwept: (report: RunRetentionReport) => void;
    readonly onError: (error: unknown) => void;
  }) {
    validateRunRetentionConfig(params.config);
    this.storage = params.storage;
    this.maxAgeMs = params.config.maxAgeMs;
    this.batchSize = params.config.batchSize ?? DEFAULT_RETENTION_BATCH_SIZE;
    this.onSwept = params.onSwept;
    const intervalMs = params.config.intervalMs ?? DEFAULT_RETENTION_INTERVAL_MS;
    this.loop = new LeaderLoop({
      name: "zorya-run-retention",
      intervalMs,
      clock: params.clock,
      leaderElection: leaderElectionFor({
        cluster: params.cluster,
        key: "runs/retention",
        intervalMs,
      }),
      tick: async () => {
        if (!this.armed) {
          this.armed = true;
          return;
        }
        const report = await this.sweep();
        this.lastSweep = report;
        this.onSwept(report);
      },
      onError: params.onError,
    });
  }

  async start(): Promise<void> {
    this.loop.start();
  }

  async stop(): Promise<void> {
    await this.loop.stop();
    this.armed = false;
  }

  /**
   * Sweep now (operators, tests). Takes the retention leadership first;
   * when another instance holds it, returns `{ skipped: "not_leader" }`
   * without deleting anything.
   */
  async purge(): Promise<RunRetentionReport> {
    if (!(await this.loop.acquire())) return { deleted: 0, skipped: "not_leader" };
    return this.sweep();
  }

  /** One sweep, under a leadership already held. */
  private async sweep(): Promise<RunRetentionReport> {
    let deleted = 0;
    for (;;) {
      const batch = await this.storage.purgeCompleted({
        olderThanMs: this.maxAgeMs,
        limit: this.batchSize,
      });
      deleted += batch;
      if (batch < this.batchSize) break;
      if (!(await this.loop.stillLeading())) break;
    }
    return { deleted };
  }
}
