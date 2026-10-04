// ---------------------------------------------------------------------------
// Postgres-backed durable scheduler — convenience facade.
//
// Wraps the generic DurableScheduler shell from @promin/workflow with a
// PgSchedulerStorage adapter. All cron/rrule/catch-up/jitter/leader logic
// lives in the workflow shell; this file is just the convenience factory
// and the legacy `DurableScheduler` export for backward compat.
// ---------------------------------------------------------------------------

import { type WallClock } from "@promin/workflow";
import {
  DurableScheduler as GenericDurableScheduler,
  type DurableSchedulerConfig as GenericConfig,
} from "@promin/workflow/scheduler";
import { PgSchedulerStorage } from "./pg-scheduler-storage.ts";
import type { DrizzleDb } from "@spilne/perfect-postgres";

export type { DurableScheduleConfig } from "@promin/workflow/scheduler";

export interface DurableSchedulerConfig {
  db: DrizzleDb;
  /** Instance ID for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lease TTL (server clock). Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
  /** Scope this scheduler instance to a single namespace. */
  namespace?: string;
  /** Hash partitioning; each partition elects its own leader. */
  partition?: GenericConfig["partition"];
  /** Max schedules claimed per poll cycle. Default: 100. */
  batchSize?: number;
  /**
   * Time source for both the scheduler (due ticks, next runs, poll cadence)
   * and its storage. Default: `SystemWallClock`.
   */
  clock?: WallClock;
  /**
   * Called when a poll or commit fails, or a stored schedule can't be
   * evaluated. The tick stream keeps running. Default: `console.error`.
   */
  onError?: GenericConfig["onError"];
  /** Upper bound for the backoff between failed polls. Default: 30 000. */
  maxErrorBackoffMs?: number;
}

/**
 * Postgres-backed durable scheduler. See @promin/workflow's DurableScheduler
 * for the full streaming/cron/rrule/catch-up surface; this class only adds
 * the Postgres storage adapter.
 */
export class DurableScheduler extends GenericDurableScheduler {
  /** Drizzle schema export — include in your migration pipeline. */
  static readonly schema = PgSchedulerStorage.schema;

  constructor(config: DurableSchedulerConfig) {
    const storage = new PgSchedulerStorage({ db: config.db, clock: config.clock });
    const cfg: GenericConfig = {
      storage,
      instanceId: config.instanceId,
      pollIntervalMs: config.pollIntervalMs,
      leaderLockTtlMs: config.leaderLockTtlMs,
      namespace: config.namespace,
      partition: config.partition,
      batchSize: config.batchSize,
      clock: config.clock,
      onError: config.onError,
      maxErrorBackoffMs: config.maxErrorBackoffMs,
    };
    super(cfg);
  }
}

/** Convenience factory for a Postgres-backed `DurableScheduler`. */
export function createDurableScheduler(config: DurableSchedulerConfig): DurableScheduler {
  return new DurableScheduler(config);
}

// Re-export the storage adapter for callers wiring it into the generic shell directly.
export { PgSchedulerStorage };
