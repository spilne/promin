// ---------------------------------------------------------------------------
// Postgres-backed durable scheduler — convenience facade.
//
// Wraps the generic DurableScheduler shell from @promin/workflow with a
// PgSchedulerStorage adapter. All cron/rrule/catch-up/jitter/leader logic
// lives in the workflow shell; this file is just the convenience factory
// and the legacy `DurableScheduler` export for backward compat.
// ---------------------------------------------------------------------------

import {
  DurableScheduler as GenericDurableScheduler,
  type DurableSchedulerConfig as GenericConfig,
} from "@promin/workflow";
import { PgSchedulerStorage } from "./pg-scheduler-storage.ts";
import type { DrizzleDb } from "./drizzle-db.ts";

export type { DurableScheduleConfig } from "@promin/workflow";

export interface DurableSchedulerConfig {
  db: DrizzleDb;
  /** Instance ID for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader lock advisory lock ID. Default: hash of "wf-scheduler-leader". */
  leaderLockId?: number;
  /** Scope this scheduler instance to a single namespace. */
  namespace?: string;
  /** Max schedules claimed per poll cycle. Default: 100. */
  batchSize?: number;
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
    const storage = new PgSchedulerStorage({
      db: config.db,
      leaderLockId: config.leaderLockId,
    });
    const cfg: GenericConfig = {
      storage,
      instanceId: config.instanceId,
      pollIntervalMs: config.pollIntervalMs,
      namespace: config.namespace,
      batchSize: config.batchSize,
    };
    super(cfg);
  }
}

/** Convenience factory mirroring `createScheduler()` from @promin/workflow. */
export function createDurableScheduler(config: DurableSchedulerConfig): DurableScheduler {
  return new DurableScheduler(config);
}

// Re-export the storage adapter for callers wiring it into the generic shell directly.
export { PgSchedulerStorage };
