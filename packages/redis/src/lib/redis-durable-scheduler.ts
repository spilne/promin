// ---------------------------------------------------------------------------
// Redis-backed durable scheduler — convenience facade.
//
// Wraps the generic DurableScheduler shell from @promin/workflow with a
// RedisSchedulerStorage adapter. Mirror of @promin/postgres's facade so
// callers can swap Postgres ↔ Redis with a one-line constructor change.
// ---------------------------------------------------------------------------

import {
  DurableScheduler as GenericDurableScheduler,
  type DurableSchedulerConfig as GenericConfig,
} from "@promin/workflow";
import {
  RedisSchedulerStorage,
  type RedisSchedulerStorageConfig,
} from "./redis-scheduler-storage.ts";
import type { RedisClient } from "./redis-client.ts";

export type { DurableScheduleConfig } from "@promin/workflow";

export interface RedisDurableSchedulerConfig {
  redis: RedisClient;
  /** Key prefix for all scheduler keys. Default: "sched". */
  prefix?: string;
  /** Instance ID for leader election. Default: random UUID. */
  instanceId?: string;
  /** Poll interval in ms. Default: 1000. */
  pollIntervalMs?: number;
  /** Leader-lock TTL in ms. Default: 3 × pollIntervalMs. */
  leaderLockTtlMs?: number;
  /** Scope this scheduler instance to a single namespace. */
  namespace?: string;
  /** Max schedules claimed per poll cycle. Default: 100. */
  batchSize?: number;
}

/**
 * Redis-backed durable scheduler. See @promin/workflow's `DurableScheduler`
 * for the full streaming/cron/rrule/catch-up surface; this class just wires
 * up the Redis storage adapter.
 */
export class RedisDurableScheduler extends GenericDurableScheduler {
  constructor(config: RedisDurableSchedulerConfig) {
    const storageConfig: RedisSchedulerStorageConfig = {
      redis: config.redis,
      prefix: config.prefix,
    };
    const storage = new RedisSchedulerStorage(storageConfig);
    const cfg: GenericConfig = {
      storage,
      instanceId: config.instanceId,
      pollIntervalMs: config.pollIntervalMs,
      leaderLockTtlMs: config.leaderLockTtlMs,
      namespace: config.namespace,
      batchSize: config.batchSize,
    };
    super(cfg);
  }
}

export function createRedisDurableScheduler(
  config: RedisDurableSchedulerConfig,
): RedisDurableScheduler {
  return new RedisDurableScheduler(config);
}

export { RedisSchedulerStorage };
export type { RedisSchedulerStorageConfig } from "./redis-scheduler-storage.ts";
