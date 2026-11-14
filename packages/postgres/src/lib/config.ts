// ---------------------------------------------------------------------------
// PostgresWorkflowStorage configuration
// ---------------------------------------------------------------------------

import type { DrizzleDb } from "@spilne/perfect-postgres";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { NoWakeups, type Wakeups } from "@promin/workflow/distributed";

export interface PostgresStorageConfig {
  /** Drizzle database instance. User provides their own connection. */
  db: DrizzleDb;

  /** Default namespace for workflow isolation. Null means unscoped. Default: null. */
  namespace?: string | null;

  /** Instance ID for lock ownership tracking. Default: random UUID. */
  instanceId?: string;

  /** Default lock duration in ms. Default: 30_000. */
  defaultLockDurationMs?: number;

  /** Whether to auto-seed lookup tables on first use. Default: true. */
  autoSeedLookups?: boolean;

  /** Log function for debugging. Default: no-op. */
  logger?: (message: string, meta?: Record<string, unknown>) => void;

  /**
   * Time source for client-side timestamps (lock expiry, purge cutoffs,
   * completion/failure/update stamps the client generates before handing
   * to Postgres). Default: `SystemWallClock`. Pass a `FakeWallClock` for
   * deterministic tests.
   */
  clock?: WallClock;

  /**
   * Told (topic `"signals"`, keyed by workflow id) when a signal is
   * delivered, so a signal scanner can scan now instead of at its next
   * interval. A latency hint sent after the write commits. Default: none.
   */
  wakeups?: Wakeups;
}

export const DEFAULT_CONFIG = {
  namespace: null,
  defaultLockDurationMs: 30_000,
  autoSeedLookups: true,
  logger: () => {},
} as const;

export function resolveConfig(config: PostgresStorageConfig): Required<PostgresStorageConfig> {
  return {
    db: config.db,
    namespace: config.namespace ?? DEFAULT_CONFIG.namespace,
    instanceId: config.instanceId ?? crypto.randomUUID(),
    defaultLockDurationMs: config.defaultLockDurationMs ?? DEFAULT_CONFIG.defaultLockDurationMs,
    autoSeedLookups: config.autoSeedLookups ?? DEFAULT_CONFIG.autoSeedLookups,
    logger: config.logger ?? DEFAULT_CONFIG.logger,
    clock: config.clock ?? SystemWallClock,
    wakeups: config.wakeups ?? NoWakeups,
  };
}
