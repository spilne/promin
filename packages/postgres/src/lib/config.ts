// ---------------------------------------------------------------------------
// PostgresWorkflowStorage configuration
// ---------------------------------------------------------------------------

import type { DrizzleDb } from "@spilne/perfect-postgres";
import { SystemWallClock, type WallClock } from "@promin/workflow";

export interface PostgresStorageConfig {
  /** Drizzle database instance. User provides their own connection. */
  db: DrizzleDb;

  /** Default namespace for workflow isolation. Null means unscoped. Default: null. */
  namespace?: string | null;

  /** Instance ID for lock ownership tracking. Default: random UUID. */
  instanceId?: string;

  /**
   * Lock implementation. Default `false`: row locks in `wf_workflow_locks`
   * with a server-side lease expiry and a monotonic fence token — they
   * exclude every other caller (any pool connection, any process) and let
   * fenced writes reject a stale holder.
   *
   * @deprecated `true` selects session-scoped `pg_try_advisory_lock`, which
   * is unsafe through a connection pool: the lock is re-entrant per
   * connection (two callers on the same idle connection both "win"),
   * `releaseLock` unlocks on whichever connection the pool hands out (so
   * locks leak until that connection closes), and no fence token is issued.
   * Only usable when the storage owns a single dedicated connection.
   */
  useAdvisoryLocks?: boolean;

  /** Default lock duration in ms. Default: 30_000. */
  defaultLockDurationMs?: number;

  /** Whether to auto-seed lookup tables on first use. Default: true. */
  autoSeedLookups?: boolean;

  /** Log function for debugging. Default: no-op. */
  logger?: (message: string, meta?: Record<string, unknown>) => void;

  /** Record step attempt history for audit trail. Default: false. */
  recordAttempts?: boolean;

  /**
   * Time source for client-side timestamps (lock expiry, purge cutoffs,
   * completion/failure/update stamps the client generates before handing
   * to Postgres). Default: `SystemWallClock`. Pass a `FakeWallClock` for
   * deterministic tests.
   */
  clock?: WallClock;
}

export const DEFAULT_CONFIG = {
  namespace: null,
  useAdvisoryLocks: false,
  defaultLockDurationMs: 30_000,
  autoSeedLookups: true,
  logger: () => {},
  recordAttempts: false,
} as const;

export function resolveConfig(config: PostgresStorageConfig): Required<PostgresStorageConfig> {
  return {
    db: config.db,
    namespace: config.namespace ?? DEFAULT_CONFIG.namespace,
    instanceId: config.instanceId ?? crypto.randomUUID(),
    useAdvisoryLocks: config.useAdvisoryLocks ?? DEFAULT_CONFIG.useAdvisoryLocks,
    defaultLockDurationMs: config.defaultLockDurationMs ?? DEFAULT_CONFIG.defaultLockDurationMs,
    autoSeedLookups: config.autoSeedLookups ?? DEFAULT_CONFIG.autoSeedLookups,
    logger: config.logger ?? DEFAULT_CONFIG.logger,
    recordAttempts: config.recordAttempts ?? DEFAULT_CONFIG.recordAttempts,
    clock: config.clock ?? SystemWallClock,
  };
}
