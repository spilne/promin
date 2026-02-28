// ---------------------------------------------------------------------------
// PostgresWorkflowStorage configuration
// ---------------------------------------------------------------------------

export interface PostgresStorageConfig {
  /** Drizzle database instance. User provides their own connection. */
  db: any; // DrizzleClient — typed loosely to avoid forcing specific pg driver

  /** Table name prefix. Default: "wf_". Allows multiple workflow engines in one DB. */
  tablePrefix?: string;

  /** Instance ID for lock ownership tracking. Default: random UUID. */
  instanceId?: string;

  /** Whether to use pg_advisory_lock (true) or row-based locks (false). Default: true. */
  useAdvisoryLocks?: boolean;

  /** Default lock duration in ms. Default: 30_000. */
  defaultLockDurationMs?: number;

  /** Whether to auto-seed lookup tables on first use. Default: true. */
  autoSeedLookups?: boolean;

  /** Log function for debugging. Default: no-op. */
  logger?: (message: string, meta?: Record<string, unknown>) => void;
}

export const DEFAULT_CONFIG = {
  tablePrefix: "wf_",
  useAdvisoryLocks: true,
  defaultLockDurationMs: 30_000,
  autoSeedLookups: true,
  logger: () => {},
} as const;

export function resolveConfig(config: PostgresStorageConfig): Required<PostgresStorageConfig> {
  return {
    db: config.db,
    tablePrefix: config.tablePrefix ?? DEFAULT_CONFIG.tablePrefix,
    instanceId: config.instanceId ?? crypto.randomUUID(),
    useAdvisoryLocks: config.useAdvisoryLocks ?? DEFAULT_CONFIG.useAdvisoryLocks,
    defaultLockDurationMs: config.defaultLockDurationMs ?? DEFAULT_CONFIG.defaultLockDurationMs,
    autoSeedLookups: config.autoSeedLookups ?? DEFAULT_CONFIG.autoSeedLookups,
    logger: config.logger ?? DEFAULT_CONFIG.logger,
  };
}
