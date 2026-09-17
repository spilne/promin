// ---------------------------------------------------------------------------
// RemoteDeploymentRegistry — tracks live remote Zorya deployments that
// have self-registered to expose RemoteAgentBackend recipes on this
// host. Mirrors the WorkerRegistry pattern (in @promin/workflow):
// register on startup, heartbeat at interval, expire on timeout.
//
// The registry only tracks the deployment metadata. The HTTP route
// layer handles AgentRegistry side-effects (upsert RemoteAgentBackend
// recipes on register; delete them on unregister / expiry).
//
// Implementations:
//   - InMemoryRemoteDeploymentRegistry — single-process default
//   - (future) Sqlite / Postgres for persistence + multi-replica
//
// All implementations must pass `remoteDeploymentRegistryTestSuite`.
// ---------------------------------------------------------------------------

/**
 * Bearer-token auth that the local server should send when calling
 * back to the remote deployment. Matches RemoteAgentBackend.auth.
 */
export interface RemoteDeploymentAuth {
  readonly kind: "bearer";
  readonly token: string;
}

export interface RegisterDeploymentInput {
  /** Base URL the remote deployment serves agent traffic on. */
  readonly endpoint: string;
  /**
   * Agent ids the remote exposes. The route layer creates a
   * `RemoteAgentBackend` recipe for each, pointing at this endpoint.
   */
  readonly agents: ReadonlyArray<string>;
  readonly auth?: RemoteDeploymentAuth;
  /**
   * TTL in milliseconds. The remote must call heartbeat() within this
   * window or the registration is treated as expired. Default: 60_000.
   */
  readonly ttlMs?: number;
}

export interface RegisteredDeployment {
  /** Server-assigned. Stable across the lifetime of one registration. */
  readonly deploymentId: string;
  readonly endpoint: string;
  readonly agents: ReadonlyArray<string>;
  readonly auth?: RemoteDeploymentAuth;
  /** Millisecond unix epoch — first registration time. */
  readonly registeredAt: number;
  /** Millisecond unix epoch — last heartbeat (or registeredAt if none). */
  readonly lastHeartbeat: number;
  readonly ttlMs: number;
}

export interface RemoteDeploymentRegistry {
  /**
   * Create a fresh registration. Returns the assigned deploymentId.
   * Each call mints a NEW id, even when the (endpoint, agents) match
   * an existing registration — callers that want upsert-by-endpoint
   * should look up the existing registration first.
   */
  register(input: RegisterDeploymentInput): Promise<RegisteredDeployment>;

  /**
   * Extend the TTL for `deploymentId`. Returns the updated row, or
   * null when the id is unknown (e.g. expired and swept by the host's
   * sweep loop). A null return signals the caller should re-register.
   */
  heartbeat(deploymentId: string): Promise<RegisteredDeployment | null>;

  /**
   * Graceful removal. Returns the removed row, or null when unknown.
   */
  unregister(deploymentId: string): Promise<RegisteredDeployment | null>;

  /** Look up by id. */
  get(deploymentId: string): Promise<RegisteredDeployment | null>;

  /** All registrations (live + expired but not yet swept). */
  list(): Promise<RegisteredDeployment[]>;

  /**
   * Sweep step — remove every registration whose
   * `lastHeartbeat + ttlMs <= now`. Returns the removed rows so the
   * route layer can clean up the corresponding RemoteAgentBackend
   * recipes. Idempotent.
   */
  expireStale(params: { readonly now: number }): Promise<RegisteredDeployment[]>;
}

/** Default TTL when register() doesn't specify. 60 seconds, same shape as WorkerRegistry. */
export const DEFAULT_DEPLOYMENT_TTL_MS = 60_000;
