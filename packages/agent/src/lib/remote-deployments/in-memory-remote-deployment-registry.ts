// ---------------------------------------------------------------------------
// InMemoryRemoteDeploymentRegistry — single-process default. Matches the
// `WorkerRegistry`'s shape (Map keyed by id, Date-based heartbeats);
// adapted to deployment-specific fields (endpoint, agents, auth, ttl).
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import {
  DEFAULT_DEPLOYMENT_TTL_MS,
  type RegisterDeploymentInput,
  type RegisteredDeployment,
  type RemoteDeploymentRegistry,
} from "./types.ts";

export interface InMemoryRemoteDeploymentRegistryConfig {
  /** Optional clock override for tests. Defaults to `Date.now()`. */
  readonly now?: () => number;
}

export class InMemoryRemoteDeploymentRegistry implements RemoteDeploymentRegistry {
  private readonly clock: () => number;
  private readonly rows = new Map<string, RegisteredDeployment>();

  constructor(config: InMemoryRemoteDeploymentRegistryConfig = {}) {
    this.clock = config.now ?? (() => Date.now());
  }

  async register(input: RegisterDeploymentInput): Promise<RegisteredDeployment> {
    const now = this.clock();
    const deployment: RegisteredDeployment = {
      deploymentId: randomUUID(),
      endpoint: input.endpoint,
      agents: [...input.agents],
      ...(input.auth !== undefined && { auth: input.auth }),
      registeredAt: now,
      lastHeartbeat: now,
      ttlMs: input.ttlMs ?? DEFAULT_DEPLOYMENT_TTL_MS,
    };
    this.rows.set(deployment.deploymentId, deployment);
    return deployment;
  }

  async heartbeat(deploymentId: string): Promise<RegisteredDeployment | null> {
    const existing = this.rows.get(deploymentId);
    if (!existing) return null;
    const now = this.clock();
    // Caller might be heartbeating a deployment that's already past
    // its TTL but hasn't been swept yet — refresh anyway. The sweep
    // loop is independent; if it has run, the row is gone and we
    // returned null above.
    const updated: RegisteredDeployment = { ...existing, lastHeartbeat: now };
    this.rows.set(deploymentId, updated);
    return updated;
  }

  async unregister(deploymentId: string): Promise<RegisteredDeployment | null> {
    const existing = this.rows.get(deploymentId);
    if (!existing) return null;
    this.rows.delete(deploymentId);
    return existing;
  }

  async get(deploymentId: string): Promise<RegisteredDeployment | null> {
    return this.rows.get(deploymentId) ?? null;
  }

  async list(): Promise<RegisteredDeployment[]> {
    return [...this.rows.values()];
  }

  async expireStale(params: { now: number }): Promise<RegisteredDeployment[]> {
    const expired: RegisteredDeployment[] = [];
    for (const [id, row] of this.rows.entries()) {
      if (row.lastHeartbeat + row.ttlMs <= params.now) {
        expired.push(row);
        this.rows.delete(id);
      }
    }
    return expired;
  }
}
