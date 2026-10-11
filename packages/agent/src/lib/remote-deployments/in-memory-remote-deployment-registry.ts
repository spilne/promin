// ---------------------------------------------------------------------------
// InMemoryRemoteDeploymentRegistry — single-process default. Matches the
// `WorkerRegistry`'s shape (Map keyed by id, Date-based heartbeats);
// adapted to deployment-specific fields (endpoint, agents, auth, ttl).
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  DEFAULT_DEPLOYMENT_TTL_MS,
  type RegisterDeploymentInput,
  type RegisteredDeployment,
  type RemoteDeploymentRegistry,
} from "./types.ts";

export interface InMemoryRemoteDeploymentRegistryConfig {
  /** Time source for heartbeat stamps and expiry. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryRemoteDeploymentRegistry implements RemoteDeploymentRegistry {
  private readonly clock: WallClock;
  private readonly rows = new Map<string, RegisteredDeployment>();

  constructor(config: InMemoryRemoteDeploymentRegistryConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  async register(input: RegisterDeploymentInput): Promise<RegisteredDeployment> {
    const now = this.clock.currentTimeMs();
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
    const now = this.clock.currentTimeMs();
    // An expired registration is dead even before the sweep removes it:
    // its recipes are about to be deleted, so reviving it would leave a
    // live deployment with no recipes. The caller re-registers instead.
    if (existing.lastHeartbeat + existing.ttlMs <= now) return null;
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

  async expireStale(): Promise<RegisteredDeployment[]> {
    const now = this.clock.currentTimeMs();
    const expired: RegisteredDeployment[] = [];
    for (const [id, row] of this.rows.entries()) {
      if (row.lastHeartbeat + row.ttlMs <= now) {
        expired.push(row);
        this.rows.delete(id);
      }
    }
    return expired;
  }
}
