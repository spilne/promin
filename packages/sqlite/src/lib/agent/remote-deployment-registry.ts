// ---------------------------------------------------------------------------
// `SqliteRemoteDeploymentRegistry` — `RemoteDeploymentRegistry` over SQLite,
// so registrations survive a restart of a single-host Zorya. Heartbeat
// stamps and expiry use the injected `WallClock` (one host, one clock).
//
//   promin_remote_deployment (deployment_id PK, endpoint, agents JSON,
//     auth JSON, registered_at, last_heartbeat, ttl_ms)
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import {
  DEFAULT_DEPLOYMENT_TTL_MS,
  type RegisterDeploymentInput,
  type RegisteredDeployment,
  type RemoteDeploymentAuth,
  type RemoteDeploymentRegistry,
} from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { SqliteDatabase } from "../sqlite-database.ts";

const TABLE = "promin_remote_deployment";

export interface SqliteRemoteDeploymentRegistryConfig {
  readonly db: SqliteDatabase;
  /** Time source for heartbeat stamps and expiry. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface Row {
  deployment_id: string;
  endpoint: string;
  agents: string;
  auth: string | null;
  registered_at: number;
  last_heartbeat: number;
  ttl_ms: number;
}

export class SqliteRemoteDeploymentRegistry implements RemoteDeploymentRegistry {
  private readonly db: SqliteDatabase;
  private readonly clock: WallClock;

  constructor(config: SqliteRemoteDeploymentRegistryConfig) {
    this.db = config.db;
    this.clock = config.clock ?? SystemWallClock;
    this.db.run(`
      CREATE TABLE IF NOT EXISTS ${TABLE} (
        deployment_id  TEXT PRIMARY KEY,
        endpoint       TEXT NOT NULL,
        agents         TEXT NOT NULL,
        auth           TEXT,
        registered_at  INTEGER NOT NULL,
        last_heartbeat INTEGER NOT NULL,
        ttl_ms         INTEGER NOT NULL
      )
    `);
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
    this.db
      .query(
        `INSERT INTO ${TABLE}
           (deployment_id, endpoint, agents, auth, registered_at, last_heartbeat, ttl_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        deployment.deploymentId,
        deployment.endpoint,
        JSON.stringify(deployment.agents),
        deployment.auth !== undefined ? JSON.stringify(deployment.auth) : null,
        now,
        now,
        deployment.ttlMs,
      );
    return deployment;
  }

  async heartbeat(deploymentId: string): Promise<RegisteredDeployment | null> {
    const now = this.clock.currentTimeMs();
    return this.db.transaction((): RegisteredDeployment | null => {
      // An expired registration is never revived, even before the sweep.
      this.db
        .query(
          `UPDATE ${TABLE} SET last_heartbeat = ?
           WHERE deployment_id = ? AND last_heartbeat + ttl_ms > ?`,
        )
        .run(now, deploymentId, now);
      const row = this.row(deploymentId);
      return row && row.last_heartbeat === now ? decode(row) : null;
    })();
  }

  async unregister(deploymentId: string): Promise<RegisteredDeployment | null> {
    return this.db.transaction((): RegisteredDeployment | null => {
      const row = this.row(deploymentId);
      if (!row) return null;
      this.db.query(`DELETE FROM ${TABLE} WHERE deployment_id = ?`).run(deploymentId);
      return decode(row);
    })();
  }

  async get(deploymentId: string): Promise<RegisteredDeployment | null> {
    const row = this.row(deploymentId);
    return row ? decode(row) : null;
  }

  async list(): Promise<RegisteredDeployment[]> {
    return this.db
      .query<Row>(`SELECT * FROM ${TABLE} ORDER BY registered_at, deployment_id`)
      .all()
      .map(decode);
  }

  async expireStale(): Promise<RegisteredDeployment[]> {
    const now = this.clock.currentTimeMs();
    return this.db.transaction((): RegisteredDeployment[] => {
      const rows = this.db
        .query<Row>(`SELECT * FROM ${TABLE} WHERE last_heartbeat + ttl_ms <= ?`)
        .all(now);
      for (const row of rows) {
        this.db.query(`DELETE FROM ${TABLE} WHERE deployment_id = ?`).run(row.deployment_id);
      }
      return rows.map(decode);
    })();
  }

  private row(deploymentId: string): Row | null {
    return this.db.query<Row>(`SELECT * FROM ${TABLE} WHERE deployment_id = ?`).get(deploymentId);
  }
}

function decode(row: Row): RegisteredDeployment {
  return {
    deploymentId: row.deployment_id,
    endpoint: row.endpoint,
    agents: JSON.parse(row.agents) as string[],
    ...(row.auth !== null && { auth: JSON.parse(row.auth) as RemoteDeploymentAuth }),
    registeredAt: row.registered_at,
    lastHeartbeat: row.last_heartbeat,
    ttlMs: row.ttl_ms,
  };
}
