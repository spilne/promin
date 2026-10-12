// ---------------------------------------------------------------------------
// `PostgresRemoteDeploymentRegistry` — `RemoteDeploymentRegistry` over the
// `remote_deployment` table, shared by every replica. Several processes
// must agree on which registrations expired, so heartbeat stamps and the
// expiry comparison both use the database clock (`clock_timestamp()`),
// never an app-side time.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import {
  DEFAULT_DEPLOYMENT_TTL_MS,
  type RegisterDeploymentInput,
  type RegisteredDeployment,
  type RemoteDeploymentAuth,
  type RemoteDeploymentRegistry,
} from "@promin/agent";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { execRaw } from "../exec-raw.ts";

export interface PostgresRemoteDeploymentRegistryConfig {
  readonly db: DrizzleDb;
}

interface Row {
  deployment_id: string;
  endpoint: string;
  agents: string[];
  auth: RemoteDeploymentAuth | null;
  registered_at: string | number;
  last_heartbeat: string | number;
  ttl_ms: string | number;
}

/** Epoch milliseconds on the database clock. */
const DB_NOW_MS = sql`(extract(epoch from clock_timestamp()) * 1000)::bigint`;

export class PostgresRemoteDeploymentRegistry implements RemoteDeploymentRegistry {
  private readonly db: DrizzleDb;

  constructor(config: PostgresRemoteDeploymentRegistryConfig) {
    this.db = config.db;
  }

  async register(input: RegisterDeploymentInput): Promise<RegisteredDeployment> {
    const [row] = (await execRaw(
      this.db,
      sql`INSERT INTO remote_deployment
            (deployment_id, endpoint, agents, auth, registered_at, last_heartbeat, ttl_ms)
          SELECT ${randomUUID()}, ${input.endpoint}, ${JSON.stringify([...input.agents])}::jsonb,
                 ${input.auth !== undefined ? JSON.stringify(input.auth) : null}::jsonb,
                 now_ms, now_ms, ${input.ttlMs ?? DEFAULT_DEPLOYMENT_TTL_MS}
          FROM (SELECT ${DB_NOW_MS} AS now_ms) t
          RETURNING *`,
    )) as Row[];
    return decode(row!);
  }

  async heartbeat(deploymentId: string): Promise<RegisteredDeployment | null> {
    // An expired registration is never revived, even before the sweep.
    const [row] = (await execRaw(
      this.db,
      sql`UPDATE remote_deployment SET last_heartbeat = ${DB_NOW_MS}
          WHERE deployment_id = ${deploymentId} AND last_heartbeat + ttl_ms > ${DB_NOW_MS}
          RETURNING *`,
    )) as Row[];
    return row ? decode(row) : null;
  }

  async unregister(deploymentId: string): Promise<RegisteredDeployment | null> {
    const [row] = (await execRaw(
      this.db,
      sql`DELETE FROM remote_deployment WHERE deployment_id = ${deploymentId} RETURNING *`,
    )) as Row[];
    return row ? decode(row) : null;
  }

  async get(deploymentId: string): Promise<RegisteredDeployment | null> {
    const [row] = (await execRaw(
      this.db,
      sql`SELECT * FROM remote_deployment WHERE deployment_id = ${deploymentId}`,
    )) as Row[];
    return row ? decode(row) : null;
  }

  async list(): Promise<RegisteredDeployment[]> {
    const rows = (await execRaw(
      this.db,
      sql`SELECT * FROM remote_deployment ORDER BY registered_at, deployment_id`,
    )) as Row[];
    return rows.map(decode);
  }

  async expireStale(): Promise<RegisteredDeployment[]> {
    // One statement: two sweepers on different replicas each get a disjoint
    // set of rows back, so a deployment's recipes are cleaned up once.
    const rows = (await execRaw(
      this.db,
      sql`DELETE FROM remote_deployment WHERE last_heartbeat + ttl_ms <= ${DB_NOW_MS}
          RETURNING *`,
    )) as Row[];
    return rows.map(decode);
  }
}

function decode(row: Row): RegisteredDeployment {
  return {
    deploymentId: row.deployment_id,
    endpoint: row.endpoint,
    agents: [...row.agents],
    ...(row.auth !== null && { auth: row.auth }),
    registeredAt: Number(row.registered_at),
    lastHeartbeat: Number(row.last_heartbeat),
    ttlMs: Number(row.ttl_ms),
  };
}
