// ---------------------------------------------------------------------------
// A Zorya instance on Postgres in worker-driven mode, for end-to-end tests
// and chaos runs. Every store is shared on the database, so several of these
// processes form one deployment (CLUSTER=1 elects leaders for wake-ups and
// recovery over Postgres leases).
//
//   DATABASE_URL=postgres://… PORT=4310 INSTANCE_ID=a CLUSTER=1 \
//     bun --conditions=@promin/source packages/zorya/e2e/host.ts
//
// Tuning (ms): RECLAIM_MS (start reclaim window), SCAN_MS (wake-up scans),
// MAINT_MS / GRACE_MS (orphan recovery), ADV_TTL_MS (advertisement TTL).
// The database must be migrated (`migrate` from @promin/postgres) first.
// Prints `READY <port>` once every service started.
// ---------------------------------------------------------------------------

import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import {
  PgLeaderLeaseStore,
  PgWorkflowAdvertisementRegistry,
  PgWorkflowStartQueue,
  PostgresWorkerRegistry,
  PostgresWorkflowStorage,
  PostgresWorkflowVersionRegistry,
} from "@promin/postgres";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { QueuedWorkflows, ZoryaServer } from "../src/index.ts";

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");
const port = env("PORT", 4310);
const instanceId = process.env.INSTANCE_ID ?? `host-${process.pid}`;

const sql = postgres(url, { max: 10, onnotice: () => {} });
const db = drizzle(sql) as unknown as DrizzleDb;

const storage = await PostgresWorkflowStorage.create({ db, instanceId });
const workflows = new QueuedWorkflows({
  storage,
  workflowStarts: new PgWorkflowStartQueue({ db, reclaimAfterMs: env("RECLAIM_MS", 120_000) }),
  advertisements: new PgWorkflowAdvertisementRegistry({ db, ttlMs: env("ADV_TTL_MS", 60_000) }),
  workerRegistry: await PostgresWorkerRegistry.create({ db, instanceId }),
  versionRegistry: new PostgresWorkflowVersionRegistry(db),
  ...(process.env.CLUSTER === "1" && {
    cluster: { instanceId, leases: new PgLeaderLeaseStore({ db }) },
  }),
  wakeups: {
    sleepScanIntervalMs: env("SCAN_MS", 1_000),
    signalScanIntervalMs: env("SCAN_MS", 1_000),
  },
  maintenance: {
    intervalMs: env("MAINT_MS", 5_000),
    orphanGraceMs: env("GRACE_MS", 10_000),
    workerTimeoutMs: env("WORKER_TIMEOUT_MS", 30_000),
  },
});

const server = new ZoryaServer({
  insecure: true,
  workflows,
  remoteWorkers: {},
  logger: {
    log: (m, ...a) => console.log(`[${instanceId}] ${m}`, ...a),
    warn: () => {},
    error: (m, ...a) => console.error(`[${instanceId}] ${m}`, ...a),
  },
});
const handle = server.listen({ port, hostname: "127.0.0.1" });
await server.ready();
console.log(`READY ${handle.port}`);

const shutdown = async () => {
  await server.stop();
  await sql.end({ timeout: 2 });
  process.exit(0);
};
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
