// ---------------------------------------------------------------------------
// server.ts — Zorya server in coordinator-driven step-dispatch mode.
//
// `coordination: { enabled: true }` makes the server own the workflow state
// machine: it computes the DAG ready-set per workflow and enqueues
// individual steps to the step queue. Workers in `mode: "step"` claim
// those tasks and execute one step body per claim.
//
// Differences vs ../split/server.ts:
//   - workflow-start queue is OFF (the coordinator doesn't need it).
//   - the trigger endpoint routes through the coordinator, not the
//     workflow-start queue.
//
// Run:
//   bun run packages/zorya/examples/split-step/server.ts
// ---------------------------------------------------------------------------

import { InMemoryStepQueue, InMemoryWorkerRegistry } from "@promin/workflow";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { Database } from "bun:sqlite";
import path from "node:path";
import { mkdirSync } from "node:fs";
import {
  ZoryaServer,
  DistributedWorkflows,
  InMemoryWorkflowAdvertisementRegistry,
} from "../../src/index.ts";

const dbPath = process.env.ZORYA_DB ?? "./target/zorya-step.db";
if (dbPath !== ":memory:") {
  mkdirSync(path.dirname(dbPath), { recursive: true });
}
const db = new Database(dbPath);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });

const stepQueue = new InMemoryStepQueue();
const workerRegistry = new InMemoryWorkerRegistry();
const advertisements = new InMemoryWorkflowAdvertisementRegistry();

const uiDir = path.join(import.meta.dir, "..", "..", "dist", "public");

const workflows = new DistributedWorkflows({
  storage,
  stepQueue,
  workerRegistry,
  advertisements,
});

const server = new ZoryaServer({
  workflows,
  uiDir,
  remoteWorkers: {},
});

const port = Number(process.env.PORT ?? 4101);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya server (coordinator mode) on http://${host}:${actualPort}`);
console.log(`  - Storage: sqlite (${dbPath})`);
console.log(`  - Coordinator: enabled — server orchestrates workflows`);
console.log(`  - Start step-mode workers in another terminal:`);
console.log(`      ZORYA_URL=http://${host}:${actualPort} \\`);
console.log(`        bun run packages/zorya/examples/split-step/worker.ts`);
console.log(`  - Spin up two worker processes to see step distribution.`);
