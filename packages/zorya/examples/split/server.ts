// ---------------------------------------------------------------------------
// server.ts — Zorya server running standalone, no user workflow code.
// Workers connect from separate processes via @promin/zorya-client.
//
// Run:
//   bun run packages/zorya/examples/split/server.ts
// ---------------------------------------------------------------------------

import { InMemoryWorkerRegistry } from "@promin/workflow";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { Database } from "bun:sqlite";
import {
  ZoryaServer,
  QueuedWorkflows,
  InMemoryWorkflowAdvertisementRegistry,
  InMemoryWorkflowStartQueue,
} from "../../src/index.ts";
import path from "node:path";
import { mkdirSync } from "node:fs";

const dbPath = process.env.ZORYA_DB ?? "./target/zorya.db";
if (dbPath !== ":memory:") {
  mkdirSync(path.dirname(dbPath), { recursive: true });
}
const db = new Database(dbPath);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });

const workerRegistry = new InMemoryWorkerRegistry();
const advertisements = new InMemoryWorkflowAdvertisementRegistry();
const workflowStarts = new InMemoryWorkflowStartQueue();

const uiDir = path.join(import.meta.dir, "..", "..", "dist", "public");

const workflows = new QueuedWorkflows({
  storage,
  workflowStarts,
  advertisements,
});

const server = new ZoryaServer({
  workflows,
  uiDir,
  remoteWorkers: {},
  workers: new (await import("../../src/index.ts")).RegistryBackedWorkersProvider(workerRegistry),
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya server on http://${host}:${actualPort}`);
console.log(`  - Storage: sqlite (${dbPath})`);
console.log(`  - Worker protocol enabled at /rpc/worker + /rpc/storage`);
console.log(`  - Start a worker with:`);
console.log(`      bun run packages/zorya/examples/split/worker.ts`);
