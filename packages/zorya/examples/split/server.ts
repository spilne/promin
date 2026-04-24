// ---------------------------------------------------------------------------
// server.ts — Zorya server running standalone, no user workflow code.
// Workers connect from separate processes via @promin/zorya-client.
//
// Run:
//   bun run packages/zorya/examples/split/server.ts
// ---------------------------------------------------------------------------

import { InMemoryStepQueue, InMemoryWorkerRegistry } from "@promin/workflow";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { Database } from "bun:sqlite";
import { ZoryaServer, InMemoryWorkflowAdvertisementRegistry } from "../../src/index.ts";
import path from "node:path";

const dbPath = process.env.ZORYA_DB ?? ":memory:";
const db = new Database(dbPath);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });

// In-memory primitives for the remote worker wire. Production would use
// the Postgres variants of StepQueue + WorkerRegistry instead.
const stepQueue = new InMemoryStepQueue();
const workerRegistry = new InMemoryWorkerRegistry();
const advertisements = new InMemoryWorkflowAdvertisementRegistry();

const uiDir = path.join(import.meta.dir, "..", "..", "dist", "public");

const server = new ZoryaServer({
  storage,
  uiDir,
  workerProtocol: {
    stepQueue,
    workerRegistry,
    advertisements,
  },
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya server on http://${host}:${actualPort}`);
console.log(`  - Storage: sqlite (${dbPath})`);
console.log(`  - Worker protocol enabled at /rpc/worker + /rpc/storage`);
console.log(`  - Start a worker with:`);
console.log(`      bun run packages/zorya/examples/split/worker.ts`);
