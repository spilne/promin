# @promin/workflow-remote

HTTP/RPC adapters that run `@promin/workflow` storage and workers over the wire: one process owns the real storage and step queue, and stateless hosts reach them through a single POST endpoint each.

## Install

```bash
bun add @promin/workflow-remote
```

## What's in the box

| Export                                                            | Side   | What                                                                                             |
| ----------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------ |
| `createWorkflowStorageHandler(storage)`                           | server | `(Request) => Promise<Response>` over any `WorkflowStorage` (in-memory, Postgres, Redis, SQLite) |
| `RemoteWorkflowStorage`                                           | client | a `WorkflowStorage` that forwards every call; plugs into any runner, worker or scanner           |
| `createWorkerApiHandler({ stepQueue, storage, workerRegistry? })` | server | the worker RPC: `claim`, `release`, `complete`, `fail`, `heartbeat` (+ worker registry methods)  |
| `RemoteStepQueue`                                                 | client | the worker subset of `StepQueue`                                                                 |
| `RemoteWorkerRegistry`                                            | client | a `WorkerRegistry` over the worker RPC                                                           |

## Quick example

```typescript
// Server: owns the database.
import { PostgresWorkflowStorage } from "@promin/postgres";
import { createWorkflowStorageHandler } from "@promin/workflow-remote";
import { createPostgresDb } from "@spilne/perfect-postgres";

const storage = await PostgresWorkflowStorage.create({
  db: createPostgresDb(process.env.DATABASE_URL!),
});
Bun.serve({ port: 3001, fetch: createWorkflowStorageHandler(storage) });
```

```typescript
// Client: no database credentials.
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { RemoteWorkflowStorage } from "@promin/workflow-remote";
import { succeed } from "@spilne/perfect-core";

const storage = new RemoteWorkflowStorage({ url: "http://coord:3001/storage" });
const runner = createWorkflowRunner({ storage });

const greet = workflow<{ id: string }>({ name: "greet" })
  .step("hello", ({ input }) => succeed(`hi ${input.id}`))
  .build();

await runner.run({ workflow: greet, workflowId: "wf_1", input: { id: "u_42" } });
```

```typescript
// Remote worker: claims from the coordinator's queue over HTTP.
import { createWorker, MapStepRegistry } from "@promin/workflow/distributed";
import { RemoteStepQueue, RemoteWorkflowStorage } from "@promin/workflow-remote";

const registry = new MapStepRegistry();
registry.register({
  stepName: "transcribe",
  handler: async (ctx) => ({ text: String(ctx.input) }),
});

const worker = createWorker({
  storage: new RemoteWorkflowStorage({ url: "http://coord:3001/storage" }),
  stepQueue: new RemoteStepQueue({ url: "http://coord:3001/workers" }),
  registry,
  capabilities: ["gpu"],
});
void worker.start();
```

## Semantics over the wire

- **Same contract.** `RemoteWorkflowStorage` runs the portable
  `storageTestSuite` (fencing matrix included) and `journalReplayTestSuite`
  against a real handler. Every fenced call carries
  its `guard` in the params object; the server checks it atomically with the
  write on the underlying storage.
- **Errors keep their tag.** Tagged errors (`FenceTokenMismatchError`,
  `WorkflowLockError`, …) are tunneled as `errorTag` + `errorFields`, so
  client code branches on `_tag` exactly as against an in-process storage.
- **Wire format.** One POST endpoint per service; the body is
  `{ method, params }` encoded with `LosslessJsonCodec` (`Date`, `BigInt`,
  `Map`, `Set`, `Error`, `undefined`, `NaN`, `±Infinity` round-trip). Every
  method takes its params object, like the storage contract.
- **Worker routing** runs in the server-side claim: `claim` carries the
  worker's `workerId`, `stepNames` and `versions`, and the handler rejects a
  claim without `workerId`, so the coordinator's dead-worker sweep can
  requeue a remote worker's tasks by id.

## Capabilities and limits

| Capability                                                                | Remote |
| ------------------------------------------------------------------------- | :----: |
| `journal`, `stepAttempts`, `compensationLedger`, `tripwire`, `resetSteps` |  yes   |
| `dueTimers`, `signalWakeups`, `orphanedRuns` (scanners and recovery)      |  yes   |
| `stepCheckpoint`                                                          |   –    |
| `summaries`, `countWorkflows`, `cancelStale`                              |   –    |
| `runEvents`, `stepStartedEvents`                                          |   –    |

- **No push events.** A single request/response can't carry a live stream,
  so `subscribeToWorkflow` and `notifyStepStarted` are not on the wire. The
  runner sees no `runEvents` capability and falls back to polling
  (`runner.subscribe({ workflowId, pollIntervalMs })`, default 500 ms); it
  sees no `stepStartedEvents` and skips `step-started` events.
- **Separate checkpoint writes.** Without `stepCheckpoint` the runner writes
  a settled step's row, attempt rows and status check as separate calls (more
  round trips per step; same fencing).
- **`RemoteStepQueue` is the worker subset.** `claim`, `release`,
  `complete`, `fail` and `heartbeat` work; `enqueue`, `requeueStuck`, `get`
  and `purge` throw, and `metrics` is not on the worker wire. Enqueueing and
  the dead-worker sweep belong to the coordinator, which uses the real queue
  next to the handler.

## When to use this package

- **One storage process**: one database and the coordinator that owns it,
  with stateless agent / SDK hosts that only speak HTTP.
- **Workers without database credentials**, or in another runtime that just
  speaks the JSON-RPC envelope.

If everything runs in one process, plug the underlying storage into
`createWorkflowRunner` directly: it is faster and has the full capability set.
