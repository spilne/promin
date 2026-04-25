# @promin/workflow-remote

HTTP/RPC adapters for running `@promin/workflow` storage and workers over the wire — centralized storage service plus remote worker protocol.

## Install

```bash
bun add @promin/workflow-remote
```

## What's in the box

- `RemoteWorkflowStorage` — client-side `WorkflowStorage` that forwards every call to an HTTP handler. Plugs into any `runner` / `workflow().run()` site.
- `createWorkflowStorageHandler(storage)` — server-side handler. Wraps any `WorkflowStorage` (in-memory, Postgres, …) into a `(Request) => Response` function suitable for `Bun.serve`, Hono, Deno, or raw Node.
- `RemoteStepQueue` + `createWorkerApiHandler` — worker-facing RPC for `claim` / `complete` / `fail` / `heartbeat` against a `StepQueue` running on the coordinator.
- `RemoteWorkerRegistry` — same shape, for `WorkerRegistry`.

## Quick example

```ts
// Server (centralized storage service)
import { Bun } from "bun";
import { PostgresWorkflowStorage } from "@promin/postgres";
import { createWorkflowStorageHandler } from "@promin/workflow-remote";

const storage = await PostgresWorkflowStorage.create({ db });
const handler = createWorkflowStorageHandler(storage);
Bun.serve({ port: 3001, fetch: handler });
```

```ts
// Client (worker / SDK process)
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { RemoteWorkflowStorage } from "@promin/workflow-remote";

const storage = new RemoteWorkflowStorage({ url: "http://coord:3001/storage" });
const runner = createWorkflowRunner({ storage });

const wf = workflow<{ id: string }>({ name: "greet" })
  .step("hello", ({ input }) => Pipeline.succeed(`hi ${input.id}`))
  .build();

await runner.run({ workflow: wf, workflowId: "wf_1", input: { id: "u_42" } });
```

The `RemoteWorkflowStorage` instance passes the portable `storageTestSuite` — every CRUD, lock, signal, suspend, and list assertion holds over the wire.

## Wire format

Single POST endpoint per service. Body is `{ method, params }` encoded via `LosslessJsonCodec` so `Date`, `BigInt`, `Map`, `Set`, `Error`, `undefined`, `NaN`, `Infinity` all round-trip. The server dispatches by `method` name and re-encodes the result. No REST, no OpenAPI, no codegen — if `WorkflowStorage` grows a method, both sides pick it up automatically (provided it's added to `wire.ts`).

`FenceTokenMismatchError` and other tagged errors are tunneled with `errorTag` + `errorFields` so client code can branch on `_tag` the same way it would against an in-process storage.

## Limitations vs in-process storage

These are intentional gaps in the HTTP transport. A workflow run against `RemoteWorkflowStorage` still works, but a few hot paths degrade or no-op compared to plugging the underlying storage directly into the runner.

### `subscribeToWorkflow` is not on the wire

JSON-RPC over a single `POST` round-trip can't carry a long-lived event stream. The wire deliberately omits `subscribeToWorkflow`, so `RemoteWorkflowStorage` does not implement it.

The runner detects this (`isSubscribableStorage(storage)` → `false`) and falls back to **polling-subscribe**: it diffs successive `loadWorkflow` snapshots into `WorkflowRunEvent`s on a 500ms (default) cadence. Step-completed and the workflow terminal events flow correctly; only `step-started` is omitted because the polling diff can't observe a step that goes from "not present" to "running" to "completed" within a single tick.

```ts
// Works — runner.subscribe falls back to polling against any non-subscribable storage.
for await (const ev of runner.subscribe(workflowId, { pollIntervalMs: 100 })) {
  if (ev.type === "workflow-completed") break;
}
```

Native push over HTTP (SSE / WebSocket) is a follow-up. Until then, lower `pollIntervalMs` if you need tighter latency, or run a process directly against a `SubscribableStorage` (in-memory, Postgres with notify) for live UIs.

### `notifyStepStarted` no-ops

The runner publishes `step-started` events via the optional `notifyStepStarted(workflowId, stepName)` storage hook. `RemoteWorkflowStorage` does not implement it, so the runner's `typeof storage.notifyStepStarted === "function"` guard skips the call. No errors, no events. See the polling-subscribe note above for why this currently doesn't matter for HTTP-fronted setups.

### `RemoteStepQueue.enqueue` throws

`enqueue` is a coordinator concern — workers never enqueue, that's what schedule-the-next-step does on the server side. `RemoteStepQueue` only exposes the worker subset of `StepQueue` (`claim`, `complete`, `fail`, `heartbeat`, `requeueStuck`) and throws on `enqueue`. If you need to push a task from a remote process, route the request through your coordinator instead of bypassing it.

In `coordination: { enabled: true }` mode (`@promin/zorya`), the server-side coordinator owns enqueue: trigger requests land on `/api/runs/trigger/:name`, the `CoordinatedTriggerService` builds a stub workflow from the worker advertisement, and `coordinator.submit(...)` enqueues the ready set to the local `StepQueue`. Step-mode workers (`ZoryaWorker({ mode: "step" })`) then claim individual tasks via the existing worker wire.

### `metrics` is not on the worker wire

The worker RPC carries only the methods workers actually call. `StepQueue.metrics` is a coordinator/observability call — read it server-side against the underlying queue, or expose it via your own admin endpoint.

## When to use this package

- You want **one storage process**: a single Postgres + a coordinator that owns it, with stateless agent / SDK hosts that only talk HTTP.
- You're running **cross-language workers**: TypeScript SDK on the coordinator, workers in another runtime that just speak the JSON-RPC envelope.
- You want to keep DB credentials off the worker fleet.

If everything runs in one process, skip this package and plug the underlying storage into `createWorkflowRunner` directly — it's strictly faster and exposes the full feature set (push subscribe, step-started events, in-memory enqueue) without the limitations above.
