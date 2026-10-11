# @promin/zorya

Zorya is a self-hostable workflow platform for promin. It is one Bun HTTP
server that:

- stores and drives durable workflow runs (`@promin/workflow`): it starts
  them, wakes them when a sleep is due or a signal arrives, and recovers
  them when the process driving them dies;
- schedules runs (cron, rrule, intervals);
- serves the agent gateway (`@promin/agent`): recipes, threads, streaming
  turns, memory, skills, fragments, roles and DAGs;
- serves a dashboard (Preact) for runs, schedules, workers, agents and
  deployments;
- lets remote workers (`@promin/zorya-client`) host workflow code and
  drive runs against Zorya's storage.

Zorya is a library: you build a `ZoryaServer` from the services you want
and the stores you choose (in-memory, SQLite, Postgres, Redis). The demo
server in `examples/demo.ts` is one such host. The Docker image runs it.

For running it in production, see [DEPLOYMENT.md](./DEPLOYMENT.md).

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Auth planes and keys](#auth-planes-and-keys)
- [Configuration reference](#configuration-reference)
- [HTTP API overview](#http-api-overview)
- [Writing and running a worker](#writing-and-running-a-worker)
- [Source layout](#source-layout)

## Quick start

From the repository root:

```bash
bun install
bun run build:ui        # dashboard assets → packages/zorya/dist/public
ZORYA_INSECURE=1 bun run demo:server   # http://localhost:4100
bun run demo:worker    # optional: a remote worker (reads ZORYA_URL, ZORYA_WORKER_API_KEY)
```

`bun run demo` starts both. `ZORYA_INSECURE=1` runs without keys, for
local development only. Set `ZORYA_API_KEY` and `ZORYA_WORKER_API_KEY`
instead to run with auth (see [Auth planes](#auth-planes-and-keys)).

Workspace packages resolve to their sources through the `@promin/source`
export condition, so scripts that import them run with
`bun --conditions=@promin/source …`. The root `package.json` scripts do
this for you.

A minimal host of your own:

```ts
import { createWorkflowRunner, workflow } from "@promin/workflow";
import { SqliteWorkflowStorage } from "@promin/sqlite";
import { LocalWorkflows, ZoryaServer } from "@promin/zorya";
import { Database } from "bun:sqlite";
import { succeed } from "@spilne/perfect-core";

const hello = workflow<{ name: string }>({ name: "hello" })
  .step("greet", ({ input }) => succeed(`hello ${input.name}`))
  .build();

const storage = SqliteWorkflowStorage.make({ db: new Database("zorya.db") });
const workflows = new LocalWorkflows({
  storage,
  runner: createWorkflowRunner({ storage }),
  definitions: { hello },
});

const server = new ZoryaServer({ workflows, apiKeys: [process.env.ZORYA_API_KEY!] });
server.listen({ port: 4100 });
await server.ready();
process.on("SIGTERM", () => void server.stop().then(() => process.exit(0)));
```

## Architecture

### Workflow services: local and worker-driven

`ZoryaServer` wraps a `ZoryaWorkflows` service, which decides where a run
executes. There are two kinds, and they can be chained (`fallback`): a
trigger goes to the first layer that knows the workflow name.

| Layer             | Who runs the code                                                                                                                                                                   | Use it when                                                                              |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `LocalWorkflows`  | The Zorya process itself, with an in-process `WorkflowRunner` and the `definitions` you pass.                                                                                       | The workflow code ships with the server (the demo's `examples/workflows`).               |
| `QueuedWorkflows` | Remote workers (`ZoryaWorker` from `@promin/zorya-client`). Zorya only queues starts; the worker drives the whole run with the engine, against Zorya's storage over `/rpc/storage`. | Workflow code lives in other services, is deployed separately, or needs its own scaling. |

The demo uses `LocalWorkflows` for its bundled workflows and a
`QueuedWorkflows` fallback for anything a remote worker advertises.

### The start queue (worker-driven runs)

`QueuedWorkflows` never runs workflow code. For each run it:

1. creates the run row in storage, then enqueues a start record in its
   `WorkflowStartQueue` for that workflow name and version;
2. a worker that advertised the workflow (`/api/advertisements`) claims the
   start (`/api/worker-protocol/claim-starts`), heartbeats the claim while
   it drives the run, and completes the claim when the run ended or
   suspended (`complete-start`), or releases it to be claimed again
   (`release-start`);
3. a claim whose heartbeats stop is reclaimable after `reclaimAfterMs`, so
   a worker that dies mid-run never strands it.

Workers only claim versions they host, so a rolling deploy runs old runs on
old code. Every run write is fenced by the run lock, so a worker that lost
its claim cannot overwrite the run.

### Scanners and recovery

A durable run parks when it sleeps, waits for a signal or waits for a child
run. Something has to notice when it can continue:

- **Sleep scanner**: finds runs whose sleep is due.
- **Signal scanner**: finds runs with a delivered signal (child-run ends
  are delivered as reserved signals).
- **Orphan recovery**: finds runs that are not ended, not locked and have
  not changed for a grace period, meaning the process driving them died.

`LocalWorkflows` resumes such runs in-process. `QueuedWorkflows` turns them
into new starts on the queue (a wake-up), so a worker drives them; its
maintenance sweep also requeues orphans and drops the advertisements of
dead workers. On boot, `LocalWorkflows` can apply a one-shot
`RecoveryStrategy` (for example, fail runs that went stale while the server
was down).

### Clustering

Several Zorya instances can share one set of stores. Pass the same
`cluster: { instanceId, leases }` to every workflows layer. Then every
background loop (scanners, recovery, maintenance) runs on one leader at a
time, elected over the shared `LeaderLeaseStore` (`PgLeaderLeaseStore`,
`RedisLeaderLeaseStore`, or `SqliteLeaderLeaseStore` on one host).

A clustered layer refuses in-memory stores (`ZoryaClusterConfigError`),
because each instance would hold its own copy. The server applies the same
check to its version registry, worker registry and scheduler storage. The
scheduler elects its own leader through its storage, so give each instance
a distinct `ZoryaScheduler.instanceId`.

### Background services and lifecycle

`listen()` starts the background services (workflows, scheduler, agents,
skills, fragments) and the worker WebSocket. A service whose `start()`
fails (storage down at boot, for example) is retried with backoff.
`/api/ready` answers 200 only once storage responds and every service has
started. `stop()` drains the server gracefully (see
[DEPLOYMENT.md](./DEPLOYMENT.md#graceful-shutdown)).

## Auth planes and keys

Every plane is authenticated. Keys travel only as `Authorization: Bearer
<key>`, including on the worker WebSocket upgrade, and are compared in
constant time.

| Plane          | Paths                                                                                                              | Credential                                                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| Operator       | `/api/*`, and the dashboard's API calls                                                                            | `apiKeys`                                                                                                                     |
| Worker         | `/ws/worker`, `/rpc/worker`, `/api/advertisements`, `/api/worker-protocol/*`                                       | `remoteWorkers.apiKeys`                                                                                                       |
| Worker storage | `/rpc/storage`                                                                                                     | Worker keys can call only the methods a runner needs (`RUNNER_STORAGE_METHODS`). Operator keys get the full storage contract. |
| Public         | `GET /api/health`, `GET /api/ready`, `GET /api/signal-tokens/:id/describe`, `POST /api/signal-tokens/:id/complete` | None. The signal-token routes check the token bearer themselves.                                                              |
| Webhooks       | `POST /webhooks/:source/:agentId`                                                                                  | Per-source HMAC signature (`zorya-v1`)                                                                                        |

The constructor throws `ZoryaInsecureConfigError` when `apiKeys` is empty,
or when `remoteWorkers` is set without `remoteWorkers.apiKeys`. The only way
to run a plane open is `insecure: true` (`ZORYA_INSECURE=1` for the demo),
which logs a warning. Never expose an insecure server.

Use different keys for the two planes: a worker key cannot read or change
runs through `/api`, and cannot reach storage methods beyond what a runner
needs.

## Configuration reference

`new ZoryaServer(config: ZoryaServerConfig)`:

| Field                         | Default                 | Meaning                                                                                                                                                                                              |
| ----------------------------- | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workflows`                   | required                | The `ZoryaWorkflows` chain (`LocalWorkflows`, `QueuedWorkflows`). Its storage backs every run route. The version registry, advertisements, definitions and cluster are found on the chain.           |
| `apiKeys`                     | none                    | Operator keys for `/api`. Required unless `insecure`.                                                                                                                                                |
| `insecure`                    | `false`                 | Allow planes without keys. Local development only.                                                                                                                                                   |
| `remoteWorkers`               | not mounted             | Mounts the worker plane. `apiKeys`: worker keys. `workerRegistry`: the registry workers register into (default: the queued layer's, else in-memory).                                                 |
| `scheduler`                   | none                    | `ZoryaScheduler`: schedule CRUD, tick loop, emit-now. Without it `GET /api/schedules` answers `configured: false`.                                                                                   |
| `agents`                      | none                    | `ZoryaAgents`: the agent gateway, plus memory, instances and roles when their stores are set.                                                                                                        |
| `skills`, `fragments`, `dags` | none                    | Registry services with CRUD routes.                                                                                                                                                                  |
| `secrets`                     | none                    | `SecretsStorage` for `/api/secrets` and BYOK credential resolution.                                                                                                                                  |
| `webhooks`                    | none                    | `{ sources, deliveries? }`: signed webhook ingress into agents. Mounted only when `agents` is set. `deliveries` (replay dedup) defaults to a store on the workflow storage, shared across instances. |
| `remoteDeployments`           | none                    | `{ registry, endpointPolicy?, sweepIntervalMs? }`: other Zorya servers register their agents here. Mounted only when `agents` is set.                                                                |
| `metrics`                     | storage-backed          | `MetricsProvider` for `/api/metrics`.                                                                                                                                                                |
| `workers`                     | registry-backed         | `WorkersProvider` for the Workers page.                                                                                                                                                              |
| `sampleInput`                 | none                    | Sample input for the dashboard trigger form, by workflow name.                                                                                                                                       |
| `uiDir`                       | none                    | Directory of the built dashboard. Unset: no UI is served.                                                                                                                                            |
| `publicBaseUrl`               | none                    | Public URL used to build signal-token completion URLs.                                                                                                                                               |
| `signalTokenMaxTtlMs`         | 90 days                 | Longest validity a signal token may request.                                                                                                                                                         |
| `sseIntervalMs`               | 1000                    | Poll interval behind run event streams.                                                                                                                                                              |
| `readinessTimeoutMs`          | 2000                    | Bound on the `/api/ready` storage probe.                                                                                                                                                             |
| `shutdownTimeoutMs`           | 10000                   | How long `stop()` waits for in-flight requests.                                                                                                                                                      |
| `startRetry`                  | 500 ms doubling to 30 s | Backoff for a service whose `start()` failed.                                                                                                                                                        |
| `clock`                       | `SystemWallClock`       | Time source for the server's own timers. Tests pass a `FakeWallClock`.                                                                                                                               |
| `logger`                      | `console`               | `{ log, warn, error }`.                                                                                                                                                                              |

`server.listen(options: ListenOptions)`:

| Field                | Default   | Meaning                                                           |
| -------------------- | --------- | ----------------------------------------------------------------- |
| `port`               | 4000      | Port. `0` picks a free one.                                       |
| `hostname`           | `0.0.0.0` | Interface to bind.                                                |
| `maxRequestBodySize` | 1 MiB     | Largest body outside `/webhooks/*`. Larger bodies get a JSON 413. |
| `maxWebhookBodySize` | 25 MiB    | Largest `/webhooks/*` body.                                       |
| `idleTimeout`        | 60 s      | Idle connection timeout (max 255).                                |
| `development`        | `false`   | Bun's development error pages.                                    |

The workflows layers have their own options (scan intervals, orphan grace,
maintenance cadence, `cluster`, `clock`, `onError`). See
`LocalWorkflowsConfig` and `QueuedWorkflowsConfig`.

### Demo server environment

`examples/demo.ts` (and the Docker image) read:

| Variable                                                                                             | Default                                             | Meaning                                                                                                          |
| ---------------------------------------------------------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `ZORYA_API_KEY`                                                                                      | none                                                | Operator key. Required unless `ZORYA_INSECURE=1`.                                                                |
| `ZORYA_WORKER_API_KEY`                                                                               | none                                                | Worker key. Required unless `ZORYA_INSECURE=1`.                                                                  |
| `ZORYA_INSECURE`                                                                                     | unset                                               | `1` runs without keys.                                                                                           |
| `PORT`                                                                                               | 4100                                                | HTTP port.                                                                                                       |
| `ZORYA_DB`                                                                                           | `./target/zorya.db` (`/data/zorya.db` in the image) | SQLite file for runs, schedules, the start queue and registries. `:memory:` for a throwaway database.            |
| `ZORYA_PG_URL`                                                                                       | unset                                               | Puts agent, skill, fragment and memory stores on Postgres (migrated on boot). Runs and schedules stay on SQLite. |
| `ZORYA_UI_DIR`                                                                                       | `packages/zorya/dist/public`                        | Built dashboard assets.                                                                                          |
| `PROMIN_SECRETS_PASSPHRASE`                                                                          | a demo value                                        | Encrypts the secrets vault. Set it for anything but a demo.                                                      |
| `ANTHROPIC_API_KEY`, `OLLAMA_URL`, `OLLAMA_MODEL`, `OLLAMA_CONSOLIDATOR_MODEL`, `ZORYA_CONSOLIDATOR` | unset                                               | LLM backends for the demo agents. Without them the agents fall back to Ollama or an echo model.                  |

## HTTP API overview

All JSON. Errors are `{ error, message?, issues? }` with a stable `error`
code. Unexpected failures answer 500 with a `requestId` that names the log
line; internal detail never reaches the caller.

| Area         | Routes                                                                                                                                                                                                                      |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Probes       | `GET /api/health` (liveness), `GET /api/ready` (readiness)                                                                                                                                                                  |
| Runs         | `GET /api/runs`, `GET /api/runs/:id`, `.../signals`, `.../attempts`, `.../history`, `.../children`, `.../journal/:stepName`; `POST /api/runs/:id/{cancel,signal,rerun,resume,mark-success,mark-failed}`                     |
| Triggering   | `POST /api/workflows/:name/runs` (`{ input, workflowId?, version?, namespace?, metadata? }`); a repeated `workflowId` for the same workflow answers with the existing run; for another workflow, `409 workflow_id_conflict` |
| Workflows    | `GET /api/workflows`, `.../definitions`, `.../sparklines`, `/api/workflows/:name/{definition,grid,history}`                                                                                                                 |
| Versions     | `GET /api/workflows/:name/versions`, `.../versions/active`; `POST .../versions/:version/promote`, `POST .../rollback`                                                                                                       |
| Signals      | `GET /api/signals`; signal tokens: `POST /api/runs/:id/signals/:name/token`, `GET /api/runs/:id/signal-tokens`, public `describe` / `complete`                                                                              |
| Live data    | `GET /api/runs/:id/events` (SSE), `GET /api/runs/:id/streams/:streamId` (SSE), `.../chunks`, `POST /api/runs/:id/streams/:streamId`, `GET /api/runs/:id/agent-stream` (SSE), `POST /api/runs/:id/query`                     |
| Fleet        | `GET /api/workers`, `GET /api/metrics`                                                                                                                                                                                      |
| Schedules    | `GET/POST /api/schedules`, `GET/PATCH/DELETE /api/schedules/:id`, `.../history`, `.../upcoming`, `POST .../emit`                                                                                                            |
| Agents       | `/api/agents` CRUD, `/api/agents/_catalog/*`, `_sources`, `_draft`, `/api/agents/:id/{versions,clone,extract-role,invoke,stream,threads,...}`                                                                               |
| Agent data   | `/api/memory/*`, `/api/instances`, `/api/agents/:id/instances/*`, `/api/roles/*`, `/api/skills/*`, `/api/fragments/*`, `/api/secrets/*`, `/api/dags/*`, `/api/remote-deployments/*`                                         |
| Worker plane | `/ws/worker`, `POST /rpc/storage`, `POST /rpc/worker`, `/api/advertisements`, `/api/worker-protocol/{claim-starts,heartbeat-start/:id,complete-start/:id,release-start/:id,starts}`                                         |
| Webhooks     | `POST /webhooks/:source/:agentId`                                                                                                                                                                                           |

The wire types live in `src/server/api-types.ts` and are exported from the
package root.

## Writing and running a worker

A worker hosts workflow code and drives runs for a `QueuedWorkflows`
layer. Mount the worker plane and a queued layer on the server:

```ts
import { QueuedWorkflows, ZoryaServer } from "@promin/zorya";

const workflows = new QueuedWorkflows({
  storage, // shared stores in production, see DEPLOYMENT.md
  workflowStarts,
  advertisements,
  workerRegistry,
});
const server = new ZoryaServer({
  workflows,
  apiKeys: [process.env.ZORYA_API_KEY!],
  remoteWorkers: { apiKeys: [process.env.ZORYA_WORKER_API_KEY!] },
});
```

Then run workers with `@promin/zorya-client`:

```ts
import { ZoryaClient, ZoryaWorker } from "@promin/zorya-client";
import { hello } from "./workflows.ts";

const worker = new ZoryaWorker({
  client: new ZoryaClient({
    url: process.env.ZORYA_URL!,
    apiKey: process.env.ZORYA_WORKER_API_KEY,
  }),
  workflows: [hello],
  workerId: process.env.WORKER_ID, // stable per process; default: random
  concurrency: 10, // most runs driven at once
});
worker.start();
await worker.ready(); // registered and advertised

process.on("SIGTERM", async () => {
  await worker.stop({ timeoutMs: 10_000 }); // finish or hand back runs in flight
  process.exit(0);
});
```

What the worker does:

- registers and heartbeats in the worker registry (`/rpc/worker`), and
  advertises the workflows and versions it hosts;
- polls the start queue for those workflows, claims at most as many starts
  as it has free slots, and drives each run with the engine against
  Zorya's storage, fenced by the run lock;
- completes a claim when its run ended or suspended, and releases it when
  the run stopped short, so another worker picks it up;
- retries start-up and every protocol call with backoff, so it can start
  before Zorya and survives Zorya restarts, re-advertising on its own.

Step bodies are at-least-once: a run resumed after a crash re-runs the step
that was in flight. Make them idempotent.

Starting runs from application code:

```ts
const client = new ZoryaClient({
  url,
  apiKey: workerKey,
  operatorApiKey: operatorKey, // `start` uses the operator plane
});
const handle = await client.start(hello, { input: { name: "Ada" } });
const result = await handle.result();
```

To run a workflow inside your own process and only persist its state to
Zorya (no queue, no advertisement), use `ZoryaRunner` instead. See
`examples/user-app.ts`. A worker can also serve query handlers and agent
streams over the `/ws/worker` control socket (`WorkerControlSocket`,
`runAgentTask`).

## Source layout

```
src/server/
  server.ts            ZoryaServer: resolves the chain's stores, mounts, lifecycle
  server-config.ts     ZoryaServerConfig, ListenOptions, Logger, defaults
  mount/
    mount-auth.ts      auth planes: which credentials each path needs
    mount-lifecycle.ts probes, background services, in-flight and stream tracking
    mount-listener.ts  Bun.serve: worker WebSocket upgrade, body limits
    mount-runs.ts      runs, workflows, versions, signals, streams, workers, metrics
    mount-agents.ts    agent gateway, memory, instances, roles
    mount-registries.ts skills, fragments, secrets, DAGs
    mount-ingress.ts   webhooks, remote deployments
    mount-schedules.ts schedules
    mount-rpc.ts       /rpc/storage, /rpc/worker, advertisements, start queue
    mount-static.ts    dashboard assets
  routes/              route handlers (routes/agents/ split by concern)
  services/            ZoryaWorkflows layers, scheduler, agents, worker socket
src/ui/                dashboard (Preact), built by scripts/build-ui.ts
examples/              the demo server, its workflows, agents and tools
e2e/                   multi-process Postgres end-to-end tests
```
