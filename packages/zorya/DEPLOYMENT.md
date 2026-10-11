# Deploying Zorya

This guide covers four shapes of deployment, from a single process to a
cluster, then the Docker image, probes and shutdown. [README.md](./README.md)
explains the architecture and the configuration fields used below.

- [Single node with SQLite](#single-node-with-sqlite)
- [Production with Postgres](#production-with-postgres)
- [Redis-backed parts and their limits](#redis-backed-parts-and-their-limits)
- [Docker image](#docker-image)
- [Health and readiness probes](#health-and-readiness-probes)
- [Graceful shutdown](#graceful-shutdown)
- [Checklist](#checklist)

## Single node with SQLite

One Zorya process with every store in one SQLite file. This is what the
demo server and the Docker image run. It suits a single host, development,
and small installations.

```ts
import { Database } from "bun:sqlite";
import { createWorkflowRunner } from "@promin/workflow";
import {
  SqliteSchedulerStorage,
  SqliteWorkerRegistry,
  SqliteWorkflowAdvertisementRegistry,
  SqliteWorkflowStartQueue,
  SqliteWorkflowStorage,
} from "@promin/sqlite";
import { LocalWorkflows, QueuedWorkflows, ZoryaScheduler, ZoryaServer } from "@promin/zorya";

const db = new Database("/data/zorya.db");
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });
const workflows = new LocalWorkflows({
  storage,
  runner: createWorkflowRunner({ storage }),
  definitions, // workflows that ship with this server
  // Anything else goes to remote workers through the start queue.
  fallback: new QueuedWorkflows({
    storage,
    workflowStarts: SqliteWorkflowStartQueue.make({ db }),
    advertisements: SqliteWorkflowAdvertisementRegistry.make({ db }),
    workerRegistry: SqliteWorkerRegistry.make({ db }),
  }),
});
const scheduler = new ZoryaScheduler({
  storage: SqliteSchedulerStorage.make({ db }),
  workflows,
});

const server = new ZoryaServer({
  workflows,
  scheduler,
  apiKeys: [process.env.ZORYA_API_KEY!],
  remoteWorkers: { apiKeys: [process.env.ZORYA_WORKER_API_KEY!] },
  uiDir: "/app/packages/zorya/dist/public",
});
server.listen({ port: 4100 });
```

Notes:

- The SQLite stores create their tables on first use. There is no separate
  migration step.
- Put the database file on a persistent volume and back it up like any
  SQLite file (for example `sqlite3 zorya.db ".backup backup.db"`).
- Runs, the start queue, advertisements and the worker registry survive a
  restart. On boot, the scanners and orphan recovery pick up runs that were
  parked or in flight when the process stopped.
- Run history grows with every run. Set `retention: { maxAgeMs }` on the
  server to delete ended runs past that age (see
  [Retention](./README.md#retention)); the demo keeps one day.
- Several processes on the same host can share one SQLite file as a
  cluster with `SqliteLeaderLeaseStore`, but one process per database is
  the expected shape. Use Postgres to scale out.

## Production with Postgres

Postgres holds every store, so any number of Zorya instances can run behind
a load balancer and share the work.

### Schema and migrations

`@promin/postgres` ships one Drizzle migration set covering workflow runs,
steps, signals, signal tokens, streams, locks, the step queue, the start
queue, advertisements, the worker registry, the version registry, the
namespace registry, schedules, leader leases, and the agent stores. Apply it before the first
instance starts, and again on each upgrade:

```ts
import { migrate } from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";

const db = createPostgresDb(process.env.DATABASE_URL!);
await migrate(db); // idempotent: safe on every boot, or as a release step
```

Running `migrate` from a one-off release job, before the new instances
roll out, keeps instances from racing to migrate. `migrationsTable` and
`migrationsSchema` let Zorya share a database with other applications.

### A Postgres host

```ts
import { createWorkflowRunner } from "@promin/workflow";
import {
  PgLeaderLeaseStore,
  PgSchedulerStorage,
  PgWorkflowAdvertisementRegistry,
  PgWorkflowStartQueue,
  PostgresWorkerRegistry,
  PostgresWorkflowStorage,
  PostgresWorkflowVersionRegistry,
} from "@promin/postgres";
import { createPostgresDb } from "@spilne/perfect-postgres";
import { LocalWorkflows, QueuedWorkflows, ZoryaScheduler, ZoryaServer } from "@promin/zorya";

const db = createPostgresDb(process.env.DATABASE_URL!);
// Unique per instance: a pod name, a hostname, or a random id.
const instanceId = process.env.INSTANCE_ID ?? crypto.randomUUID();
const cluster = { instanceId, leases: new PgLeaderLeaseStore({ db }) };

const storage = await PostgresWorkflowStorage.create({ db, instanceId });
const versionRegistry = new PostgresWorkflowVersionRegistry(db);
const workerRegistry = await PostgresWorkerRegistry.create({ db, instanceId });

const workflows = new LocalWorkflows({
  storage,
  runner: createWorkflowRunner({ storage }),
  definitions,
  versionRegistry,
  cluster,
  fallback: new QueuedWorkflows({
    storage,
    workflowStarts: new PgWorkflowStartQueue({ db }),
    advertisements: new PgWorkflowAdvertisementRegistry({ db }),
    workerRegistry,
    versionRegistry,
    cluster,
  }),
});

const scheduler = new ZoryaScheduler({
  storage: new PgSchedulerStorage({ db }),
  workflows,
  instanceId,
});

const server = new ZoryaServer({
  workflows,
  scheduler,
  apiKeys: process.env.ZORYA_API_KEYS!.split(","),
  remoteWorkers: { apiKeys: process.env.ZORYA_WORKER_API_KEYS!.split(",") },
  uiDir: process.env.ZORYA_UI_DIR,
  publicBaseUrl: "https://zorya.example.com",
});
server.listen({ port: Number(process.env.PORT ?? 4100) });
process.on("SIGTERM", () => void server.stop().then(() => process.exit(0)));
```

A worker-only deployment drops `LocalWorkflows` and uses the
`QueuedWorkflows` layer on its own (`e2e/host.ts` is a complete example).
Agents, skills, fragments, roles, secrets and memory have Postgres stores
too (`PostgresAgentRegistry`, `PostgresSkillRegistry`,
`PostgresFragmentStore`, `PostgresRoleRegistry`, `PostgresSecretsStorage`,
`PostgresMemoryStore`, all from `@promin/postgres`); pass them to the
corresponding Zorya services. For registered namespaces (see the README's
Namespaces section) pass `namespaces: new PostgresNamespaceRegistry({ db })`;
every instance then sees the same namespaces and archive states.

### Leases and clustering

With `cluster` set, the instances elect one leader per background loop
(sleep scanner, signal scanner, orphan recovery, queued maintenance or run
retention) through `wf_leader_leases`. Lease expiry is computed on the
database clock, so instance clock skew does not matter. A lease is held for about
three loop intervals (at least 5 s); when its holder dies, a peer takes
over within that window. Retention sweeps hourly by default, so its lease
can take up to three hours to move after its holder dies (a clean `stop()`
releases it at once); a missed sweep only delays deletion. The leader's `retention` settings apply, so give
every instance the same ones.

The scheduler elects its own leader through `PgSchedulerStorage`. Give each
instance a distinct `instanceId` there too.

Everything else is already safe to run on every instance at once: run
writes are fenced by the run lock, start claims by claim tokens, schedule
ticks by the scheduler lease, and webhook deliveries are deduplicated in
the shared workflow storage.

A clustered layer refuses in-memory stores at construction
(`ZoryaClusterConfigError`), including the server's worker registry,
version registry, scheduler storage and namespace registry. That catches a store you forgot to
share.

### Multiple instances behind a load balancer

- Every instance serves the full API and the dashboard. Any instance can
  take any request that only touches storage: run reads and actions,
  triggers, signals, SSE run events and stream reads (they poll the shared
  storage), schedules, agent CRUD, and the worker HTTP protocol (start
  claims, advertisements, `/rpc/*`).
- **Use sticky routing for the worker WebSocket and what rides on it.** A
  worker's `/ws/worker` socket lives on the one instance it connected to.
  Two features go through that socket and are answered only by that
  instance:
  - `POST /api/runs/:id/query` (workflow query handlers): asks the workers
    connected to the instance that received the request;
  - `GET /api/runs/:id/agent-stream` (live agent token streams): relays
    frames from workers connected to that instance.

  With several instances, route a worker's socket and the dashboard
  clients that query its runs to the same instance (for example, hash on a
  worker or tenant header, or run a dedicated instance for interactive
  agent streaming), or accept that these two features answer only for
  workers on the same instance. Everything else is unaffected.

- **Worker wake-ups are per instance; polling covers the rest.** When an
  instance queues a start (a trigger, a schedule tick, a sleep or signal
  wake-up, a recovery sweep, a resume), it sends "work available" over
  `/ws/worker` only to the workers connected to _that_ instance. A worker
  connected elsewhere finds the start at its next fallback poll (default
  5 s ±20%), so in a cluster start latency is a few milliseconds for
  workers on the enqueuing instance and up to one poll interval for the
  others. To tighten it, lower `pollWorkflowStarts.intervalMs` on the
  workers (each poll is one claim query), or route triggers and workers to
  the same instance. There is no shared notification bus between
  instances yet (a Postgres `LISTEN/NOTIFY` or Redis pub/sub fan-out would
  close this gap). The follow-up wake-up of an unclaimed start only knows
  about claims made on its own instance, so in a cluster it may wake that
  instance's workers once more for a start claimed elsewhere (a wasted
  claim, never a lost start). A worker whose `/ws/worker` upgrade fails
  (a proxy without WebSocket support, a key mismatch) reports it once and
  polls every second until the socket opens. `remoteWorkers.wakeups: false`
  turns the messages off; workers then still poll at the 5 s
  socket-open interval, so lower `pollWorkflowStarts.intervalMs` (to 1 s,
  say) when you disable them. Only workers that claim starts are woken: a
  socket opened for queries or agent streams, or a worker with
  `pollWorkflowStarts: false`, identifies as not claiming and gets no
  "work available" messages.

- Agent threads: set a `turnGate` on `ZoryaAgents` (an `AgentTurnGate`
  over `PostgresLeaseStore`) so two instances never run the same
  conversation turn at once.
- Configure the load balancer to send SSE through unbuffered, with an idle
  timeout above the stream heartbeat, and to allow WebSocket upgrades on
  `/ws/worker`.
- Health-check every instance with `/api/ready` and take it out of the
  pool on 503 (see [probes](#health-and-readiness-probes)).

## Redis-backed parts and their limits

`@promin/redis` provides `RedisWorkflowStorage`, `RedisStepQueue`,
`RedisSchedulerStorage` and `RedisLeaderLeaseStore` (all Redis Cluster
safe). That is enough for:

- `LocalWorkflows` on `RedisWorkflowStorage`, with in-process workflow code;
- `ZoryaScheduler` on `RedisSchedulerStorage`;
- leader election for the scanners and recovery on `RedisLeaderLeaseStore`.

The limits today:

- **There is no Redis start queue, advertisement registry, worker registry
  or version registry.** Worker-driven runs (`QueuedWorkflows`) need a
  `WorkflowStartQueue` and an advertisement registry, so with Redis storage
  they would have to use the in-memory ones: lost on restart and not shared
  between instances, which a clustered layer refuses. Keep worker-driven
  workflows on Postgres (or SQLite on one host).
- **Clustering needs a shared worker registry.** The server's worker
  registry defaults to an in-memory one, which a cluster refuses. A
  clustered Redis deployment has to pass a shared registry
  (`remoteWorkers.workerRegistry`, for example `PostgresWorkerRegistry`);
  otherwise run it as a single instance.
- Leave `versionRegistry` unset on a clustered Redis deployment, or point
  it at Postgres: the in-memory version registry is refused too.
- The per-instance worker WebSocket limits above (query handlers and agent
  streams) apply with any backend.

## Docker image

`packages/zorya/Dockerfile` builds the demo server into a production
image:

```bash
docker build -f packages/zorya/Dockerfile -t zorya:latest .   # from the repo root
docker run --rm -p 4100:4100 -v zorya-data:/data --stop-timeout 30 \
  -e ZORYA_API_KEY="$(openssl rand -hex 32)" \
  -e ZORYA_WORKER_API_KEY="$(openssl rand -hex 32)" \
  zorya:latest
```

How it is built (multi-stage):

1. **build** installs the whole monorepo, builds the dashboard
   (`dist/public`) and writes manifests for the runtime workspaces
   (`scripts/image-manifest.ts`);
2. **deps** installs only the runtime dependencies of the workspaces the
   server loads (`workflow`, `workflow-remote`, `agent`, `evals`, `sqlite`,
   `postgres`, `zorya`), at the versions pinned in `bun.lock`;
3. **sources** copies those workspaces' sources without tests, plus the
   Postgres migrations and `packages/zorya/examples`;
4. **runtime** combines them on a clean `oven/bun` Alpine image.

The server runs from source under Bun (`--conditions=@promin/source`)
rather than as one bundle, because the demo imports its workflows, agents
and tools from `examples/` at boot, and those files must share module
instances with the server.

| Setting     | Value                                                    |
| ----------- | -------------------------------------------------------- |
| Port        | `4100` (`PORT`)                                          |
| Volume      | `/data` (the SQLite database, `ZORYA_DB=/data/zorya.db`) |
| User        | `bun` (non-root). Only `/data` is writable.              |
| Healthcheck | `GET /api/ready` every 15 s                              |
| Stop signal | `SIGTERM` (graceful, see below)                          |

Environment variables are listed in
[README.md](./README.md#demo-server-environment). `ZORYA_API_KEY` and
`ZORYA_WORKER_API_KEY` are required; the container exits at start without
them, unless `ZORYA_INSECURE=1`.

The image runs the demo host (`examples/demo.ts`) on SQLite. For a
Postgres cluster, build your own host module (as above) and use the same
Dockerfile stages with a different `ENTRYPOINT`.

## Health and readiness probes

| Probe             | Answers                                                                                                                                                                                                                                                                                                                                        | Use for                                   |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| `GET /api/health` | `200 { ok: true }` while the process serves HTTP. Touches nothing.                                                                                                                                                                                                                                                                             | Liveness                                  |
| `GET /api/ready`  | `200` when a storage read succeeds within `readinessTimeoutMs` and every background service has started; else `503` with `{ ready, storage, services }`. Once shutdown begins the listener refuses new connections, so a probe on a fresh connection fails to connect; one on an open keep-alive connection gets `503 { reason: "stopping" }`. | Readiness and load-balancer health checks |

Both are public (no key). A service whose `start()` failed is retried with
backoff, and its state (`pending`, `starting`, `retrying`, `started`,
`stopped`, attempts, last error) is reported in `services`, so a deploy
that boots against a database that is down stays not-ready instead of
serving without its background loops.

Kubernetes:

```yaml
spec:
  terminationGracePeriodSeconds: 30 # longer than shutdownTimeoutMs
  containers:
    - name: zorya
      livenessProbe:
        httpGet: { path: /api/health, port: 4100 }
        periodSeconds: 10
      readinessProbe:
        httpGet: { path: /api/ready, port: 4100 }
        periodSeconds: 5
        failureThreshold: 2
```

## Graceful shutdown

`server.stop()` (the demo and the image call it on `SIGTERM` and `SIGINT`):

1. stops accepting connections; requests that still arrive on open
   keep-alive connections get `503 shutting_down` (the probes answer,
   `/api/ready` with 503);
2. closes worker WebSockets and open SSE streams, so clients reconnect to
   another instance;
3. waits up to `shutdownTimeoutMs` (default 10 s) for in-flight requests;
4. stops the background services, which release their run locks and
   leader leases, so peers take over at once instead of after a lease
   expiry;
5. closes the remaining connections.

Set the orchestrator's grace period above `shutdownTimeoutMs` plus a few
seconds. A second signal to the demo exits at once.

Runs survive any shutdown, graceful or not: a run that was in flight is
picked up by orphan recovery (after `orphanGraceMs`) on a surviving
instance, or on this one after it restarts. Workers stopped with
`worker.stop({ timeoutMs })` finish or release their runs the same way.

## Checklist

- Distinct, high-entropy operator and worker keys (`openssl rand -hex 32`).
  Rotate by listing old and new keys together, then dropping the old one.
- TLS in front of Zorya. Keys are bearer tokens.
- `PROMIN_SECRETS_PASSPHRASE` set when the secrets vault is in use.
- Postgres migrated before rollout; one `instanceId` per instance.
- `cluster` set on every workflows layer when there is more than one
  instance; sticky routing for `/ws/worker` if you use query handlers or
  agent streams.
- `retention` set to how long ended runs must stay visible, so run history
  stays bounded.
- Readiness probe on `/api/ready`; grace period above `shutdownTimeoutMs`.
