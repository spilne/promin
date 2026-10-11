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
import { workflow } from "@promin/workflow";
import { LocalWorkflows, ZoryaServer } from "@promin/zorya";
import { createSqliteZoryaStack } from "@promin/zorya/sqlite";

const hello = workflow<{ name: string }>({ name: "hello" })
  .step("greet", ({ input }) => `hello ${input.name}`)
  .build();

// Every store on one SQLite file (needs `@promin/sqlite` installed).
const stack = createSqliteZoryaStack({ path: "./data/zorya.db" });
const workflows = new LocalWorkflows({
  storage: stack.storage,
  runner: stack.runner,
  definitions: { hello },
});

const server = ZoryaServer.builder()
  .workflows(workflows)
  .apiKeys([process.env.ZORYA_API_KEY!])
  .build();
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

### Retention

Runs are kept forever unless the server is given `retention`. With it, a
background loop deletes ended runs (completed, failed, tripwire) once they
ended more than `maxAgeMs` ago, together with everything stored for them.
Each sweep deletes in bounded batches until nothing expired is left, so
one large backlog does not become one long delete. Running, suspended and
pending runs are never touched. The age is measured against the storage's
own clock, the one that stamped `completedAt`.

```ts
const server = new ZoryaServer({
  workflows,
  apiKeys,
  retention: { maxAgeMs: 30 * 24 * 60 * 60 * 1000 }, // 30 days
});
```

### Namespaces

Runs, schedules, agent memory, instances and secrets carry a namespace: an
id of 1–128 letters, digits and `_ . : -`, starting with a letter or digit
(`NAMESPACE_ID_PATTERN`). Every route that takes a namespace validates it so.
Without further configuration any well-formed namespace is accepted, and work
without one lands in the global namespace.

Give the server a `NamespaceRegistry` (`namespaces`; `SqliteNamespaceRegistry`
from the SQLite stack, `PostgresNamespaceRegistry`, or the in-memory one for
tests) to make namespaces explicit entities. `/api/namespaces` then manages
them: each has a display name, description, free-form metadata, a status
(`active` or `archived`) and a capability policy (`ai`, `agents`,
`workflows`, `tools` limits) that is validated and stored for hosts to read;
Zorya does not enforce it. Routes that put new work into a namespace check
it: triggering a run, creating a schedule, invoking an agent, writing
namespace memory, an agent instance or a namespace- or resource-scoped secret,
and accepting a webhook delivery answer `404 namespace_not_found` for an
unregistered namespace and `409 namespace_archived` for an archived one. Reads
and filters are not checked (an unknown namespace matches nothing; archived
data stays readable and deletable), and the global namespace needs no entry.
Schedules already in a namespace keep firing after it is archived; disable
them first. The dashboard's agent chat uses the namespace `default` when no
namespace is selected, so register it if you use the chat.

A namespace is deleted in two steps: archive it (`PATCH { "status":
"archived" }`), then `DELETE` it, which answers `409 namespace_in_use` while
runs or schedules still name it. At start, the server registers (as active)
every well-formed namespace its stored runs already use, so adding a
registry to an existing deployment keeps its tenants working.

```ts
const stack = createSqliteZoryaStack({ path: "./data/zorya.db" });
await stack.namespaceRegistry.create({ id: "acme", displayName: "Acme" });
const server = ZoryaServer.builder()
  .workflows(workflows)
  .namespaces(stack.namespaceRegistry)
  .apiKeys([process.env.ZORYA_API_KEY!])
  .build();
```

### Clustering

Several Zorya instances can share one set of stores. Pass the same
`cluster: { instanceId, leases }` to every workflows layer. Then every
background loop (scanners, recovery, maintenance, retention) runs on one
leader at a time, elected over the shared `LeaderLeaseStore`
(`PgLeaderLeaseStore`, `RedisLeaderLeaseStore`, or `SqliteLeaderLeaseStore`
on one host).

A clustered layer refuses in-memory stores (`ZoryaClusterConfigError`),
because each instance would hold its own copy. The server applies the same
check to its version registry, worker registry, scheduler storage and
namespace registry. The
scheduler elects its own leader through its storage, so give each instance
a distinct `ZoryaScheduler.instanceId`.

### Background services and lifecycle

`listen()` starts the background services (workflows, scheduler, agents,
skills, fragments, retention, and the one-shot namespace adoption) and the
worker WebSocket. A service whose
`start()` fails (storage down at boot, for example) is retried with backoff.
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

`new ZoryaServer(config: ZoryaServerConfig)`, or the same fields through
`ZoryaServer.builder()`: one setter per field, `build()` (or
`listen(options)`), and `buildConfig()` for the plain config. `build()`
throws when `workflows()` is missing; the constructor then applies the key
checks below.

```ts
const server = ZoryaServer.builder()
  .workflows(workflows)
  .scheduler(scheduler)
  .agents(agents)
  .secrets(stack.secretsStorage)
  .apiKeys([process.env.ZORYA_API_KEY!])
  .remoteWorkers({ apiKeys: [process.env.ZORYA_WORKER_API_KEY!] })
  .sampleInput((name) => sampleInputFor(name))
  .uiDir(uiDir)
  .build();
```

| Field                         | Default                 | Meaning                                                                                                                                                                                                                                                                          |
| ----------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workflows`                   | required                | The `ZoryaWorkflows` chain (`LocalWorkflows`, `QueuedWorkflows`). Its storage backs every run route. The version registry, advertisements, definitions and cluster are found on the chain.                                                                                       |
| `apiKeys`                     | none                    | Operator keys for `/api`. Required unless `insecure`.                                                                                                                                                                                                                            |
| `insecure`                    | `false`                 | Allow planes without keys. Local development only.                                                                                                                                                                                                                               |
| `remoteWorkers`               | not mounted             | Mounts the worker plane. `apiKeys`: worker keys. `workerRegistry`: the registry workers register into (default: the queued layer's, else in-memory). `wakeups`: "work available" messages to connected workers (`false` off; `{ coalesceMs, followUpMs }`, defaults 20 and 250). |
| `scheduler`                   | none                    | `ZoryaScheduler`: schedule CRUD, tick loop, emit-now. Without it `GET /api/schedules` answers `configured: false`.                                                                                                                                                               |
| `agents`                      | none                    | `ZoryaAgents`: the agent gateway, plus memory, instances and roles when their stores are set.                                                                                                                                                                                    |
| `skills`, `fragments`, `dags` | none                    | Registry services with CRUD routes.                                                                                                                                                                                                                                              |
| `secrets`                     | none                    | `SecretsStorage` for `/api/secrets` and BYOK credential resolution.                                                                                                                                                                                                              |
| `namespaces`                  | free-form namespaces    | `NamespaceRegistry` for `/api/namespaces`; new runs, schedules and agent state then need a registered, active namespace (see [Namespaces](#namespaces)).                                                                                                                         |
| `webhooks`                    | none                    | `{ sources, deliveries? }`: signed webhook ingress into agents. Mounted only when `agents` is set. `deliveries` (replay dedup) defaults to a store on the workflow storage, shared across instances.                                                                             |
| `remoteDeployments`           | none                    | `{ registry, endpointPolicy?, sweepIntervalMs? }`: other Zorya servers register their agents here. Mounted only when `agents` is set.                                                                                                                                            |
| `retention`                   | keep forever            | `{ maxAgeMs, intervalMs?, batchSize? }`: delete ended runs (completed, failed, tripwire) older than `maxAgeMs` with all their records, on one leader, every `intervalMs` (default 1 h, the first one interval after start), in batches of `batchSize` (default 500).             |
| `metrics`                     | storage-backed          | `MetricsProvider` for `/api/metrics`.                                                                                                                                                                                                                                            |
| `workers`                     | registry-backed         | `WorkersProvider` for the Workers page.                                                                                                                                                                                                                                          |
| `sampleInput`                 | none                    | Sample input for the dashboard trigger form, by workflow name.                                                                                                                                                                                                                   |
| `uiDir`                       | none                    | Directory of the built dashboard. Unset: no UI is served.                                                                                                                                                                                                                        |
| `publicBaseUrl`               | none                    | Public URL used to build signal-token completion URLs.                                                                                                                                                                                                                           |
| `signalTokenMaxTtlMs`         | 90 days                 | Longest validity a signal token may request.                                                                                                                                                                                                                                     |
| `sseIntervalMs`               | 1000                    | Poll interval behind run event streams.                                                                                                                                                                                                                                          |
| `readinessTimeoutMs`          | 2000                    | Bound on the `/api/ready` storage probe.                                                                                                                                                                                                                                         |
| `shutdownTimeoutMs`           | 10000                   | How long `stop()` waits for in-flight requests.                                                                                                                                                                                                                                  |
| `startRetry`                  | 500 ms doubling to 30 s | Backoff for a service whose `start()` failed.                                                                                                                                                                                                                                    |
| `clock`                       | `SystemWallClock`       | Time source for the server's own timers. Tests pass a `FakeWallClock`.                                                                                                                                                                                                           |
| `logger`                      | `console`               | `{ log, warn, error }`.                                                                                                                                                                                                                                                          |

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

### SQLite stack

`createSqliteZoryaStack({ path, secretsPassphrase?, clock? })` from
`@promin/zorya/sqlite` (needs `@promin/sqlite`) opens one Bun SQLite
database (WAL, foreign keys on; `":memory:"` for an ephemeral one) and
returns every store a single-process Zorya uses on it: workflow `storage`
and a `runner` over it, `schedulerStorage`, the worker protocol's
`workflowStarts`, `advertisements` and `workerRegistry`, the
`namespaceRegistry`, the agent
registries and stores, and `secretsStorage` — only when a
`secretsPassphrase` is given; there is no default passphrase. Larger
deployments build stores themselves, or replace some of these (the demo
moves agent state to Postgres with `ZORYA_PG_URL`).

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
| `ZORYA_RETENTION_DAYS`                                                                               | 1                                                   | Ended runs older than this are deleted (`retention.maxAgeMs`).                                                   |
| `ZORYA_RETENTION_INTERVAL_MS`                                                                        | 3600000                                             | Delay between retention sweeps.                                                                                  |
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
| Namespaces   | `GET/POST /api/namespaces` (`?status=active\|archived`), `GET/PATCH/DELETE /api/namespaces/:id`; without a registry only `GET`, with `configured: false`                                                                    |
| Agents       | `/api/agents` CRUD, `/api/agents/_catalog/*`, `_sources`, `_draft`, `/api/agents/:id/{versions,clone,extract-role,invoke,stream,threads,...}`                                                                               |
| Agent data   | `/api/memory/*`, `/api/instances`, `/api/agents/:id/instances/*`, `/api/roles/*`, `/api/skills/*`, `/api/fragments/*`, `/api/secrets/*`, `/api/dags/*`, `/api/remote-deployments/*`                                         |
| Worker plane | `/ws/worker`, `POST /rpc/storage`, `POST /rpc/worker`, `/api/advertisements`, `/api/worker-protocol/{claim-starts,heartbeat-start/:id,complete-start/:id,release-start/:id,starts}`                                         |
| Webhooks     | `POST /webhooks/:source/:agentId`                                                                                                                                                                                           |

The wire types live in `src/server/api-types.ts` and are exported from the
package root.

### Agent knowledge (RAG)

Local agent recipes bind retrievers by id in `backend.knowledge` (see
[Retrieval in `@promin/agent`](../agent/README.md#retrieval-rag)). The host
owns the retrievers: pass a `RetrieverRegistry` as `ZoryaAgents.retrievers`
and the same registry to `resolveLocalAgent({ retrievers })` in `resolve`.

```ts
const retrievers = new InMemoryRetrieverRegistry([
  { id: "docs", retriever: docsRetriever, description: "Product docs", tags: ["public"] },
]);
const agents = new ZoryaAgents({
  registry,
  retrievers,
  resolve: (recipe) => resolveLocalAgent(recipe, { runner, llm, tools, retrievers }),
});
```

`GET /api/agents/_catalog/retrievers` lists the registry's ids, descriptions,
tags and metadata (never the retriever) and answers an empty list without a
registry. The agent editor's **Knowledge / RAG** section adds bindings from
that catalog (or by manual id), sets each binding's mode (`tool`, `context`,
`tool + context`), tool name and description, top K, chunk-size cap and
whether sources and scores are shown, and warns about ids the catalog does not
list. The search tools a binding adds appear among the agent's system tools.
Recipe writes validate `backend.knowledge`: unique ids, a known mode, and a
tool name of letters, digits, `_` or `-`.

### Managed knowledge bases

`ZoryaKnowledgeBases` lets operators create knowledge bases and fill them
from the dashboard's **Knowledge** page. Each base is a retriever registered
under its id in a `RetrieverRegistry`; pass that registry to `ZoryaAgents`
and the resolver so recipes bind the bases by id.

```ts
import { ZoryaKnowledgeBases, createUrlKnowledgeSourceAdapter } from "@promin/zorya";
import { SqliteKnowledgeBaseStore } from "@promin/sqlite";

const knowledgeBases = new ZoryaKnowledgeBases({
  store: SqliteKnowledgeBaseStore.make({ db }), // or PostgresKnowledgeBaseStore
  embeddings, // optional: cosine ranking instead of keyword overlap
  sourceAdapters: [createUrlKnowledgeSourceAdapter()],
  seed: [{ id: "handbook", description: "Org handbook", documents }],
});
const retrievers = knowledgeBases.registry;
const agents = new ZoryaAgents({
  registry,
  retrievers,
  resolve: (recipe) => resolveLocalAgent(recipe, { runner, llm, tools, retrievers }),
});
const server = new ZoryaServer({ workflows, agents, knowledgeBases, apiKeys });
```

The store (`InMemoryKnowledgeBaseStore`, `SqliteKnowledgeBaseStore`,
`PostgresKnowledgeBaseStore`; conformance suite `knowledgeBaseStoreTestSuite`)
keeps each base and every source's text and last ingestion outcome, so a base
survives restarts and the recipes binding it keep resolving. By default each
base is an `InMemoryRetriever` that `start()` rebuilds from the stored text.
For a durable index pass a factory, for example one pgvector table per base:

```ts
new ZoryaKnowledgeBases({
  store,
  retrievers: {
    durable: true, // chunks survive restarts; nothing is re-ingested at start
    create: async ({ base, chunker }) => {
      const retriever = new PgVectorRetriever({
        db,
        embeddings,
        dimensions: 1536,
        chunker,
        tableName: `kb_${base.id.replace(/[^A-Za-z0-9_]/g, "_")}`,
      });
      await retriever.ensureSchema({ createExtension: true });
      return retriever;
    },
  },
});
```

Ingestion (chunk, embed, store) runs inside the request. A failure answers
`502 ingest_failed` with the reason, and the source stays listed with status
`failed` and that reason (the base reports `degraded`) until it is re-indexed
from its stored text or removed. Writes to one base run one at a time. A
source's text is bounded by the server's request body limit and by
`maxSourceCharacters` (default 2,000,000).

Source adapters load documents server-side and are opt-in.
`createUrlKnowledgeSourceAdapter` fetches public http(s) pages only by default
(the remote-deployment `RemoteEndpointPolicy`, checked on every redirect),
caps the body (2 MiB) and the time (15 s), refuses binary content and reduces
HTML to text. `createFileKnowledgeSourceAdapter({ root })` reads text files
under `root` only. Implement `KnowledgeSourceAdapter` for other sources.

| Route                                                          |                                                                                                                                 |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `GET/POST /api/knowledge-bases`                                | list, create (409 when the id is taken, also by a host-wired retriever)                                                         |
| `GET /api/knowledge-bases/_adapters`                           | import kinds the server accepts                                                                                                 |
| `GET/PATCH/DELETE /api/knowledge-bases/:id`                    | read, edit description / tags / metadata, delete (409 `knowledge_base_in_use` while a recipe binds it; `?force=true` overrides) |
| `GET/POST /api/knowledge-bases/:id/sources`                    | list sources (without text), ingest `{ id, text, title?, uri?, mimeType?, tags?, metadata? }`                                   |
| `POST /api/knowledge-bases/:id/sources/import`                 | ingest through an adapter: `{ kind, config }`                                                                                   |
| `POST /api/knowledge-bases/:id/sources/:sourceId/reindex`      | re-ingest a stored source                                                                                                       |
| `DELETE /api/knowledge-bases/:id/sources/:sourceId`            | drop a source and its chunks                                                                                                    |
| `GET /api/knowledge-bases/:id/chunks?sourceId=&limit=&offset=` | chunk inspection                                                                                                                |
| `POST /api/knowledge-bases/:id/search`                         | `{ query, topK?, tags?, metadata?, maxChunkCharacters? }`; 502 `search_failed` when the retriever throws                        |

The service reads the store once at start, so replicas sharing a store see
each other's new bases after a restart; serve the Knowledge page from one
replica, and use a durable retriever so searches agree across replicas.

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
- claims starts of those workflows as soon as Zorya makes them claimable:
  Zorya sends a small "work available" message over the `/ws/worker`
  control socket to about as many connected workers as there are new
  starts (round-robin among those that host the run's workflow and
  version; a burst is coalesced into a few messages), and they claim at
  once. A start still unclaimed 250 ms later (the woken workers were busy)
  wakes every matching worker. A claim takes at most as many starts as the
  worker has free slots and claims again at once while batches come back
  full; a worker with every slot busy ignores the message, since a freed
  slot claims anyway. Polling stays the fallback for a lost message or a
  start queued on another Zorya instance: every 5 s (±20% jitter) while
  the control socket is open, every 1 s while it is not (not yet
  connected, dropped, refused — reported once per outage) or with
  `controlSocket: false`;
- drives each claimed run with the engine against Zorya's storage, fenced
  by the run lock. A start names the run, not its input: the run is driven
  with the input stored on it, so a large input is stored and sent once;
- completes a claim when its run ended or suspended, and releases it when
  the run stopped short, so another worker picks it up;
- retries start-up and every protocol call with backoff, so it can start
  before Zorya and survives Zorya restarts, re-advertising on its own.

Step bodies are at-least-once: a run resumed after a crash re-runs the step
that was in flight. Make them idempotent.

`ZoryaWorker.builder()` builds the same `ZoryaWorkerConfig` fluently, one
setter per field; `.workflow()` and `.workflows(...)` add to the hosted
definitions across calls, and `build()` throws without a `client()` or a
workflow:

```ts
const worker = ZoryaWorker.builder()
  .client(new ZoryaClient({ url, apiKey: process.env.ZORYA_WORKER_API_KEY }))
  .workflows(hello, orderWorkflow)
  .workflow(paymentWorkflow)
  .capabilities(["payments"])
  .concurrency(4)
  .sampleInput(sampleInputFor)
  .build();
```

Typed definitions (`Workflow<{ name: string }, string, MyError>`) are
accepted as they are, here and in `ZoryaWorkerConfig.workflows`; no cast is
needed.

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
  server-builder.ts    ZoryaServerBuilder (ZoryaServer.builder())
  sqlite-stack.ts      createSqliteZoryaStack (the @promin/zorya/sqlite entry)
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
