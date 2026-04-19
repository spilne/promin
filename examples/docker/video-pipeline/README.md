# Video Pipeline — Distributed Workflow Demo

Shows how promin runs a single workflow across specialized worker pools
routed by step name. Each step goes to the worker class that can run it —
GPU for transcode, CPU for thumbnails, default for everything else — and
the coordinator orchestrates the whole DAG across Postgres.

## The DAG

```
               ┌───────────┐
               │  decode   │ ← default queue
               └─────┬─────┘
                     │
           ┌─────────┴─────────┐
           ▼                   ▼
    ┌─────────────┐      ┌───────────┐
    │  transcode  │ gpu  │ metadata  │ default
    └──────┬──────┘      └─────┬─────┘
           ▼                   │
    ┌─────────────┐            │
    │  thumbnail  │ cpu        │
    └──────┬──────┘            │
           │                   │
           └──────┐    ┌───────┘
                  ▼    ▼
               ┌──────────┐
               │  notify  │ default
               └──────────┘
```

- `decode` and `metadata` run in parallel after `decode` completes.
- `transcode` and `metadata` run in parallel — no dependency between them.
- `thumbnail` waits for `transcode`; `notify` joins both branches.

## Running locally

Requires Docker Compose v2.

```bash
# Bring the cluster up
make up

# Tail the workers
make logs

# Submit a job (blocks until complete, prints the result)
make submit VIDEO=my-video-123

# Scale a worker pool
make scale-gpu N=3

# Tear down
make down
```

Or the raw commands:

```bash
docker compose up --build -d
docker compose run --rm submit my-video-123
docker compose up -d --scale worker-gpu=3
docker compose down -v
```

## What each service does

| Service       | Role                                                         |
| ------------- | ------------------------------------------------------------ |
| `postgres`    | Workflow state + step queue (via `@promin/postgres`)         |
| `migrator`    | One-shot: runs drizzle migrations, exits 0                   |
| `coordinator` | Routes ready steps to queues, tracks completions             |
| `worker-gpu`  | Registers `transcode`, claims from `"gpu"` queue             |
| `worker-cpu`  | Registers `thumbnail`, claims from `"cpu"` queue             |
| `worker-def`  | Registers `decode` / `metadata` / `notify`, claims `default` |
| `submit`      | On-demand CLI — `docker compose run --rm submit <id>`        |

## Files

| File                  | Purpose                                                     |
| --------------------- | ----------------------------------------------------------- |
| `workflow.ts`         | Workflow DAG + per-step handlers (exported)                 |
| `shared.ts`           | Postgres + storage + step queue factory (env-driven)        |
| `migrator.ts`         | Runs migrations and exits                                   |
| `coordinator.ts`      | Coordinator entry point                                     |
| `worker-transcode.ts` | GPU worker — `transcode`                                    |
| `worker-thumbnail.ts` | CPU worker — `thumbnail`                                    |
| `worker-default.ts`   | Default worker — `decode` / `metadata` / `notify`           |
| `submit.ts`           | CLI that submits a workflow and polls for the result        |
| `docker-compose.yml`  | Service definitions                                         |
| `Dockerfile`          | Shared image — all services run from it with different CMDs |

## Swapping the stubs for real logic

Step handlers live in `workflow.ts` (`decodeHandler`, `transcodeHandler`,
etc). Each handler is a `StepHandler` — `(ctx) => Pipeline`. Replace the
`sleep()` calls with real ffmpeg/S3/DB work and the wiring stays the same;
workers will just take longer to finish each task. A real GPU worker would
mount `/dev/dri` (or `--gpus all` on nvidia-docker) onto `worker-gpu` and
call `ffmpeg -hwaccel ...` inside `transcodeHandler`.

## Known quirk — first step runs twice

`submit.ts` spins up a throwaway `createCoordinator` instance just to
call `.submit()`, which registers the DAG in storage AND enqueues the
first step. The long-running `coordinator.ts` process then also runs its
own `enqueueReady` tick against the newly-appearing workflow, so the
first step (`decode`) gets enqueued by both sides. Two tasks land in
the step queue, a worker claims both, and the log shows two `[decode]`
lines. Later steps stay single — once `decode`'s status is `running`/
`completed` in storage, neither coordinator enqueues it again.

Workflow correctness is unaffected (final result is right; compensation
and retries behave the same), but throughput-sensitive deployments
should submit via a single coordinator instance rather than running a
local submit + a cluster coordinator side-by-side. Fixing this in the
library would mean making `stepQueue.enqueue` idempotent on
`(workflowId, stepName, run)` — tracked separately.

## What it demonstrates

- **Multi-queue routing** — `transcode` always lands on `worker-gpu`,
  `thumbnail` on `worker-cpu`. The registry on each worker ONLY has its
  queue's handlers, so if a task ever landed on the wrong queue it would
  stay claimable by the right worker (fail-safe, not fail-fast).
- **Parallel fan-out** — `metadata` runs in parallel with the transcode →
  thumbnail branch. The coordinator enqueues both as soon as `decode`
  completes.
- **DAG join** — `notify` waits for both `thumbnail` AND `metadata` before
  running. Step dependencies are expressed by `dependsOn` in the workflow
  definition.
- **Independent worker scaling** — `make scale-gpu N=3` spins up more
  transcode capacity without touching other workers.
- **Postgres as the single source of truth** — every service is stateless;
  kill and restart any one and it reconnects and catches up.
