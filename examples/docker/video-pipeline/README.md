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

Requires Docker Compose v2. The coordinator submits a fresh demo workflow
every 5s so `docker compose up` is enough — no client step.

```bash
# Bring the cluster up + tail the pipeline
make up && make logs

# Scale a worker pool
make scale-gpu N=3

# Tear down
make down
```

Raw commands:

```bash
docker compose up --build             # attach and watch logs
docker compose up -d --scale worker-gpu=3
docker compose down -v
```

Tune the submission rate with `SUBMIT_INTERVAL_MS` on the coordinator
service (env var, milliseconds).

## What each service does

| Service       | Role                                                                    |
| ------------- | ----------------------------------------------------------------------- |
| `postgres`    | Workflow state + step queue (via `@promin/postgres`)                    |
| `migrator`    | One-shot: runs drizzle migrations, exits 0                              |
| `coordinator` | Routes ready steps, tracks completions, submits demo workflows every 5s |
| `worker-gpu`  | Registers `transcode`, claims from `"gpu"` queue                        |
| `worker-cpu`  | Registers `thumbnail`, claims from `"cpu"` queue                        |
| `worker-def`  | Registers `decode` / `metadata` / `notify`, claims `default`            |

## Files

| File                  | Purpose                                                     |
| --------------------- | ----------------------------------------------------------- |
| `workflow.ts`         | Workflow DAG + per-step handlers (exported)                 |
| `shared.ts`           | Postgres + storage + step queue factory (env-driven)        |
| `migrator.ts`         | Runs migrations and exits                                   |
| `coordinator.ts`      | Scheduler + timer-driven submitter (one process, one coord) |
| `worker-transcode.ts` | GPU worker — `transcode`                                    |
| `worker-thumbnail.ts` | CPU worker — `thumbnail`                                    |
| `worker-default.ts`   | Default worker — `decode` / `metadata` / `notify`           |
| `docker-compose.yml`  | Service definitions                                         |
| `Dockerfile`          | Shared image — all services run from it with different CMDs |

## Swapping the stubs for real logic

Step handlers live in `workflow.ts` (`decodeHandler`, `transcodeHandler`,
etc). Each handler is a `StepHandler` — `(ctx) => Pipeline`. Replace the
`sleep()` calls with real ffmpeg/S3/DB work and the wiring stays the same;
workers will just take longer to finish each task. A real GPU worker would
mount `/dev/dri` (or `--gpus all` on nvidia-docker) onto `worker-gpu` and
call `ffmpeg -hwaccel ...` inside `transcodeHandler`.

## A note on the "one coordinator" pattern

An earlier version of this demo shipped a separate `submit.ts` container
that called `.submit()` via its own throwaway `createCoordinator`. End-to-
end testing surfaced a race: that throwaway coord enqueued the first
step AND the long-running coord, on its next tick, discovered the new
workflow and ran `enqueueReady` against its own (empty) in-memory
dedupe set — so `decode` got enqueued twice. Workflow correctness was
preserved, but one worker ran `decode` twice.

The fix in this demo is to submit from the same coord instance that
runs the scheduler. Timer inside `coordinator.start()`, single
`enqueued: Set` backing dedupe — no race.

In production either use the same pattern (coordinator also exposes an
HTTP submit endpoint on the leader) or rely on an idempotent
`stepQueue.enqueue` at the library level.

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
