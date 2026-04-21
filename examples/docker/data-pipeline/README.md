# Data pipeline — parallel fan-out/fan-in + singleflight dedup

Batch data pipeline that ingests a dataset, fans out to three parallel
transform shards, reduces their results, and publishes a report.

## What it demonstrates

- **Parallel fan-out/fan-in** — three `transform-shard-*` steps all depend
  on `ingest-dataset` and have no dependency on each other, so the
  coordinator dispatches them simultaneously. `reduce-results` declares
  `dependsOn: ["transform-shard-a", "transform-shard-b", "transform-shard-c"]`
  and runs only when all three complete.
- **Singleflight dedup** — the workflow is built with
  `idempotency: { ttl: 5 * 60_000, onInFlight: "join" }`. A second
  submission with the same `workflowId` within the TTL is a no-op — the
  existing run continues and any waiter joins it. The coordinator
  demonstrates this by checking `coordinator.status()` immediately after
  submit and logging "already in flight — duplicate skipped".
- **Independent scaling** — bump `worker-transform` to 3 replicas and
  all three shards can execute on separate containers simultaneously.
- **Per-capability routing** — `ingest-dataset` routes to `worker-ingest`,
  the three shards route to `worker-transform`, `reduce-results` routes
  to `worker-loader`, and `publish-report` runs on `worker-default`.

## DAG

```
ingest-dataset    (needs=ingest)
        │
   ┌────┼────┐
   ▼    ▼    ▼
transform-shard-a  transform-shard-b  transform-shard-c   ← run in parallel
      (needs=transform)
   └────┬────┘
        ▼
reduce-results    (needs=loader)      ← waits for all 3 shards
        │
        ▼
publish-report    (default)
```

## Run

```bash
docker compose up --build
```

A new dataset submits every 8s. You'll see the three shards fire in
parallel and the reducer wait for all three:

```
[coordinator]       submitted dataset-pipeline-1 (#1)
[coordinator]       dataset-pipeline-1 already in flight — duplicate skipped
[ingest]            dataset-pipeline-1 — reading 30 records from s3://raw-data/demo-dataset-1.parquet
[transform-a]       dataset-pipeline-1 — processing 10/30 records
[transform-b]       dataset-pipeline-1 — processing 10/30 records
[transform-c]       dataset-pipeline-1 — processing 10/30 records
[reduce]            dataset-pipeline-1 — merging 3 shards, 30 rows, sum=1427
[publish]           dataset-pipeline-1 — report for demo-dataset-1: 30 rows → s3://reports/...
```

Scale the transform pool to 3 replicas (one per shard):

```bash
docker compose up --scale worker-transform=3
```

Watch a single dataset end-to-end:

```bash
docker compose logs -f | grep dataset-pipeline-3
```

## Environment tuning

| Variable             | Default                                         | Purpose                         |
| -------------------- | ----------------------------------------------- | ------------------------------- |
| `DATABASE_URL`       | `postgres://promin:promin@postgres:5432/promin` | Postgres connection             |
| `SUBMIT_INTERVAL_MS` | `8000`                                          | Demo-dataset submission cadence |

## Files

- `workflow.ts` — DAG definition + step handlers (shared by all services)
- `coordinator.ts` — coordinator loop + demo-dataset submitter
- `worker-ingest.ts` / `worker-transform.ts` / `worker-loader.ts` / `worker-default.ts`
- `shared.ts` — Postgres bootstrap
- `migrator.ts` — one-shot Drizzle migrations
- `docker-compose.yml` — full stack
- `Dockerfile` — shared image for all services
