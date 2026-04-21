# Order fulfillment — saga compensation + signals

Distributed order workflow with saga-style compensation, signal-driven
suspension, and per-capability worker pools.

## What it demonstrates

- **Saga compensation** — `reserve-inventory` and `charge-payment` each
  declare `compensate` hooks. If a later step fails after retries, the
  runner cascades compensation bottom-up: the payment gets refunded and
  the inventory reservation is released.
- **Retry with backoff** — `charge-payment` fails its first attempt,
  succeeds the second; the step's `retry: { maxRetries: 3, baseDelayMs: 500 }`
  handles the transient gateway error without surfacing it.
- **Signal-driven suspension** — `wait-for-shipped` suspends the workflow
  until the warehouse (here, the coordinator itself for demo purposes)
  posts a `shipped` signal via `storage.deliverSignal`.
- **Per-capability worker routing** — each step declares `needs` and
  workers declare matching `capabilities`. No routing table; the
  step queue's `needs ⊆ capabilities` filter does the matching.
- **Scaling independently** — bump `worker-payment` to 5 replicas when
  the payment gateway is the bottleneck without touching other pools.

## DAG

```
reserve-inventory (needs=inventory)
        │                            compensate → release stock
        ▼
charge-payment    (needs=payment)    ← flaky, retries
        │                            compensate → refund
        ▼
generate-label    (needs=shipping)
        │
        ▼
notify-warehouse  (default)
        │
        ▼
wait-for-shipped  (signal)           ← suspends ~3s for demo
        │
        ▼
mark-delivered    (default)
```

## Run

```bash
docker compose up --build
```

New demo order submitted every 6s; a fake "shipped" signal posts 3s
after each order. You'll see logs like:

```
[coordinator] submitted demo-order-1-abc123 (#1)
[inventory]   demo-order-1-abc123 — reserving 2 line(s) for demo-order-1
[payment]     demo-order-1-abc123 — charging 4999¢ (attempt 1)
[payment]     demo-order-1-abc123 — charging 4999¢ (attempt 2)   ← retry succeeded
[shipping]    demo-order-1-abc123 — generating label for demo-order-1
[notify]      demo-order-1-abc123 — warehouse notified for demo-order-1
[coordinator] signal shipped → demo-order-1-abc123                ← workflow resumes
[delivered]   demo-order-1-abc123 — order demo-order-1 shipped via 1ZDEMOORDER1
```

Scale a pool:

```bash
docker compose up --scale worker-payment=3 --scale worker-shipping=2
```

Follow a single order end-to-end:

```bash
docker compose logs -f | grep demo-order-3
```

## Trying the compensation path

To watch saga rollback, edit `workflow.ts` so `chargePaymentHandler`
always throws (remove the `if (ctx.attempt === 1)` guard). With
`maxRetries: 3` the charge will exhaust retries, and the runner will
fire `reserve-inventory`'s `compensate` hook (you'll see
`[compensate] releasing reservation res-demo-order-N-...` in the logs)
before marking the workflow failed.

## Environment tuning

| Variable             | Default                                         | Purpose                                      |
| -------------------- | ----------------------------------------------- | -------------------------------------------- |
| `DATABASE_URL`       | `postgres://promin:promin@postgres:5432/promin` | Postgres connection                          |
| `SUBMIT_INTERVAL_MS` | `6000`                                          | Demo-order submission cadence                |
| `SHIP_AFTER_MS`      | `3000`                                          | Delay before the coordinator posts `shipped` |

## Files

- `workflow.ts` — DAG definition + step handlers (shared by coordinator + workers)
- `coordinator.ts` — coordinator loop + demo-order submitter + signal delivery
- `worker-inventory.ts` / `worker-payment.ts` / `worker-shipping.ts` / `worker-default.ts`
- `shared.ts` — Postgres bootstrap
- `migrator.ts` — one-shot Drizzle migrations
- `docker-compose.yml` — full stack
- `Dockerfile` — shared image for all services
