# Scheduler

Cron, RRULE, and interval scheduling for workflows. Non-blocking — sleeps on a fiber between ticks.

## Main Idea

A scheduler manages named schedules and emits `ScheduleTick` events. Each tick contains the schedule ID, nominal fire time, actual fire time, and a monotonic tick number. The scheduler implements `Streamable<ScheduleTick>`: `stream()` and `subscribe()` return a perfect `Stream`, so ticks plug directly into stream operators and the `trigger()` pipe.

Perfect streams are single-use, so every `stream()` / `subscribe()` call builds a fresh one. Stopping the consumer (`take(n)`, `interruptAfter`, breaking out of a `for await` over `toAsyncIterable()`) cancels pending timers and removes listeners.

Management methods (`register`, `unregister`, `pause`, `resume`, `list`) are async on every implementation: they resolve once the change is applied and reject on an invalid config or a storage error.

Two implementations:

| Implementation      | Package            | Persistence                                                        | Multi-instance                                        |
| ------------------- | ------------------ | ------------------------------------------------------------------ | ----------------------------------------------------- |
| `InMemoryScheduler` | `@promin/workflow` | None                                                               | No                                                    |
| `DurableScheduler`  | `@promin/workflow` | Pluggable (`SchedulerStorage`): in-memory, Postgres, Redis, SQLite | Yes (fenced leader lease per namespace and partition) |

## ScheduleConfig

Each schedule requires exactly one trigger type:

```typescript
interface ScheduleConfig {
  id: string; // Unique identifier
  name?: string; // Human-readable name
  cron?: string; // Cron expression (5 or 6 field)
  rrule?: string; // iCalendar RRULE (RFC 5545)
  intervalMs?: number; // Fixed interval in milliseconds
  timezone?: string; // IANA timezone (default: "UTC")
  enabled?: boolean; // Active state (default: true)
  startAt?: Date; // Don't fire before this time
  endAt?: Date; // Stop firing after this time
  jitterMs?: number; // Random [0, jitterMs) delay before each tick is emitted
  metadata?: Record<string, unknown>; // Passed through to ScheduleTick
}
```

## ScheduleTick

Emitted when a schedule fires:

```typescript
interface ScheduleTick {
  scheduleId: string; // Which schedule fired
  scheduleName?: string; // Human-readable name
  scheduledAt: Date; // Nominal fire time (cron-computed)
  firedAt: Date; // When it was emitted (later than scheduledAt under jitter/load)
  tickNumber: number; // Monotonic counter (0, 1, 2, ...); same on redelivery
  metadata?: Record<string, unknown>;
}
```

## InMemoryScheduler

Non-blocking, in-process scheduler. No persistence, no multi-instance coordination. Good for development, single-process services, and tests.

```typescript
import { InMemoryScheduler } from "@promin/workflow/scheduler";

const scheduler = new InMemoryScheduler();

// Cron — every weekday at 9am EST
await scheduler.register({
  id: "morning-report",
  cron: "0 9 * * MON-FRI",
  timezone: "America/New_York",
  metadata: { team: "analytics" },
});

// Fixed interval — every 30 seconds
await scheduler.register({ id: "health-check", intervalMs: 30_000 });

// RRULE — biweekly on Tuesday at 10am
await scheduler.register({
  id: "standup",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
});
```

### Streaming ticks

```typescript
import { trigger } from "@promin/workflow";

// Stream a single schedule into a workflow trigger
await scheduler
  .stream("morning-report")
  .through(
    trigger({
      workflow: reportWorkflow,
      runner,
      storage,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `report-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain()
  .run();

// Stream all schedules merged, including schedules registered later
for await (const tick of scheduler.subscribe().toAsyncIterable()) {
  console.log(`${tick.scheduleId} fired at ${tick.firedAt}`);
}
```

### Runtime control

```typescript
await scheduler.pause("health-check"); // Stops emitting, keeps config
await scheduler.resume("health-check"); // Resumes emitting
await scheduler.unregister({ scheduleId: "health-check" }); // Removes entirely, stream ends
await scheduler.list(); // All registered ScheduleConfigs
```

A schedule paused, replaced or removed while a stream waits for its next fire time emits nothing for that wait. The in-memory scheduler has no persistence and no catch-up.

## Durable Scheduler

`DurableScheduler` polls a `SchedulerStorage`. It adds:

- **Persistent schedules** in the storage backend
- **Catch-up** for missed runs: when more than one occurrence was missed (the scheduler was down), the newest `max(1, maxCatchUp)` fire, oldest first — for cron, RRULE and interval schedules alike
- **Leader election** with fenced leases per namespace (and per partition) so only one instance fires and a stale leader can't commit
- **Jitter** (`jitterMs`) delays each next run by a random `[0, jitterMs)`, spreading schedules that share a boundary
- **Backfill** to generate ticks for past time ranges

### Delivery guarantee: at least once

Each poll computes the due ticks, emits them, and commits the fire state only after the consumer has pulled past them; then it waits `pollIntervalMs` and polls again. A tick is acknowledged when the consumer pulls the next one. If the consumer stops early (`take(n)`, interruption, crash), schedules whose ticks were all acknowledged are committed and the rest stay due: the next poll emits them again with the **same `tickNumber`**. Derive run ids with `scheduleTickRunId({ scheduleId: tick.scheduleId, tickNumber: tick.tickNumber })` (or another id derived only from the tick, like `toWorkflowId` below) so a redelivered tick is a no-op. Zorya's scheduler loop gives the same guarantee: it dispatches a poll's ticks, then commits.

Storage errors never end the stream. A failed poll is reported through `onError` and retried with exponential backoff (on the injected clock, capped by `maxErrorBackoffMs`); a failed commit is reported and its ticks are redelivered; a stored schedule that can't be evaluated (say, an invalid cron written straight to storage) is reported, disabled and skipped while the others keep firing. Paused schedules leave due-tracking (`nextRun = null`), so they never crowd active ones out of a poll batch.

### Leader election and fencing

Only the holder of a **leader lease** polls. There is one lease per namespace and, for a partitioned scheduler, per partition (`schedulerLeaderKey`), so the partitions of a namespace fire in parallel. Every `SchedulerStorage` is a `LeaderLeaseStore`:

- `tryAcquireLeader({ key, instanceId, ttlMs })` acquires or refreshes the lease in one atomic step and returns it (or `null` while another instance holds it). Each lease carries an `epoch` that goes up whenever a new lease starts on the key; a refresh keeps it. Postgres (`wf_leader_leases`, migration `0049`) and Redis (Lua) measure the TTL on the server clock; in-memory and SQLite use the injected `WallClock`.
- `releaseLeader({ lease })` gives it up. The scheduler releases when its last stream stops, so another instance takes over at its next poll instead of after the TTL.
- `commitPoll({ updates, lease })` is **fenced**: it writes nothing and throws `StaleLeaseError` unless the lease's epoch is still current, checked in the same transaction (or Lua script) as the writes. A leader that paused past its TTL can still emit the ticks it had planned (they are redelivered under the same `tickNumber` by the new leader), but it can't commit stale fire state over the new leader's.

Entries also carry a compare-and-set guard (`expectedTickCount`): a poll never commits over a fire that took its tick numbers in the meantime. `triggerNow` and `backfill` take their numbers the same way, so a manual fire never shares a `tickNumber` with another fire.

The lease API is exported for other leader-elected loops: `PgLeaderLeaseStore` / `assertPgLeaseCurrent` (`@promin/postgres`), `RedisLeaderLeaseStore` (`@promin/redis`), `SqliteLeaderLeaseStore` (`@promin/sqlite`), `InMemoryLeaderLeases`, and `LeaseLeaderElection`, which wraps a store and key as a `tryAcquire()` / `release()` election that also exposes the current lease for fencing.

### Postgres

```typescript
import { createDurableScheduler, migrate } from "@promin/postgres";

await migrate(db);
const scheduler = createDurableScheduler({ db });

// Register persistent schedule
await scheduler.register({
  id: "daily-etl",
  cron: "0 2 * * *",
  timezone: "America/New_York",
  maxCatchUp: 3,
  jitterMs: 30_000,
});

// Trigger workflow from schedule
await scheduler
  .stream("daily-etl")
  .through(
    trigger({
      workflow: etlWorkflow,
      runner,
      storage,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `etl-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain()
  .run();

// Management
const next5 = await scheduler.nextFireTimes({ scheduleId: "daily-etl", count: 5 });
await scheduler.triggerNow("daily-etl");
await scheduler.backfill({
  scheduleId: "daily-etl",
  from: new Date("2026-03-01"),
  to: new Date("2026-03-20"),
});
```
