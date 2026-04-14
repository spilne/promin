# Scheduler

Cron, RRULE, and interval scheduling for workflows. Non-blocking — uses `Effect.sleep()` to yield the fiber between ticks.

## Main Idea

A scheduler manages named schedules and emits `ScheduleTick` events. Each tick contains the schedule ID, nominal fire time, actual fire time, and a monotonic tick number. The scheduler implements `Streamable<ScheduleTick>`, so it plugs directly into StreamPipeline and the `trigger()` combinator.

Two implementations:

| Implementation      | Package            | Persistence | Multi-instance                               |
| ------------------- | ------------------ | ----------- | -------------------------------------------- |
| `InMemoryScheduler` | `@promin/workflow` | None        | No                                           |
| `DurableScheduler`  | `@promin/postgres` | Postgres    | Yes (leader election via `pg_advisory_lock`) |

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
  firedAt: Date; // Actual fire time (may differ due to jitter/load)
  tickNumber: number; // Monotonic counter (0, 1, 2, ...)
  metadata?: Record<string, unknown>;
}
```

## InMemoryScheduler

Non-blocking, in-process scheduler. No persistence, no multi-instance coordination. Good for development, single-process services, and tests.

```typescript
import { createScheduler } from "@promin/workflow";

const scheduler = createScheduler();

// Cron — every weekday at 9am EST
scheduler.register({
  id: "morning-report",
  cron: "0 9 * * MON-FRI",
  timezone: "America/New_York",
  metadata: { team: "analytics" },
});

// Fixed interval — every 30 seconds
scheduler.register({ id: "health-check", intervalMs: 30_000 });

// RRULE — biweekly on Tuesday at 10am
scheduler.register({
  id: "standup",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
});
```

### Streaming ticks

```typescript
import { trigger } from "@promin/workflow";
import { StreamPipeline } from "@promin/core";

// Stream a single schedule into a workflow trigger
scheduler
  .stream("morning-report")
  .through(
    trigger({
      workflow: reportWorkflow,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `report-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain();

// Stream all schedules merged
StreamPipeline.fromSource(scheduler).forEach((tick) =>
  console.log(`${tick.scheduleId} fired at ${tick.firedAt}`),
);
```

### Runtime control

```typescript
scheduler.pause("health-check"); // Stops emitting, keeps config
scheduler.resume("health-check"); // Resumes emitting
scheduler.unregister("health-check"); // Removes entirely, stream ends
scheduler.list(); // All registered ScheduleConfigs
```

## Durable Scheduler (Postgres)

For production multi-instance deployments, use `DurableScheduler` from `@promin/postgres`. It adds:

- **Persistent schedules** stored in Postgres
- **Catch-up** for missed runs (e.g., server was down)
- **Leader election** via `pg_advisory_lock` so only one instance fires
- **Jitter** to spread load across time
- **Backfill** to generate ticks for past time ranges
- **Overlap policies** to control concurrent schedule executions

```typescript
import { createDurableScheduler, migrate } from "@promin/postgres";

await migrate(db);
const scheduler = createDurableScheduler({ db });

// Register persistent schedule
await scheduler.registerAsync({
  id: "daily-etl",
  cron: "0 2 * * *",
  timezone: "America/New_York",
  maxCatchUp: 3,
  jitterMs: 30_000,
});

// Trigger workflow from schedule
scheduler
  .stream("daily-etl")
  .through(
    trigger({
      workflow: etlWorkflow,
      toInput: (tick) => ({ date: tick.scheduledAt.toISOString().split("T")[0] }),
      toWorkflowId: (tick) => `etl-${tick.scheduledAt.toISOString().split("T")[0]}`,
    }),
  )
  .drain();

// Management
const next5 = await scheduler.nextFireTimes("daily-etl", 5);
await scheduler.triggerNow("daily-etl");
await scheduler.backfill("daily-etl", {
  from: new Date("2026-03-01"),
  to: new Date("2026-03-20"),
});
```
