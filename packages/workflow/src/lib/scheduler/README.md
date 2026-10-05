# Scheduler

`@promin/workflow/scheduler` fires named schedules — cron, iCalendar RRULE or a
fixed interval — as a stream of `ScheduleTick`s that you feed into workflows.

| Implementation      | Persistence                                                | Multi-instance                                             | Delivery                               |
| ------------------- | ---------------------------------------------------------- | ---------------------------------------------------------- | -------------------------------------- |
| `InMemoryScheduler` | none                                                       | no                                                         | best effort, no catch-up               |
| `DurableScheduler`  | a `SchedulerStorage`: in-memory, Postgres, Redis or SQLite | yes: one fenced leader lease per namespace (and partition) | at least once, deduped by `tickNumber` |

Both implement `Scheduler`: `register`, `unregister`, `pause`, `resume`
and `list` return promises (they reject on an invalid config or a storage
error), and `stream(scheduleId?)` / `subscribe()` return a fresh perfect
`Stream<ScheduleTick>` on every call. Stopping the consumer (`take(n)`,
interruption, breaking out of `toAsyncIterable()`) clears pending timers.
All time math — next fire times, `firedAt`, waits, backoff — runs on the
injected `clock` (`WallClock`), so a `FakeWallClock` drives a scheduler in
tests.

## Schedules and ticks

```typescript
import type { DurableScheduleConfig, ScheduleTick } from "@promin/workflow/scheduler";

const nightly: DurableScheduleConfig = {
  id: "nightly-report", // unique id
  name: "Nightly report",
  cron: "0 2 * * *", // exactly one of: cron (5 or 6 fields), rrule, intervalMs
  timezone: "America/New_York", // IANA zone, default UTC
  startAt: new Date("2026-01-01T00:00:00Z"), // optional window
  jitterMs: 30_000, // delay each emission by a random [0, jitterMs)
  maxCatchUp: 3, // durable only: fire at most the 3 newest missed occurrences
  metadata: { team: "analytics" }, // copied onto every tick
};

function describe(tick: ScheduleTick): string {
  // scheduledAt: nominal time; firedAt: actual emission; tickNumber: 0, 1, 2, ... (same on redelivery)
  return `${tick.scheduleId}#${tick.tickNumber} due ${tick.scheduledAt.toISOString()} fired ${tick.firedAt.toISOString()}`;
}
```

`validateScheduleConfig` (used by every `register`) rejects a missing or
double trigger, an invalid cron / RRULE, a non-positive interval and negative
`jitterMs` / `maxCatchUp`.

## InMemoryScheduler

```typescript
import { createWorkflowRunner, InMemoryWorkflowStorage, trigger, workflow } from "@promin/workflow";
import { InMemoryScheduler } from "@promin/workflow/scheduler";
import { succeed } from "@spilne/perfect-core";

const scheduler = new InMemoryScheduler();
await scheduler.register({ id: "health-check", intervalMs: 30_000 });
await scheduler.register({ id: "standup", rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10" });

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });
const check = workflow<{ at: string }>({ name: "health-check" })
  .step("ping", ({ input }) => succeed(`ok at ${input.at}`))
  .build();

await scheduler
  .stream("health-check")
  .take(3)
  .through(
    trigger({
      workflow: check,
      runner,
      storage,
      toInput: (tick) => ({ at: tick.scheduledAt.toISOString() }),
      toWorkflowId: (tick) => `health-check-${tick.tickNumber}`,
      onDuplicate: "skip",
    }),
  )
  .drain()
  .run();

await scheduler.pause("standup");
await scheduler.unregister({ scheduleId: "standup", reason: "cancelled" });
```

A schedule paused, replaced or removed while a stream waits for its next fire
emits nothing for that wait. There is no persistence and no catch-up: ticks
that came due while nothing was consuming are not emitted later.

## DurableScheduler

`DurableScheduler` keeps schedules in a `SchedulerStorage` and polls it:

```typescript
import {
  DurableScheduler,
  InMemorySchedulerStorage,
  scheduleTickRunId,
} from "@promin/workflow/scheduler";

const scheduler = new DurableScheduler({
  storage: new InMemorySchedulerStorage(), // PgSchedulerStorage, RedisSchedulerStorage, SqliteSchedulerStorage
  instanceId: "scheduler-a",
  pollIntervalMs: 1_000,
  namespace: "prod",
  onError: (event) => console.error(event.phase, event.error),
});

await scheduler.register({ id: "daily-etl", cron: "0 2 * * *", maxCatchUp: 3 });

for await (const tick of scheduler.stream().toAsyncIterable()) {
  const runId = scheduleTickRunId({ scheduleId: tick.scheduleId, tickNumber: tick.tickNumber });
  console.log("start", runId); // start the workflow under this id: a redelivery is a no-op
  break;
}

console.log(await scheduler.nextFireTimes({ scheduleId: "daily-etl", count: 5 }));
await scheduler.triggerNow("daily-etl");
await scheduler.backfill({
  scheduleId: "daily-etl",
  from: new Date("2026-03-01"),
  to: new Date("2026-03-03"),
});
await scheduler.update({ scheduleId: "daily-etl", patch: { cron: "0 3 * * *" } });
```

The Postgres and Redis packages ship ready-made facades:
`new DurableScheduler({ db })` from `@promin/postgres` and
`new RedisDurableScheduler({ redis, prefix })` from `@promin/redis`.

### Delivery guarantee: at least once

A poll computes the due ticks **without writing anything**, emits them, and
commits the schedules' fire state (`lastFired`, `tickCount`, `nextRun`) only
after the consumer has pulled past them. A tick counts as acknowledged when
the consumer pulls the next one. If the consumer stops early (`take(n)`,
interruption, a crash), the stream's finalizer commits the schedules whose
ticks were all acknowledged and leaves the rest due, so the next poll —
here or on another instance — emits them again **with the same
`tickNumber`**.

So a tick may be delivered more than once, never skipped while a scheduler
is running. Make the consumer idempotent by deriving the run id from the tick
only: `scheduleTickRunId({ scheduleId, tickNumber })`, or a `toWorkflowId`
built from `scheduleId` and `tickNumber` / `scheduledAt`, with
`trigger({ onDuplicate: "skip" })` or the runner's terminal gate (a
completed run is answered from storage, not run again).

- **Catch-up.** When the scheduler was down across several occurrences, the
  newest `max(1, maxCatchUp)` of them fire, oldest first — for cron, RRULE
  and interval schedules alike.
- **Jitter** pushes each next run back by a random `[0, jitterMs)`; `firedAt`
  is the real emission time.
- **Storage errors never end the stream.** A failed poll is reported through
  `onError({ phase })` and retried with exponential backoff on the clock
  (capped by `maxErrorBackoffMs`, default 30 s); a failed commit is reported
  and its ticks are redelivered; a stored schedule that cannot be evaluated
  (an invalid cron written straight to storage) is reported, disabled and
  skipped while the others keep firing.
- **Paused schedules** have `nextRun = null`, so they never crowd active ones
  out of a poll batch; resuming seeds `nextRun` again.
- **Manual fires.** `triggerNow` and `backfill` take their tick numbers with a
  compare-and-set on `tickCount` and retry on conflict, so a manual fire never
  shares a `tickNumber` with a scheduled one.

### Leader election and fencing

Only the holder of a **leader lease** polls. Every `SchedulerStorage` is a
`LeaderLeaseStore`:

- `tryAcquireLeader({ key, instanceId, ttlMs })` acquires or refreshes the
  lease atomically and returns it (or `null` while another instance holds
  it). A lease carries an `epoch` that increases whenever a new lease starts
  on the key — takeover, expiry, release — and stays the same on refresh.
  Postgres (`wf_leader_leases`, `NOW()`) and Redis (`PX` keys in Lua) measure
  the TTL on the server clock; in-memory and SQLite on the injected
  `WallClock`. The TTL is `leaderLockTtlMs` (default 3 × `pollIntervalMs`).
- `releaseLeader({ lease })` gives it up. The scheduler releases when its
  last stream stops, so another instance takes over at its next poll instead
  of after the TTL.
- `commitPoll({ updates, lease })` is **fenced**: in the same transaction (or
  Lua script) as the writes, it checks that the lease's epoch is still
  current, and otherwise writes nothing and throws `StaleLeaseError`. Each
  entry also carries `expectedTickCount` (compare-and-set); entries that lost
  come back as `conflicts`. A leader that paused past its TTL may still emit
  the ticks it had planned — the new leader redelivers them under the same
  `tickNumber` — but it can never commit stale fire state over the new
  leader's, so tick numbers never regress or skip.

The lease API is reusable by other leader-elected loops (the distributed
coordinator and scanners use it): `InMemoryLeaderLeases`,
`PgLeaderLeaseStore` / `assertPgLeaseCurrent` (`@promin/postgres`),
`RedisLeaderLeaseStore` (`@promin/redis`), `SqliteLeaderLeaseStore`
(`@promin/sqlite`), and `LeaseLeaderElection`, which wraps a store and key as
a `tryAcquire()` / `release()` election exposing the current lease for
fencing.

```typescript
import {
  InMemoryLeaderLeases,
  LeaseLeaderElection,
  schedulerLeaderKey,
} from "@promin/workflow/scheduler";

const leases = new InMemoryLeaderLeases();
const election = new LeaseLeaderElection({
  store: leases,
  key: schedulerLeaderKey({ namespace: "prod" }),
  instanceId: "worker-1",
  ttlMs: 5_000,
});
if (await election.tryAcquire()) {
  console.log("leading with epoch", election.lease?.epoch);
  await election.release();
}
```

### Namespaces and partitions

- `namespace` scopes a scheduler instance: `findDue`, `list` and the lease
  are per namespace, so tenants poll independently.
- `partition: { index, count }` splits one namespace's schedules by
  `schedulePartition({ id, count })`. Each partition elects its own leader
  (one lease per namespace and partition), so N partitions fire in parallel;
  run one or more instances per partition. Run **every** partition: a
  partition with no live instance leaves its due schedules at the head of the
  due index, and a large enough backlog crowds the other partitions out of
  each poll (`findDue` fetches `batchSize × count` ids before filtering).

### Storage backends

| Storage                    | Package            | Lease TTL clock  | Notes                                                                     |
| -------------------------- | ------------------ | ---------------- | ------------------------------------------------------------------------- |
| `InMemorySchedulerStorage` | `@promin/workflow` | `WallClock`      | single process                                                            |
| `PgSchedulerStorage`       | `@promin/postgres` | server (`NOW()`) | fenced `commitPoll` checks the epoch under `FOR SHARE` in one transaction |
| `RedisSchedulerStorage`    | `@promin/redis`    | server (`PX`)    | keys tagged `{<prefix>}` (one Cluster slot); one Lua script per commit    |
| `SqliteSchedulerStorage`   | `@promin/sqlite`   | `WallClock`      | single-statement lease CAS; fence checked inside the commit transaction   |

All of them run `schedulerStorageTestSuite` from `@promin/workflow/testing`.
