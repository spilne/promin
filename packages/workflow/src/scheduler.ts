// ---------------------------------------------------------------------------
// @promin/workflow/scheduler — cron, RRULE and interval schedules.
//
// `InMemoryScheduler` runs schedules in-process; `DurableScheduler` keeps
// them in a `SchedulerStorage` with catch-up, leader leases and
// at-least-once tick delivery. Both emit `ScheduleTick`s that `trigger()`
// turns into workflow runs.
// ---------------------------------------------------------------------------

export {
  type ScheduleConfig,
  type DurableScheduleConfig,
  type ScheduleTick,
  scheduleTickRunId,
} from "./lib/scheduler/types.ts";
export type { Scheduler } from "./lib/scheduler/scheduler.ts";
export {
  InMemoryScheduler,
  type InMemorySchedulerConfig,
} from "./lib/scheduler/in-memory-scheduler.ts";
export {
  type SchedulerStorage,
  type ScheduleCommit,
  type CommitPollResult,
  isTickLogStorage,
} from "./lib/scheduler/scheduler-storage.ts";
export { InMemorySchedulerStorage } from "./lib/scheduler/in-memory-scheduler-storage.ts";
export {
  type LeaderLease,
  type LeaderLeaseStore,
  type InMemoryLeaderLeasesConfig,
  InMemoryLeaderLeases,
  LeaseLeaderElection,
  StaleLeaseError,
  isStaleLeaseError,
  schedulerLeaderKey,
} from "./lib/scheduler/leader-lease.ts";
export {
  DurableScheduler,
  type DurableSchedulerConfig,
  type SchedulerErrorEvent,
  type SchedulerErrorPhase,
  type PlannedSchedule,
  computeDueTicks,
  computeNextRun,
  planDueTicks,
  commitPlannedSchedules,
  schedulePartition,
} from "./lib/scheduler/durable-scheduler.ts";
export { validateScheduleConfig } from "./lib/scheduler/schedule-config.ts";
