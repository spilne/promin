/**
 * Scheduler — cron, intervals, and streaming ticks
 *
 * InMemoryScheduler is non-blocking — uses Effect.sleep to yield
 * the fiber until the next fire time. No polling, no setInterval.
 *
 * For production, use DurableScheduler from @promin/postgres which adds
 * persistence, catch-up, overlap policies, and leader election.
 */

import { createScheduler } from "@promin/core";

// Register cron and interval schedules
function registerSchedules() {
  const scheduler = createScheduler();

  // Cron — every weekday at 9am EST
  scheduler.register({
    id: "morning-report",
    cron: "0 9 * * MON-FRI",
    timezone: "America/New_York",
    metadata: { team: "analytics" },
  });

  // Fixed interval — every 30 seconds
  scheduler.register({
    id: "health-check",
    intervalMs: 30_000,
    metadata: { type: "heartbeat" },
  });

  // Every minute
  scheduler.register({
    id: "metrics-collect",
    cron: "* * * * *",
    name: "Metrics Collector",
  });

  // List all schedules
  console.log("Schedules:", scheduler.list().map((s) => s.id));

  // Pause / resume
  scheduler.pause("health-check");
  scheduler.resume("health-check");

  // Unregister
  scheduler.unregister("metrics-collect");

  return scheduler;
}

// Stream ticks from a specific schedule
async function streamSingleSchedule() {
  const scheduler = createScheduler();
  scheduler.register({ id: "fast", intervalMs: 100 });

  // Take first 3 ticks
  const ticks = await scheduler.stream("fast").take(3).collect();

  for (const tick of ticks) {
    console.log(`Tick ${tick.tickNumber}: scheduled=${tick.scheduledAt}, fired=${tick.firedAt}`);
  }
}

// Stream all schedules merged
async function streamAllSchedules() {
  const scheduler = createScheduler();
  scheduler.register({ id: "a", intervalMs: 100 });
  scheduler.register({ id: "b", intervalMs: 200 });

  const ticks = await scheduler.stream().take(5).collect();
  console.log("Merged ticks:", ticks.map((t) => `${t.scheduleId}#${t.tickNumber}`));
}

// RRULE — complex calendar recurrence (biweekly, quarterly, etc.)
function rruleSchedules() {
  const scheduler = createScheduler();

  // Biweekly on Tuesday at 10am
  scheduler.register({
    id: "biweekly-standup",
    rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
    metadata: { type: "standup" },
  });

  // Quarterly on the 1st at 9am
  scheduler.register({
    id: "quarterly-review",
    rrule: "FREQ=MONTHLY;INTERVAL=3;BYMONTHDAY=1;BYHOUR=9",
  });

  // Every second Monday at 9:30
  scheduler.register({
    id: "sprint-planning",
    rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=MO;BYHOUR=9;BYMINUTE=30",
  });

  console.log("RRULE schedules:", scheduler.list().map((s) => s.id));
  return scheduler;
}

export { registerSchedules, streamSingleSchedule, streamAllSchedules, rruleSchedules };
