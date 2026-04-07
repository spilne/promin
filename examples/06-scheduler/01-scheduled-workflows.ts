/**
 * Schedule recurring tasks with cron and rrule.
 * The scheduler streams ticks — connect to workflows via trigger.
 */

import { createScheduler } from "@promin/core";

const scheduler = createScheduler();

// Every weekday at 9am
scheduler.register({
  id: "morning-report",
  cron: "0 9 * * MON-FRI",
  timezone: "America/New_York",
  metadata: { team: "analytics" },
});

// Every 30 seconds
scheduler.register({ id: "health-check", intervalMs: 30_000 });

// Biweekly on Tuesday at 10am (rrule)
scheduler.register({
  id: "sprint-planning",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
});

// Stream ticks from a single schedule
scheduler.stream("morning-report").forEach((tick) => {
  const date = tick.scheduledAt.toISOString().split("T")[0];
  console.log(`Generating report for ${date}`);
});

// Pause / resume at runtime
scheduler.pause("health-check");
scheduler.resume("health-check");
