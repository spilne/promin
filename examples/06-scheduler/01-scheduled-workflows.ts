/**
 * Schedule recurring tasks with cron and rrule.
 * The scheduler streams ticks — connect to workflows via trigger.
 */

import { createScheduler } from "@promin/workflow";

const scheduler = createScheduler();

// Every weekday at 9am
await scheduler.register({
  id: "morning-report",
  cron: "0 9 * * MON-FRI",
  timezone: "America/New_York",
  metadata: { team: "analytics" },
});

// Every 30 seconds
await scheduler.register({ id: "health-check", intervalMs: 30_000 });

// Biweekly on Tuesday at 10am (rrule)
await scheduler.register({
  id: "sprint-planning",
  rrule: "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;BYHOUR=10",
});

// Pause / resume at runtime
await scheduler.pause("health-check");
await scheduler.resume("health-check");

// Stream ticks from a single schedule — stream() returns a perfect Stream,
// consumed here with for-await. Breaking out of the loop stops the stream
// and cancels its pending timer.
for await (const tick of scheduler.stream("morning-report").toAsyncIterable()) {
  const date = tick.scheduledAt.toISOString().split("T")[0];
  console.log(`Generating report for ${date}`);
}
