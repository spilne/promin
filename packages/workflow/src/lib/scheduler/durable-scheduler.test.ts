// ---------------------------------------------------------------------------
// Run the portable Scheduler conformance suite against the DurableScheduler
// shell wired to InMemorySchedulerStorage. Exercises the storage interface
// end-to-end with the same poll-based semantics that Postgres/Redis adapters
// share.
// ---------------------------------------------------------------------------

import { schedulerTestSuite } from "./scheduler-test-suite.ts";
import { DurableScheduler } from "./durable-scheduler.ts";
import { InMemorySchedulerStorage } from "./in-memory-scheduler-storage.ts";

schedulerTestSuite("DurableScheduler+InMemoryStorage", () => {
  const storage = new InMemorySchedulerStorage();
  const scheduler = new DurableScheduler({ storage, pollIntervalMs: 25 });
  return {
    scheduler,
    register: (config) => scheduler.registerAsync(config),
    unregister: (id, options) => scheduler.unregisterAsync(id, options),
    pause: (id) => scheduler.pauseAsync(id),
    resume: (id) => scheduler.resumeAsync(id),
    list: async () => scheduler.listAsync(),
  };
});
