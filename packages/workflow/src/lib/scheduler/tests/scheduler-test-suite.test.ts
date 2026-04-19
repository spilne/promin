// ---------------------------------------------------------------------------
// Run the portable Scheduler conformance suite against InMemoryScheduler.
// ---------------------------------------------------------------------------

import { schedulerTestSuite } from "../scheduler-test-suite.ts";
import { createScheduler } from "../in-memory-scheduler.ts";

schedulerTestSuite("InMemoryScheduler", () => ({
  scheduler: createScheduler(),
}));
