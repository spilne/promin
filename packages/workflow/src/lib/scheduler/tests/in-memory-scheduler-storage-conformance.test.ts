// ---------------------------------------------------------------------------
// InMemorySchedulerStorage — portable conformance suite invocation.
//
// Single line. The suite (scheduler-storage-test-suite.ts) covers every
// SchedulerStorage method's contract; this file just plugs the InMemory
// implementation into it. Postgres + Redis tests do the same against their
// respective backends.
// ---------------------------------------------------------------------------

import { schedulerStorageTestSuite } from "@promin/workflow/testing";
import { InMemorySchedulerStorage } from "../in-memory-scheduler-storage.ts";

schedulerStorageTestSuite(() => new InMemorySchedulerStorage());
