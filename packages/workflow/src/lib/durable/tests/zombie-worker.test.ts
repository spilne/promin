import { FakeWallClock } from "../../shared/wall-clock.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import type { WorkflowStorage } from "../workflow-storage.ts";
import { zombieWorkerTestSuite } from "../zombie-worker-test-suite.ts";

// The storage judges lock expiry on its own clock: moving it past the
// runner's lease (120s) is the stalled holder's lease running out. The
// runner keeps the real clock, so no heartbeat fires during the test.
const clocks = new WeakMap<WorkflowStorage, FakeWallClock>();

zombieWorkerTestSuite({
  createStorage: () => {
    const clock = FakeWallClock.create(Date.now());
    const storage = new InMemoryWorkflowStorage({ clock });
    clocks.set(storage, clock);
    return storage;
  },
  // Single-process backend: the second worker shares the instance.
  createPeer: (storage) => storage,
  expireLock: async ({ storage }) => {
    const clock = clocks.get(storage)!;
    clock.set(clock.currentTimeMs() + 121_000);
  },
});
