// ---------------------------------------------------------------------------
// InMemoryPendingApprovalStore — runs the pendingApprovalStoreTestSuite on a
// FakeWallClock.
// ---------------------------------------------------------------------------

import { FakeWallClock } from "@promin/workflow";
import { InMemoryPendingApprovalStore } from "../in-memory-pending-approval-store.ts";
import { pendingApprovalStoreTestSuite } from "../pending-approval-store-test-suite.ts";

pendingApprovalStoreTestSuite(async () => {
  const clock = FakeWallClock.create(1_700_000_000_000);
  return {
    store: new InMemoryPendingApprovalStore({ clock }),
    advance: async (ms) => {
      clock.advance(ms);
    },
  };
});
