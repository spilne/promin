// ---------------------------------------------------------------------------
// `SqlitePendingApprovalStore` — runs the PendingApprovalStore conformance
// suite on a FakeWallClock, plus persistence across instances.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { pendingApprovalStoreTestSuite } from "@promin/agent/testing";
import { FakeWallClock } from "@promin/workflow";
import { SqlitePendingApprovalStore } from "../pending-approval-store.ts";

pendingApprovalStoreTestSuite(async () => {
  const clock = FakeWallClock.create(1_700_000_000_000);
  return {
    store: SqlitePendingApprovalStore.make({ db: new Database(":memory:"), clock }),
    advance: async (ms) => {
      clock.advance(ms);
    },
  };
});

describe("SqlitePendingApprovalStore — persistence", () => {
  it("survives a new store instance on the same database", async () => {
    const db = new Database(":memory:");
    const first = SqlitePendingApprovalStore.make({ db });
    await first.record({
      namespaceId: "acme",
      threadId: "t",
      toolCallId: "c",
      workflowId: "wf",
      toolName: "deploy",
      input: { env: "prod" },
    });
    const second = SqlitePendingApprovalStore.make({ db });
    const row = await second.get({ namespaceId: "acme", threadId: "t", toolCallId: "c" });
    expect(row).toMatchObject({ workflowId: "wf", input: { env: "prod" }, status: "pending" });
  });
});
