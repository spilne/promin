// ---------------------------------------------------------------------------
// InMemoryPendingApprovalStore — runs the pendingApprovalStoreTestSuite on a
// FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
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

describe("InMemoryPendingApprovalStore — immutable snapshots", () => {
  it("freezes detached rows and their requester identity", async () => {
    const store = new InMemoryPendingApprovalStore();
    const recorded = await store.record({
      namespaceId: "acme",
      agentId: "ops-bot",
      resourceId: "alice",
      threadId: "t",
      toolCallId: "call-1",
      workflowId: "wf-1",
      toolName: "deploy",
      input: { env: "prod" },
      elevated: true,
      requestContext: {
        actor: { kind: "service-account", id: "requester-sa" },
        effective: { kind: "user", id: "alice" },
      },
    });
    const read = await store.get({
      namespaceId: "acme",
      agentId: "ops-bot",
      resourceId: "alice",
      threadId: "t",
      toolCallId: "call-1",
    });

    expect(read).not.toBe(recorded);
    expect(Object.isFrozen(recorded)).toBe(true);
    expect(Object.isFrozen(recorded.input)).toBe(true);
    expect(Object.isFrozen(recorded.requestContext)).toBe(true);
    expect(Object.isFrozen(recorded.requestContext?.actor)).toBe(true);
    expect(Object.isFrozen(read)).toBe(true);
  });
});
