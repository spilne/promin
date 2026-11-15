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
  it("upgrades legacy rows with an unknown, fail-closed approval boundary", async () => {
    const db = new Database(":memory:");
    db.run(`CREATE TABLE promin_pending_approval (
      namespace_id TEXT NOT NULL, thread_id TEXT NOT NULL, tool_call_id TEXT NOT NULL,
      resource_id TEXT, agent_id TEXT, workflow_id TEXT NOT NULL, tool_name TEXT NOT NULL,
      input TEXT NOT NULL, requested_at INTEGER NOT NULL, expires_at INTEGER,
      status TEXT NOT NULL, decided_by TEXT, decided_at INTEGER, reason TEXT, delivered_at INTEGER,
      PRIMARY KEY (namespace_id, thread_id, tool_call_id)
    )`);
    db.run(`INSERT INTO promin_pending_approval
      (namespace_id, thread_id, tool_call_id, workflow_id, tool_name, input, requested_at, status)
      VALUES ('acme', 't', 'legacy', 'wf-old', 'deploy', '{}', 1, 'pending')`);

    const store = SqlitePendingApprovalStore.make({ db });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: null,
        resourceId: null,
        threadId: "t",
        toolCallId: "legacy",
      }),
    ).toMatchObject({
      elevated: null,
      requestContext: null,
      decidedByPrincipal: null,
    });
    expect(
      await store.record({
        namespaceId: "acme",
        agentId: "ops-bot",
        resourceId: "alice",
        threadId: "t",
        toolCallId: "new",
        workflowId: "wf-new",
        toolName: "deploy",
        input: {},
        elevated: false,
        requestContext: { actor: { kind: "user", id: "alice" } },
      }),
    ).toMatchObject({
      elevated: false,
      requestContext: { actor: { kind: "user", id: "alice" } },
    });
    await store.record({
      namespaceId: "acme",
      agentId: "other-bot",
      resourceId: "bob",
      threadId: "t",
      toolCallId: "new",
      workflowId: "wf-bob",
      toolName: "deploy",
      input: {},
      elevated: false,
      requestContext: { actor: { kind: "user", id: "bob" } },
    });
    expect(
      await store.get({
        namespaceId: "acme",
        agentId: "other-bot",
        resourceId: "bob",
        threadId: "t",
        toolCallId: "new",
      }),
    ).toMatchObject({ workflowId: "wf-bob" });
  });

  it("survives a new store instance on the same database", async () => {
    const db = new Database(":memory:");
    const first = SqlitePendingApprovalStore.make({ db });
    await first.record({
      namespaceId: "acme",
      agentId: null,
      resourceId: null,
      threadId: "t",
      toolCallId: "c",
      workflowId: "wf",
      toolName: "deploy",
      input: { env: "prod" },
      elevated: false,
      requestContext: null,
    });
    const second = SqlitePendingApprovalStore.make({ db });
    const row = await second.get({
      namespaceId: "acme",
      agentId: null,
      resourceId: null,
      threadId: "t",
      toolCallId: "c",
    });
    expect(row).toMatchObject({ workflowId: "wf", input: { env: "prod" }, status: "pending" });
  });
});
