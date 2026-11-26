// ---------------------------------------------------------------------------
// PostgresPendingApprovalStore — runs the @promin/agent
// pendingApprovalStoreTestSuite against a real PG container. Time is the
// database server's, so `advance` waits real time.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { snapshotPendingApprovalBoundary } from "@promin/agent";
import { pendingApprovalStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresPendingApprovalStore } from "../pending-approval-store.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_pending_approval`;
});

pendingApprovalStoreTestSuite(async () => ({
  store: new PostgresPendingApprovalStore({ db: pg.db }),
  advance: (ms) => new Promise<void>((resolve) => setTimeout(resolve, ms + 5)),
}));

describe("PostgresPendingApprovalStore — Postgres-specific", () => {
  it("two stores on one database share rows and decisions", async () => {
    const a = new PostgresPendingApprovalStore({ db: pg.db });
    const b = new PostgresPendingApprovalStore({ db: pg.db });
    await a.record({
      namespaceId: "acme",
      agentId: "ops-bot",
      resourceId: "alice",
      threadId: "t",
      toolCallId: "c",
      workflowId: "wf",
      toolName: "deploy",
      input: null,
      elevated: false,
      requestContext: { actor: { kind: "user", id: "alice" } },
    });
    const key = {
      namespaceId: "acme",
      agentId: "ops-bot",
      resourceId: "alice",
      threadId: "t",
      toolCallId: "c",
    };
    const expected = snapshotPendingApprovalBoundary((await b.get(key))!);
    expect((await b.decide({ expected, approved: true, decidedBy: "bob" })).outcome).toBe(
      "decided",
    );
    expect((await a.decide({ expected, approved: false })).outcome).toBe("already_decided");
  });
});
