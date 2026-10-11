// ---------------------------------------------------------------------------
// PostgresPendingApprovalStore — runs the @promin/agent
// pendingApprovalStoreTestSuite against a real PG container. Time is the
// database server's, so `advance` waits real time.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
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
      threadId: "t",
      toolCallId: "c",
      workflowId: "wf",
      toolName: "deploy",
      input: null,
    });
    const key = { namespaceId: "acme", threadId: "t", toolCallId: "c" };
    expect((await b.decide({ ...key, approved: true, decidedBy: "bob" })).outcome).toBe("decided");
    expect((await a.decide({ ...key, approved: false })).outcome).toBe("already_decided");
  });
});
