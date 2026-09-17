// ---------------------------------------------------------------------------
// SqliteToolAuditLogger — durable backend for the elevated-tool audit log,
// exercised against an in-memory SQLite database.
// Pinned cases:
//   1. record() -> list() round-trip; recorded_at is clock-assigned
//   2. an actual createElevatedTool call's ctx.audit() entry persists
//   3. multiple audit() calls in one tool body -> multiple rows, in order
//   4. nullable fields (agentId / target / meta) omitted on read when absent
//   5. list({ namespaceId }) filters by caller namespace
//   6. list({ since, until }) filters by recorded-at window
//   7. two logger instances on the same db see each other's writes
//   8. respects a custom table name
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { z } from "zod";
import { FakeClock } from "@promin/core";
import { createElevatedTool } from "@promin/agent";
import { SqliteToolAuditLogger } from "../tool-audit-logger.ts";

const scope = { namespaceId: "acme", resourceId: "alice", agentId: "billing-agent" };

describe("SqliteToolAuditLogger", () => {
  it("round-trips a recorded entry, with a clock-assigned timestamp", async () => {
    const clock = FakeClock.create(1_700_000);
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:"), clock });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      action: "refund",
    });

    expect(await logger.list()).toEqual([
      {
        namespaceId: "acme",
        resourceId: "alice",
        agentId: "billing-agent",
        toolName: "issue_refund",
        action: "refund",
        timestamp: 1_700_000,
      },
    ]);
  });

  it("persists an elevated tool's ctx.audit() entry", async () => {
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:") });
    const refund = createElevatedTool({
      name: "issue_refund",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "refund", target: "order:42", meta: { amount: 100 } });
        return "done";
      },
    });

    expect(await refund.execute({}, { scope, toolAuditLogger: logger })).toBe("done");

    const [r] = await logger.list();
    expect(r).toEqual({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      action: "refund",
      target: "order:42",
      meta: { amount: 100 },
      timestamp: expect.any(Number),
    });
  });

  it("emits one row per audit() call, oldest first", async () => {
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:") });
    const tool = createElevatedTool({
      name: "bulk_op",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "step-1" });
        ctx.audit({ action: "step-2" });
        ctx.audit({ action: "step-3" });
      },
    });
    await tool.execute({}, { scope, toolAuditLogger: logger });

    // list() is newest-first; reverse for insertion order.
    const actions = (await logger.list()).map((r) => r.action).reverse();
    expect(actions).toEqual(["step-1", "step-2", "step-3"]);
  });

  it("omits agentId / target / meta on read when not supplied", async () => {
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:") });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "minimal",
      action: "did-a-thing",
    });
    const [r] = await logger.list();
    expect(r).toEqual({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "minimal",
      action: "did-a-thing",
      timestamp: expect.any(Number),
    });
    expect(r && "agentId" in r).toBe(false);
    expect(r && "target" in r).toBe(false);
    expect(r && "meta" in r).toBe(false);
  });

  it("filters by caller namespace", async () => {
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:") });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "t",
      action: "acme-action",
    });
    await logger.record({
      namespaceId: "globex",
      resourceId: "bob",
      toolName: "t",
      action: "globex-action",
    });

    expect((await logger.list({ namespaceId: "acme" })).map((r) => r.action)).toEqual([
      "acme-action",
    ]);
    expect((await logger.list({ namespaceId: "globex" })).map((r) => r.action)).toEqual([
      "globex-action",
    ]);
  });

  it("filters by recorded-at window", async () => {
    const clock = FakeClock.create(1_000_000);
    const logger = SqliteToolAuditLogger.make({ db: new Database(":memory:"), clock });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "t",
      action: "in-window",
    });

    // A window bracketing the write sees it; one entirely in the past does not.
    expect((await logger.list({ since: 999_000, until: 1_001_000 })).map((r) => r.action)).toEqual([
      "in-window",
    ]);
    expect(await logger.list({ until: 999_000 })).toEqual([]);
  });

  it("two instances on the same db see each other's writes", async () => {
    const db = new Database(":memory:");
    const writer = SqliteToolAuditLogger.make({ db });
    const reader = SqliteToolAuditLogger.make({ db });
    await writer.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "shared",
      action: "cross-instance",
    });
    expect((await reader.list()).map((r) => r.action)).toEqual(["cross-instance"]);
  });

  it("respects a custom table name", async () => {
    const db = new Database(":memory:");
    const logger = SqliteToolAuditLogger.make({ db, table: "my_audit" });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "t",
      action: "custom-table",
    });
    const rows = db.query("SELECT action FROM my_audit").all() as Array<{ action: string }>;
    expect(rows).toEqual([{ action: "custom-table" }]);
  });
});
