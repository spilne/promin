// ---------------------------------------------------------------------------
// PostgresToolAuditLogger — durable backend for the elevated-tool audit log,
// exercised against a real PG container.
// Pinned cases:
//   1. record() → list() round-trip; recorded_at is server-clock assigned
//   2. an actual createElevatedTool call's ctx.audit() entry persists
//   3. multiple audit() calls in one tool body → intent rows, then outcome rows
//   4. nullable fields (agentId / target / meta) omitted on read when absent
//   5. list({ namespaceId }) filters by caller namespace
//   6. list({ since, until }) filters by recorded-at window
//   7. two logger instances on the same db see each other's writes
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { z } from "zod";
import { createElevatedTool } from "@promin/agent";
import { migrate } from "../../migrate.ts";
import { PostgresToolAuditLogger } from "../tool-audit-logger.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_audit_log`;
});

const scope = { namespaceId: "acme", resourceId: "alice", agentId: "billing-agent" };

describe("PostgresToolAuditLogger", () => {
  it("round-trips a recorded entry, with a server-clock timestamp", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    const before = Date.now();
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      action: "refund",
      phase: "intent",
    });
    const after = Date.now();

    const records = await logger.list();
    expect(records).toHaveLength(1);
    const [r] = records;
    expect(r).toMatchObject({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      action: "refund",
      phase: "intent",
    });
    // recorded_at owned by the DB clock — within the call window, not
    // a value the caller supplied (record() takes no timestamp).
    expect(r?.timestamp).toBeGreaterThanOrEqual(before - 1_000);
    expect(r?.timestamp).toBeLessThanOrEqual(after + 1_000);
  });

  it("persists an elevated tool's ctx.audit() entry", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    const refund = createElevatedTool({
      name: "issue_refund",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "refund", target: "order:42", meta: { amount: 100 } });
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
      phase: "completed",
      timestamp: expect.any(Number),
    });
  });

  it("emits one row per audit() call, oldest first", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    const tool = createElevatedTool({
      name: "bulk_op",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "step-1" });
        await ctx.audit({ action: "step-2" });
        await ctx.audit({ action: "step-3" });
      },
    });
    await tool.execute({}, { scope, toolAuditLogger: logger });

    // list() is newest-first; reverse for insertion order.
    const rows = (await logger.list()).map((r) => `${r.phase}:${r.action}`).reverse();
    expect(rows).toEqual([
      "intent:step-1",
      "intent:step-2",
      "intent:step-3",
      "completed:step-1",
      "completed:step-2",
      "completed:step-3",
    ]);
  });

  it("omits agentId / target / meta on read when not supplied", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "minimal",
      action: "did-a-thing",
      phase: "intent",
    });
    const [r] = await logger.list();
    expect(r).toEqual({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "minimal",
      action: "did-a-thing",
      phase: "intent",
      timestamp: expect.any(Number),
    });
    expect(r && "agentId" in r).toBe(false);
    expect(r && "target" in r).toBe(false);
    expect(r && "meta" in r).toBe(false);
  });

  it("filters by caller namespace", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "t",
      action: "acme-action",
      phase: "intent",
    });
    await logger.record({
      namespaceId: "globex",
      resourceId: "bob",
      toolName: "t",
      action: "globex-action",
      phase: "intent",
    });

    expect((await logger.list({ namespaceId: "acme" })).map((r) => r.action)).toEqual([
      "acme-action",
    ]);
    expect((await logger.list({ namespaceId: "globex" })).map((r) => r.action)).toEqual([
      "globex-action",
    ]);
  });

  it("filters by recorded-at window", async () => {
    const logger = new PostgresToolAuditLogger({ db: pg.db });
    const start = Date.now();
    await logger.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "t",
      action: "in-window",
      phase: "intent",
    });

    // Window that brackets the write sees it; a window entirely in the
    // past does not.
    expect(
      (await logger.list({ since: start - 5_000, until: Date.now() + 5_000 })).map((r) => r.action),
    ).toEqual(["in-window"]);
    expect(await logger.list({ until: start - 5_000 })).toEqual([]);
  });

  it("two instances on the same db see each other's writes", async () => {
    const writer = new PostgresToolAuditLogger({ db: pg.db });
    const reader = new PostgresToolAuditLogger({ db: pg.db });
    await writer.record({
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "shared",
      action: "cross-instance",
      phase: "intent",
    });
    expect((await reader.list()).map((r) => r.action)).toEqual(["cross-instance"]);
  });
});
