// ---------------------------------------------------------------------------
// createElevatedTool — audit-log emission.
// An elevated tool must call ctx.audit() per invocation; each call writes an
// `intent` record before the action and an outcome record after the body.
// Pinned cases:
//   1. One ctx.audit() call → intent + completed records carrying scope,
//      toolName and toolCallId
//   2. action / target / meta round-trip from the audit() call
//   3. Multiple audit() calls → intents in order, then outcomes in order
//   4. The intent is persisted before the side effect runs
//   5. A throwing body → intent + failed record with the error
//   6. No ToolAuditLogger wired → audit() still enforced, just not persisted
//   7. Elevated tool that never calls audit() → still throws
//   8. A logger that rejects fails audit() and the tool call
//   9. executeToolCall threads the logger and toolCallId onto ctx
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { FakeWallClock } from "@promin/workflow";
import { createElevatedTool } from "../tool.ts";
import { InMemoryToolAuditLogger } from "../tool-audit/in-memory-tool-audit-logger.ts";
import type { ToolAuditLogger } from "../tool-audit/types.ts";
import { executeToolCall } from "../agent-shared.ts";

const scope = { namespaceId: "acme", resourceId: "alice", agentId: "billing-agent" };

describe("createElevatedTool — audit log emission", () => {
  it("writes an intent and a completed record per ctx.audit() call", async () => {
    const clock = FakeWallClock.create(1_700_000);
    const logger = new InMemoryToolAuditLogger({ clock });
    const refund = createElevatedTool({
      name: "issue_refund",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "refund" });
        return "done";
      },
    });

    await refund.execute({}, { scope, toolAuditLogger: logger, toolCallId: "call-1" });

    const base = {
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      toolCallId: "call-1",
      action: "refund",
      timestamp: 1_700_000,
    };
    expect(logger.list()).toEqual([
      { ...base, phase: "intent" },
      { ...base, phase: "completed" },
    ]);
  });

  it("round-trips action / target / meta from the audit() call", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "grant_role",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "grant", target: "user:bob", meta: { role: "admin" } });
      },
    });

    await tool.execute({}, { scope, toolAuditLogger: logger });

    const [entry] = logger.list();
    expect(entry?.action).toBe("grant");
    expect(entry?.target).toBe("user:bob");
    expect(entry?.meta).toEqual({ role: "admin" });
  });

  it("records intents in call order, then their outcomes", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "bulk_op",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "step-1" });
        await ctx.audit({ action: "step-2" });
      },
    });

    await tool.execute({}, { scope, toolAuditLogger: logger });

    expect(logger.list().map((r) => `${r.phase}:${r.action}`)).toEqual([
      "intent:step-1",
      "intent:step-2",
      "completed:step-1",
      "completed:step-2",
    ]);
  });

  it("persists the intent before the side effect runs", async () => {
    const logger = new InMemoryToolAuditLogger();
    let recordedBeforeAction: string[] = [];
    const tool = createElevatedTool({
      name: "delete_tenant",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "delete", target: "tenant:42" });
        recordedBeforeAction = logger.list().map((r) => `${r.phase}:${r.action}`);
        throw new Error("crashed mid-delete");
      },
    });

    await expect(tool.execute({}, { scope, toolAuditLogger: logger })).rejects.toThrow(
      "crashed mid-delete",
    );

    expect(recordedBeforeAction).toEqual(["intent:delete"]);
    expect(logger.list().map((r) => ({ phase: r.phase, error: r.error }))).toEqual([
      { phase: "intent", error: undefined },
      { phase: "failed", error: "crashed mid-delete" },
    ]);
  });

  it("enforces audit() even with no logger wired — just doesn't persist", async () => {
    const tool = createElevatedTool({
      name: "no_logger",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "did-a-thing" });
        return "ok";
      },
    });
    // No toolAuditLogger on ctx — the call still succeeds (audit was called).
    expect(await tool.execute({}, { scope })).toBe("ok");
  });

  it("still throws when an elevated tool never calls audit()", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "forgot_audit",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });
    await expect(tool.execute({}, { scope, toolAuditLogger: logger })).rejects.toThrow(
      /without calling ctx\.audit/,
    );
    expect(logger.list()).toHaveLength(0);
  });

  it("rejects audit() and fails the tool call when the logger rejects", async () => {
    const failing: ToolAuditLogger = {
      record: async () => {
        throw new Error("audit store offline");
      },
    };
    let acted = false;
    const tool = createElevatedTool({
      name: "audit_fails",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "x" });
        acted = true;
        return "ok";
      },
    });
    await expect(tool.execute({}, { scope, toolAuditLogger: failing })).rejects.toThrow(
      /audit store offline/,
    );
    expect(acted).toBe(false);
  });

  it("fails the call when an un-awaited audit() write rejects", async () => {
    const failing: ToolAuditLogger = {
      record: async () => {
        throw new Error("audit store offline");
      },
    };
    const tool = createElevatedTool({
      name: "audit_not_awaited",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        void ctx.audit({ action: "x" });
        return "ok";
      },
    });
    await expect(tool.execute({}, { scope, toolAuditLogger: failing })).rejects.toThrow(
      /audit store offline/,
    );
  });

  it("executeToolCall threads the logger and toolCallId onto ctx", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "via_execute",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        await ctx.audit({ action: "ran" });
        return "ok";
      },
    });

    const result = await executeToolCall({
      call: { id: "c1", name: "via_execute", input: {} },
      tool,
      scope,
      toolAuditLogger: logger,
    });

    expect(result.content).toBe("ok");
    expect(logger.list().map((r) => [r.phase, r.action, r.toolCallId])).toEqual([
      ["intent", "ran", "c1"],
      ["completed", "ran", "c1"],
    ]);
  });
});
