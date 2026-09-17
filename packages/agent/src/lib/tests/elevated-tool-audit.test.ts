// ---------------------------------------------------------------------------
// createElevatedTool — audit-log emission.
// An elevated tool must call ctx.audit() per invocation; this turns each
// such call into a durable ToolAuditLogger record.
// Pinned cases:
//   1. One ctx.audit() call → one record carrying the caller scope + toolName
//   2. action / target / meta round-trip from the audit() call
//   3. Multiple audit() calls → multiple records, in order
//   4. No ToolAuditLogger wired → audit() still enforced, just not persisted
//   5. Elevated tool that never calls audit() → still throws (unchanged)
//   6. A logger that rejects fails the tool call
//   7. executeToolCall threads the logger onto ctx.toolAuditLogger
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { FakeClock } from "@promin/core";
import { createElevatedTool } from "../tool.ts";
import { InMemoryToolAuditLogger } from "../tool-audit/in-memory-tool-audit-logger.ts";
import type { ToolAuditLogger } from "../tool-audit/types.ts";
import { executeToolCall } from "../agent-shared.ts";

const scope = { namespaceId: "acme", resourceId: "alice", agentId: "billing-agent" };

describe("createElevatedTool — audit log emission", () => {
  it("emits one record per ctx.audit() call, carrying caller scope + toolName", async () => {
    const clock = FakeClock.create(1_700_000);
    const logger = new InMemoryToolAuditLogger({ clock });
    const refund = createElevatedTool({
      name: "issue_refund",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "refund" });
        return "done";
      },
    });

    await refund.execute({}, { scope, toolAuditLogger: logger });

    const records = logger.list();
    expect(records).toHaveLength(1);
    expect(records[0]).toEqual({
      namespaceId: "acme",
      resourceId: "alice",
      agentId: "billing-agent",
      toolName: "issue_refund",
      action: "refund",
      timestamp: 1_700_000,
    });
  });

  it("round-trips action / target / meta from the audit() call", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "grant_role",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "grant", target: "user:bob", meta: { role: "admin" } });
      },
    });

    await tool.execute({}, { scope, toolAuditLogger: logger });

    const [entry] = logger.list();
    expect(entry?.action).toBe("grant");
    expect(entry?.target).toBe("user:bob");
    expect(entry?.meta).toEqual({ role: "admin" });
  });

  it("emits one record per audit() call, in order", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "bulk_op",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "step-1" });
        ctx.audit({ action: "step-2" });
      },
    });

    await tool.execute({}, { scope, toolAuditLogger: logger });

    expect(logger.list().map((r) => r.action)).toEqual(["step-1", "step-2"]);
  });

  it("enforces audit() even with no logger wired — just doesn't persist", async () => {
    const tool = createElevatedTool({
      name: "no_logger",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "did-a-thing" });
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

  it("fails the tool call when the logger rejects", async () => {
    const failing: ToolAuditLogger = {
      record: async () => {
        throw new Error("audit store offline");
      },
    };
    const tool = createElevatedTool({
      name: "audit_fails",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "x" });
        return "ok";
      },
    });
    await expect(tool.execute({}, { scope, toolAuditLogger: failing })).rejects.toThrow(
      /audit store offline/,
    );
  });

  it("executeToolCall threads the logger onto ctx.toolAuditLogger", async () => {
    const logger = new InMemoryToolAuditLogger();
    const tool = createElevatedTool({
      name: "via_execute",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "ran" });
        return "ok";
      },
    });

    const result = await executeToolCall(
      { id: "c1", name: "via_execute", input: {} },
      tool,
      undefined,
      undefined,
      scope,
      logger,
    );

    expect(result.content).toBe("ok");
    expect(logger.list().map((r) => r.action)).toEqual(["ran"]);
  });
});
