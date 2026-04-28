// ---------------------------------------------------------------------------
// ApprovalStorage + attachApprovalStorage — verifies:
//   - InMemoryApprovalStorage CRUD + status transitions + idempotency
//   - decide() rejects status conflicts but is no-op on same-status replay
//   - expirePending() flips stale rows
//   - attachApprovalStorage mirrors bus events into storage end-to-end
//     (request created on approval.requested, decided on approval.decision)
//   - request id composition is stable
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeClock } from "@promin/core";
import { InMemoryApprovalStorage } from "../in-memory-approval-storage.ts";
import { attachApprovalStorage, requestIdFor } from "../attach.ts";
import { ApprovalDecisionConflictError } from "../types.ts";
import { SessionEventBus } from "../../session-logger.ts";

describe("InMemoryApprovalStorage", () => {
  it("create persists a pending row with createdAt + null decision fields", async () => {
    const clock = FakeClock.create(1_000);
    const storage = new InMemoryApprovalStorage({ clock });
    const row = await storage.create({
      requestId: "wf-1::call-1",
      workflowId: "wf-1",
      signalName: "approve:call-1",
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "send_invoice",
      toolInput: { invoiceId: 42 },
      summary: "send invoice 42",
    });
    expect(row).toMatchObject({
      requestId: "wf-1::call-1",
      status: "pending",
      createdAt: 1_000,
      decidedAt: null,
      decidedBy: null,
      decisionReason: null,
    });
  });

  it("create is idempotent on the same requestId", async () => {
    const storage = new InMemoryApprovalStorage();
    const a = await storage.create({
      requestId: "r1",
      workflowId: "wf-1",
      signalName: "approve:c1",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "first",
    });
    const b = await storage.create({
      requestId: "r1",
      workflowId: "wf-1",
      signalName: "approve:c1",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "second", // ignored — first wins
    });
    expect(b).toBe(a);
    expect(b.summary).toBe("first");
  });

  it("decide transitions pending → approved with decidedBy + decidedAt", async () => {
    const clock = FakeClock.create(1_000);
    const storage = new InMemoryApprovalStorage({ clock });
    await storage.create({
      requestId: "r1",
      workflowId: "wf-1",
      signalName: "approve:c1",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "test",
    });
    clock.advance(500);
    const decided = await storage.decide({
      requestId: "r1",
      decision: "approved",
      decidedBy: "alice@acme",
      reason: "looks good",
    });
    expect(decided).toMatchObject({
      status: "approved",
      decidedBy: "alice@acme",
      decisionReason: "looks good",
      decidedAt: 1_500,
    });
  });

  it("decide is idempotent on same-status replay", async () => {
    const storage = new InMemoryApprovalStorage();
    await storage.create({
      requestId: "r1",
      workflowId: "wf-1",
      signalName: "approve:c1",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "test",
    });
    const a = await storage.decide({
      requestId: "r1",
      decision: "approved",
      decidedBy: "op",
    });
    const b = await storage.decide({
      requestId: "r1",
      decision: "approved",
      decidedBy: "op",
    });
    expect(b).toBe(a); // no-op, same row
  });

  it("decide throws ApprovalDecisionConflictError when transitioning approved → rejected", async () => {
    const storage = new InMemoryApprovalStorage();
    await storage.create({
      requestId: "r1",
      workflowId: "wf-1",
      signalName: "approve:c1",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "test",
    });
    await storage.decide({ requestId: "r1", decision: "approved", decidedBy: "op" });
    expect(
      storage.decide({ requestId: "r1", decision: "rejected", decidedBy: "other" }),
    ).rejects.toBeInstanceOf(ApprovalDecisionConflictError);
  });

  it("list filters by namespaceId + status + toolName", async () => {
    const storage = new InMemoryApprovalStorage();
    await storage.create({
      requestId: "r1",
      workflowId: "w",
      signalName: "s",
      namespaceId: "acme",
      toolName: "send",
      toolInput: {},
      summary: "",
    });
    await storage.create({
      requestId: "r2",
      workflowId: "w",
      signalName: "s",
      namespaceId: "globex",
      toolName: "send",
      toolInput: {},
      summary: "",
    });
    await storage.create({
      requestId: "r3",
      workflowId: "w",
      signalName: "s",
      namespaceId: "acme",
      toolName: "delete",
      toolInput: {},
      summary: "",
    });
    await storage.decide({ requestId: "r1", decision: "approved", decidedBy: "op" });

    expect((await storage.list({ namespaceId: "acme" })).map((r) => r.requestId).sort()).toEqual([
      "r1",
      "r3",
    ]);
    expect(
      (await storage.list({ namespaceId: "acme", status: "pending" })).map((r) => r.requestId),
    ).toEqual(["r3"]);
    expect((await storage.list({ toolName: "delete" })).map((r) => r.requestId)).toEqual(["r3"]);
  });

  it("expirePending flips pending rows whose expiresAt is past", async () => {
    const clock = FakeClock.create(1_000);
    const storage = new InMemoryApprovalStorage({ clock });
    await storage.create({
      requestId: "expired",
      workflowId: "w",
      signalName: "s",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "",
      expiresAt: 1_500,
    });
    await storage.create({
      requestId: "fresh",
      workflowId: "w",
      signalName: "s",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "",
      expiresAt: 5_000,
    });
    await storage.create({
      requestId: "no-expiry",
      workflowId: "w",
      signalName: "s",
      namespaceId: "acme",
      toolName: "x",
      toolInput: {},
      summary: "",
      // no expiresAt — never expires
    });

    const count = await storage.expirePending(2_000);
    expect(count).toBe(1);
    expect((await storage.get("expired"))?.status).toBe("expired");
    expect((await storage.get("fresh"))?.status).toBe("pending");
    expect((await storage.get("no-expiry"))?.status).toBe("pending");
  });
});

describe("requestIdFor", () => {
  it("composes a stable id from (workflowId, toolCallId)", () => {
    expect(requestIdFor("wf-1", "call-2")).toBe("wf-1::call-2");
  });
});

describe("attachApprovalStorage — bus → storage bridge", () => {
  it("creates a row on approval.requested and decides on approval.decision", async () => {
    const bus = new SessionEventBus();
    const storage = new InMemoryApprovalStorage();
    const handle = attachApprovalStorage({
      bus,
      storage,
      workflowId: "wf-1",
      namespaceId: "acme",
      resourceId: "alice",
    });

    // Loop fires tool.start FIRST (carries input), then approval.requested.
    bus.emit({
      type: "tool.start",
      turn: 0,
      step: 0,
      name: "send_invoice",
      input: { invoiceId: 42 },
    });
    bus.emit({
      type: "approval.requested",
      turn: 0,
      toolCallId: "call-1",
      toolName: "send_invoice",
    });

    // Microtask flush — the bus subscriber fires synchronously but the
    // storage create is async.
    await Promise.resolve();
    await Promise.resolve();

    const row = await storage.get("wf-1::call-1");
    expect(row).toMatchObject({
      requestId: "wf-1::call-1",
      workflowId: "wf-1",
      signalName: "approve:call-1",
      namespaceId: "acme",
      resourceId: "alice",
      toolName: "send_invoice",
      toolInput: { invoiceId: 42 },
      status: "pending",
    });
    expect(row?.summary).toContain("send_invoice");

    bus.emit({
      type: "approval.decision",
      turn: 0,
      toolCallId: "call-1",
      approved: true,
    });
    await Promise.resolve();
    await Promise.resolve();

    const decided = await storage.get("wf-1::call-1");
    expect(decided?.status).toBe("approved");
    expect(decided?.decidedBy).toBe("system:hook");

    handle.detach();
  });

  it("rejected decision lands as status: rejected", async () => {
    const bus = new SessionEventBus();
    const storage = new InMemoryApprovalStorage();
    attachApprovalStorage({
      bus,
      storage,
      workflowId: "wf-1",
      namespaceId: "acme",
    });

    bus.emit({ type: "tool.start", turn: 0, step: 0, name: "danger", input: {} });
    bus.emit({
      type: "approval.requested",
      turn: 0,
      toolCallId: "c2",
      toolName: "danger",
    });
    bus.emit({
      type: "approval.decision",
      turn: 0,
      toolCallId: "c2",
      approved: false,
    });
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect((await storage.get("wf-1::c2"))?.status).toBe("rejected");
  });

  it("custom summarize callback shapes the human-readable summary", async () => {
    const bus = new SessionEventBus();
    const storage = new InMemoryApprovalStorage();
    attachApprovalStorage({
      bus,
      storage,
      workflowId: "wf-1",
      namespaceId: "acme",
      summarize: (e) => `Operator review needed for ${e.toolName} (call ${e.toolCallId})`,
    });

    bus.emit({ type: "tool.start", turn: 0, step: 0, name: "x", input: {} });
    bus.emit({ type: "approval.requested", turn: 0, toolCallId: "cz", toolName: "x" });
    await Promise.resolve();
    await Promise.resolve();

    expect((await storage.get("wf-1::cz"))?.summary).toBe("Operator review needed for x (call cz)");
  });

  it("detach() stops further mirroring", async () => {
    const bus = new SessionEventBus();
    const storage = new InMemoryApprovalStorage();
    const handle = attachApprovalStorage({
      bus,
      storage,
      workflowId: "wf-1",
      namespaceId: "acme",
    });
    handle.detach();

    bus.emit({ type: "tool.start", turn: 0, step: 0, name: "x", input: {} });
    bus.emit({ type: "approval.requested", turn: 0, toolCallId: "c", toolName: "x" });
    await Promise.resolve();
    await Promise.resolve();

    expect(await storage.get("wf-1::c")).toBeNull();
  });
});
