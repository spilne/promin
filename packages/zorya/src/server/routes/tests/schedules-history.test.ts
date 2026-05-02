// ---------------------------------------------------------------------------
// Smoke test for `getScheduleHistory` — verifies the full round-trip:
//
//   schedule (with agentTrigger metadata)
//     + tick recorded in scheduler tick log
//     + workflow row created at scheduleTickRunId(...)
//   → history endpoint joins them and returns real status, not the
//     hardcoded "completed" agent fallback.
//
// This is the integration glue that the per-component tests don't cover:
// the deterministic-runId contract feeding into the route's join.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemorySchedulerStorage,
  InMemoryWorkflowStorage,
  scheduleTickRunId,
} from "@promin/workflow";
import { getScheduleHistory } from "../schedules.ts";

interface HistoryRow {
  tickNumber?: number;
  workflowId: string;
  workflowName: string;
  status: string;
  kind?: "workflow" | "agent";
  startedAt?: string;
  completedAt?: string;
  lagMs?: number;
  durationMs?: number;
}

async function callHistory(
  schedulerStorage: InMemorySchedulerStorage,
  workflowStorage: InMemoryWorkflowStorage,
  scheduleId: string,
): Promise<{ history: HistoryRow[]; total: number }> {
  const handler = getScheduleHistory(schedulerStorage, workflowStorage);
  const res = await handler(new Request(`http://x/api/schedules/${scheduleId}/history`), {
    id: scheduleId,
  });
  return (await res.json()) as { history: HistoryRow[]; total: number };
}

async function logTick(
  sch: InMemorySchedulerStorage,
  scheduleId: string,
  tickNumber: number,
  firedAt: Date,
): Promise<void> {
  await sch.commitPoll([
    {
      id: scheduleId,
      firedAt,
      tickIncrement: 1,
      nextRun: new Date(firedAt.getTime() + 60_000),
      ticks: [{ scheduleId, scheduledAt: firedAt, firedAt, tickNumber }],
    },
  ]);
}

describe("getScheduleHistory — agent ticks join the workflow row at scheduleTickRunId", () => {
  it("an agent-triggered tick that produced a completed workflow row reports kind=agent + real status", async () => {
    const sch = new InMemorySchedulerStorage();
    const wfs = new InMemoryWorkflowStorage();

    await sch.upsertSchedule({
      id: "daily-summary",
      intervalMs: 60_000,
      enabled: true,
      metadata: {
        agentTrigger: true,
        agentId: "summary-bot",
        task: "Recap email inbox",
        namespaceId: "acme",
        createdByAgent: "summary-bot",
      },
    });

    const firedAt = new Date("2026-05-02T09:00:00Z");
    await logTick(sch, "daily-summary", 0, firedAt);

    // Simulate `dispatchAgentSchedule` having created and completed the
    // workflow row at the deterministic id.
    const wfId = scheduleTickRunId("daily-summary", 0);
    await wfs.createWorkflow({
      workflowId: wfId,
      workflowName: "summary-bot",
      input: { task: "Recap email inbox" },
    });
    await wfs.completeWorkflow(wfId, { ok: true });

    const { history, total } = await callHistory(sch, wfs, "daily-summary");

    expect(total).toBe(1);
    expect(history).toHaveLength(1);
    const row = history[0]!;
    // The whole point: `status` comes from the workflow row, NOT the
    // legacy hardcoded "completed" fallback for agent schedules. (When
    // the row's status happens to be "completed" the legacy behaviour
    // would coincidentally pass — that's why the next test exists.)
    expect(row.status).toBe("completed");
    // Schedule provenance — the chip stays "agent" because the
    // schedule's metadata says so.
    expect(row.kind).toBe("agent");
    expect(row.workflowId).toBe(wfId);
    expect(row.workflowName).toBe("summary-bot");
  });

  it("an agent tick whose workflow row failed reports status=failed (not the legacy 'completed')", async () => {
    // This is the test the legacy hardcoded-completed path failed: every
    // agent tick used to be reported "completed" regardless of the
    // underlying invocation outcome.
    const sch = new InMemorySchedulerStorage();
    const wfs = new InMemoryWorkflowStorage();
    await sch.upsertSchedule({
      id: "flaky-bot",
      intervalMs: 60_000,
      enabled: true,
      metadata: { agentTrigger: true, agentId: "flaky", task: "x", namespaceId: "n" },
    });
    const firedAt = new Date("2026-05-02T09:05:00Z");
    await logTick(sch, "flaky-bot", 0, firedAt);
    const wfId = scheduleTickRunId("flaky-bot", 0);
    await wfs.createWorkflow({ workflowId: wfId, workflowName: "flaky", input: {} });
    await wfs.failWorkflow(wfId, "boom");

    const { history } = await callHistory(sch, wfs, "flaky-bot");
    expect(history[0]!.status).toBe("failed");
    expect(history[0]!.kind).toBe("agent");
  });

  it("an agent tick with no workflow row yet (race: tick logged, dispatch in flight) reports pending", async () => {
    // Belt-and-suspenders: confirms the join doesn't crash when the
    // workflow row isn't there yet.
    const sch = new InMemorySchedulerStorage();
    const wfs = new InMemoryWorkflowStorage();
    await sch.upsertSchedule({
      id: "race-bot",
      intervalMs: 60_000,
      enabled: true,
      metadata: { agentTrigger: true, agentId: "r", task: "x", namespaceId: "n" },
    });
    const firedAt = new Date();
    await logTick(sch, "race-bot", 0, firedAt);

    const { history } = await callHistory(sch, wfs, "race-bot");
    expect(history[0]!.status).toBe("pending");
    expect(history[0]!.kind).toBe("agent");
  });

  it("a non-agent (workflow-triggered) schedule still joins and reports kind=workflow", async () => {
    // Regression guard: my refactor unified the two paths; make sure
    // the workflow-trigger case still works the same way.
    const sch = new InMemorySchedulerStorage();
    const wfs = new InMemoryWorkflowStorage();
    await sch.upsertSchedule({
      id: "nightly",
      intervalMs: 86_400_000,
      enabled: true,
      metadata: { workflowName: "send-email" },
    });
    const firedAt = new Date("2026-05-02T03:00:00Z");
    await logTick(sch, "nightly", 0, firedAt);
    const wfId = scheduleTickRunId("nightly", 0);
    await wfs.createWorkflow({ workflowId: wfId, workflowName: "send-email", input: {} });
    await wfs.completeWorkflow(wfId, "sent");

    const { history } = await callHistory(sch, wfs, "nightly");
    expect(history[0]!.kind).toBe("workflow");
    expect(history[0]!.status).toBe("completed");
    expect(history[0]!.workflowName).toBe("send-email");
  });
});
