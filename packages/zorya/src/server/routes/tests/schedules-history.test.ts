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
import { InMemoryWorkflowStorage } from "@promin/workflow";
import { InMemorySchedulerStorage, scheduleTickRunId } from "@promin/workflow/scheduler";
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

/** The run id tick `tickNumber` of the schedule's current incarnation dispatches. */
async function runIdOf(
  sch: InMemorySchedulerStorage,
  scheduleId: string,
  tickNumber: number,
): Promise<string> {
  const { incarnation } = (await sch.loadScheduleState(scheduleId))!;
  return scheduleTickRunId({ scheduleId, incarnation, tickNumber });
}

async function logTick(
  sch: InMemorySchedulerStorage,
  scheduleId: string,
  tickNumber: number,
  firedAt: Date,
): Promise<void> {
  const { incarnation } = (await sch.loadScheduleState(scheduleId))!;
  await sch.commitPoll({
    updates: [
      {
        id: scheduleId,
        firedAt,
        tickIncrement: 1,
        nextRun: new Date(firedAt.getTime() + 60_000),
        ticks: [{ scheduleId, scheduledAt: firedAt, firedAt, incarnation, tickNumber }],
      },
    ],
  });
}

describe("getScheduleHistory — schedule incarnations", () => {
  it("a schedule deleted and created again doesn't join the old schedule's runs", async () => {
    const sch = new InMemorySchedulerStorage();
    const wfs = new InMemoryWorkflowStorage();
    const metadata = { workflowName: "nightly-wf" };

    await sch.upsertSchedule({ id: "nightly", intervalMs: 60_000, metadata });
    await logTick(sch, "nightly", 0, new Date("2026-05-01T09:00:00Z"));
    const oldRun = await runIdOf(sch, "nightly", 0);
    await wfs.createWorkflow({ workflowId: oldRun, workflowName: "nightly-wf", input: {} });
    await wfs.failWorkflow({ workflowId: oldRun, error: "old failure" });

    await sch.deleteSchedule("nightly");
    await sch.upsertSchedule({ id: "nightly", intervalMs: 60_000, metadata });
    await logTick(sch, "nightly", 0, new Date("2026-05-02T09:00:00Z"));
    const newRun = await runIdOf(sch, "nightly", 0);
    expect(newRun).not.toBe(oldRun);

    const { history, total } = await callHistory(sch, wfs, "nightly");
    expect(total).toBe(1);
    expect(history.map((h) => [h.tickNumber, h.workflowId, h.status])).toEqual([
      [0, newRun, "pending"],
    ]);
  });
});

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
    const wfId = await runIdOf(sch, "daily-summary", 0);
    await wfs.createWorkflow({
      workflowId: wfId,
      workflowName: "summary-bot",
      input: { task: "Recap email inbox" },
    });
    await wfs.completeWorkflow({ workflowId: wfId, result: { ok: true } });

    const { history, total } = await callHistory(sch, wfs, "daily-summary");

    expect(total).toBe(1);
    expect(history).toHaveLength(1);
    const row = history[0]!;
    // `status` comes from the workflow row, not a fixed "completed" for
    // agent schedules (the next test covers a failed row).
    expect(row.status).toBe("completed");
    // Schedule provenance — the chip stays "agent" because the
    // schedule's metadata says so.
    expect(row.kind).toBe("agent");
    expect(row.workflowId).toBe(wfId);
    expect(row.workflowName).toBe("summary-bot");
  });

  it("an agent tick whose workflow row failed reports status=failed", async () => {
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
    const wfId = await runIdOf(sch, "flaky-bot", 0);
    await wfs.createWorkflow({ workflowId: wfId, workflowName: "flaky", input: {} });
    await wfs.failWorkflow({ workflowId: wfId, error: "boom" });

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
    const wfId = await runIdOf(sch, "nightly", 0);
    await wfs.createWorkflow({ workflowId: wfId, workflowName: "send-email", input: {} });
    await wfs.completeWorkflow({ workflowId: wfId, result: "sent" });

    const { history } = await callHistory(sch, wfs, "nightly");
    expect(history[0]!.kind).toBe("workflow");
    expect(history[0]!.status).toBe("completed");
    expect(history[0]!.workflowName).toBe("send-email");
  });
});
