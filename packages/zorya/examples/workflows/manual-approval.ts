// ---------------------------------------------------------------------------
// manual-approval workflow — the long-lived demo of /approvals.
//
// Same shape as approval-flow (submit -> review-with-signal -> apply), but
// the signal name uses the `approve:` prefix that the dashboard's
// /approvals inbox filters on (listPendingApprovals matches
// signalName.startsWith("approve:")) — so each suspended run shows up in
// the operator's approval inbox. The auto-signaler in demo.ts is
// hardcoded to `name: "approval-flow"`, so this workflow's suspensions
// are NOT auto-resolved.
//
// Tool metadata (toolName / toolInput) is normally written by agentLoop
// when an agent calls a require-approval tool; this workflow bypasses
// the agent path, so /approvals shows the row with tool "unknown" — the
// workflow name + suspendedAt + "View run" link still work and the
// signal can be resolved from the workflow run's Signals tab.
//
// Paired with the `manual-approval-hourly` schedule (see seedSchedules in
// demo.ts) so a new pending approval appears every hour, plus one right
// after demo boot via the scheduler's never-fired-yet kickstart.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow } from "@promin/workflow";

export interface ManualApprovalInput {
  requestId: number;
  requester?: string;
  description?: string;
}

function pSleep(ms: number): Pipeline<void, never> {
  return Pipeline.fromPromise(() => new Promise<void>((r) => setTimeout(r, ms)));
}

export const manualApprovalWorkflow = workflow<ManualApprovalInput>({
  name: "manual-approval",
  type: "platform",
})
  .step("submit", ({ input }) =>
    pSleep(400).map(() => ({
      requestId: input.requestId,
      submittedBy: input.requester ?? "scheduler",
      description: input.description ?? "Hourly manual-approval demo run",
    })),
  )
  .journaled("review", function* (ctx) {
    const previous = yield* ctx.activity("load-request", async () => ({
      requestId: (ctx as unknown as { prev: { requestId: number } }).prev.requestId,
    }));

    // Wait on `approve:<id>` — the prefix the /approvals inbox filters on
    // (listPendingApprovals.findSuspendedApprovalStep). No auto-signaler
    // targets this workflow, so the suspension is long-lived and the run
    // shows up in /approvals as actually pending. Resolve it by delivering
    // the same signal name from the workflow run's Signals tab.
    const decision = yield* ctx.signal<{ approved: boolean; by?: string }>(
      `approve:demo-${previous.requestId}`,
    );

    return { requestId: previous.requestId, approved: decision.approved, by: decision.by };
  })
  .step(
    "apply",
    ({ prev }) => {
      const approved = (prev as { approved: boolean }).approved;
      return pSleep(300).map(() => ({
        applied: approved,
        at: new Date().toISOString(),
      }));
    },
    { dependsOn: ["review"] },
  )
  .build();
