// ---------------------------------------------------------------------------
// manual-approval workflow — the long-lived demo of /approvals.
//
// Same shape as approval-flow (submit -> review-with-signal -> apply), but
// keyed on a different signal name (`manual-approval`) and a different
// workflow name. The auto-signaler in demo.ts is hardcoded to
// `name: "approval-flow"`, so this workflow's suspensions are NOT auto-
// resolved — each fire stays pending until an operator approves or rejects
// it from the dashboard's /approvals view.
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

    // Wait for an operator to deliver the `manual-approval` signal from the
    // dashboard. No auto-signaler targets this workflow, so the suspension
    // is long-lived and the run shows up in /approvals as actually pending.
    const decision = yield* ctx.signal<{ approved: boolean; by?: string }>("manual-approval");

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
