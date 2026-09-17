// ---------------------------------------------------------------------------
// approval-flow workflow — demonstrates ctx.signal + ctx.sleep inside a
// journaled step.
//
// submit → review (journaled: sleep 5s cooldown, then wait for "approval"
// signal) → apply
//
// Dashboard exposure:
// - review step appears with stepType="journal"; while it's inside ctx.sleep
//   the workflow status is "suspended" and the step shows wakeAt.
// - While inside ctx.signal the status is "suspended" and the step shows
//   signalName="approval". The Signals tab lets you deliver the signal,
//   and the signal history is visible there.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { Pipeline } from "@promin/core";

export interface ApprovalFlowInput {
  requestId: number;
  requester?: string;
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

function pSleep(ms: number): Pipeline<void, never> {
  return Pipeline.fromPromise(() => new Promise<void>((r) => setTimeout(r, ms)));
}

export const approvalFlowWorkflow = workflow<ApprovalFlowInput>({
  name: "approval-flow",
  type: "platform",
})
  .step("submit", ({ input }) =>
    pSleep(delay(800, 2_500)).map(() => ({
      requestId: input.requestId,
      submittedBy: input.requester ?? "anon",
    })),
  )
  .journaled("review", function* (ctx) {
    const previous = yield* ctx.activity(
      "load-request",
      async () =>
        new Promise<{ requestId: number }>((r) =>
          setTimeout(
            () =>
              r({ requestId: (ctx as unknown as { prev: { requestId: number } }).prev.requestId }),
            1_000,
          ),
        ),
    );

    // Short cooldown before the review window opens.
    yield* ctx.sleep(delay(4_000, 8_000));

    // Wait for an external actor (a human, a webhook, or another workflow)
    // to deliver the "approval" signal. Resumes with whatever payload was
    // attached to the signal.
    const approval = yield* ctx.signal<{ approved: boolean; by?: string }>("approval");

    return { requestId: previous.requestId, approved: approval.approved, by: approval.by };
  })
  .step(
    "apply",
    ({ prev }) => {
      const approved = (prev as { approved: boolean }).approved;
      return pSleep(delay(800, 2_500)).map(() => ({
        applied: approved,
        at: new Date().toISOString(),
      }));
    },
    { dependsOn: ["review"] },
  )
  .build();
