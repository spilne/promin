/**
 * Approval workflow with human-in-the-loop.
 * Submit request → wait for manager signal → process or reject.
 * The workflow suspends durably — survives server restarts.
 */

import { workflow, InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const approval = workflow<{ requestId: string; requestedBy: string }>({
  name: "approval-flow",
})
  .stepAsync("submit", async ({ input }) => {
    await notifyManager(input.requestId, input.requestedBy);
    return { submitted: true };
  })
  .waitForSignal<{ approved: boolean; approvedBy: string }>("wait-for-approval", {
    signalName: "manager-decision",
    timeoutMs: 24 * 60 * 60_000, // 24 hours
  })
  .stepAsync("process", async ({ prev }) => {
    if (!prev.approved) {
      return { status: "rejected" as const, by: prev.approvedBy };
    }
    await fulfillRequest();
    return { status: "approved" as const, by: prev.approvedBy };
  })
  .build();

// Start the workflow — suspends at waitForSignal
const handle = await runner.start({
  workflow: approval,
  workflowId: "req-001",
  input: { requestId: "req-001", requestedBy: "alice" },
});

// Later, when manager approves (e.g. from a webhook):
await handle.signal("manager-decision", { approved: true, approvedBy: "bob" });

// Workflow resumes and completes
const result = await handle.result();
console.log(result); // { status: "approved", by: "bob" }

// Stubs
async function notifyManager(_reqId: string, _by: string) {}
async function fulfillRequest() {}
