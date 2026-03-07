/**
 * Durable workflows — survive crashes, resume from checkpoints
 *
 * workflow() persists step results to storage. On crash + restart,
 * completed steps are skipped and execution continues from the
 * first incomplete step.
 */

import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  type WorkflowDefinition,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// Basic durable workflow
async function basicDurable() {
  const result = await workflow<{ userId: string }>({
    name: "onboard-user",
    storage,
    type: "onboarding",
    metadata: { team: "growth" },
  })
    .step("create-account", ({ input }) =>
      Pipeline.succeed({ accountId: `acc_${input.userId}`, email: "user@example.com" }),
    )
    .stepAsync("send-welcome", async ({ prev }) => {
      // In real code: await mailer.send(prev.email, "Welcome!");
      return { sent: true, email: prev.email };
    })
    .run({ workflowId: "onboard-u42", input: { userId: "u_42" } });

  console.log(result); // { sent: true, email: "user@example.com" }
}

// Build reusable workflow definitions
async function reusableDefinition() {
  const onboardUser: WorkflowDefinition<{ userId: string }, { sent: boolean }> = workflow<{
    userId: string;
  }>({
    name: "onboard",
    storage,
  })
    .step("create", ({ input }) => Pipeline.succeed({ id: input.userId }))
    .stepAsync("notify", async ({ prev }) => ({ sent: true }))
    .build();

  // Run multiple times with different inputs
  await onboardUser.run({ workflowId: "onboard-1", input: { userId: "u_1" } });
  await onboardUser.run({ workflowId: "onboard-2", input: { userId: "u_2" } });
}

// Resume after crash — completed steps are skipped
async function resumeAfterCrash() {
  // Simulate: step-1 completed in a previous run, then process crashed
  await storage.createWorkflow({
    workflowId: "resume-1",
    workflowName: "etl",
    input: { date: "2026-04-01" },
  });
  await storage.saveStepResult({
    workflowId: "resume-1",
    stepName: "extract",
    result: [1, 2, 3],
    durationMs: 500,
    startedAt: new Date(),
  });

  let extractCalled = false;

  const result = await workflow<{ date: string }>({ name: "etl", storage })
    .step("extract", () => {
      extractCalled = true; // This won't run — already checkpointed
      return Pipeline.succeed([1, 2, 3]);
    })
    .step("transform", ({ prev }) => Pipeline.succeed(prev.map((n) => n * 10)))
    .run({ workflowId: "resume-1", input: { date: "2026-04-01" } });

  console.log(result); // [10, 20, 30]
  console.log("extract re-executed:", extractCalled); // false — skipped
}

// Query and manage workflows
async function queryWorkflows() {
  const running = await storage.listWorkflows({ status: "running" });
  const failed = await storage.listWorkflows({ status: "failed", type: "onboarding" });
  const recent = await storage.listWorkflows({ limit: 10 });

  console.log(`Running: ${running.length}, Failed: ${failed.length}, Recent: ${recent.length}`);

  // Cancel a workflow
  // await storage.cancelWorkflow("onboard-1");

  // Load specific workflow state
  const state = await storage.loadWorkflow("onboard-1");
  if (state) {
    console.log(`Workflow ${state.workflowId}: ${state.status}`);
    console.log(`Steps:`, Object.keys(state.steps));
  }
}

export { basicDurable, reusableDefinition, resumeAfterCrash, queryWorkflows };
