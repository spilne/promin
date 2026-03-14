/**
 * Durable sleep — workflows that sleep for days, weeks, or months
 *
 * workflow.sleep() suspends the workflow and stores a wakeAt timestamp
 * in Postgres. The process doesn't need to stay running — a SleepScanner
 * periodically checks for expired sleeps and resumes them.
 *
 * This example: insurance policy renewal with reminders over 120 days.
 */

import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  createSleepScanner,
  type WorkflowDefinition,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Policy renewal workflow — runs over 120 days
// ---------------------------------------------------------------------------

interface PolicyInput {
  policyId: string;
  customerId: string;
  email: string;
  expiresAt: string;
  premium: number;
}

const policyRenewal = workflow<PolicyInput>({
  name: "policy-renewal",
  storage,
  type: "insurance",
  metadata: { team: "underwriting" },
})
  // Day 0: Policy sold — record in system
  .stepAsync("record-policy", async ({ input }) => {
    console.log(`[Day 0] Policy ${input.policyId} created, expires ${input.expiresAt}`);
    return { policyId: input.policyId, premium: input.premium };
  })

  // Day 90: First reminder — 30 days before expiry
  .sleep("wait-90-days", 90 * 24 * 60 * 60 * 1000)

  .stepAsync("first-reminder", async ({ input }) => {
    console.log(`[Day 90] Sending 30-day renewal reminder to ${input.email}`);
    // await email.send({ to: input.email, template: "renewal-reminder-30d", ... });
    return { reminderSent: "30-day" };
  })

  // Day 111: Second reminder — 9 days before expiry
  .sleep("wait-21-days", 21 * 24 * 60 * 60 * 1000)

  .stepAsync("second-reminder", async ({ input }) => {
    console.log(`[Day 111] Sending urgent 9-day reminder + SMS to ${input.customerId}`);
    // await email.send({ to: input.email, template: "renewal-urgent-9d", ... });
    // await sms.send(input.customerId, "Your policy expires in 9 days!");
    return { reminderSent: "9-day" };
  })

  // Day 118: Final warning — 2 days before expiry
  .sleep("wait-7-days", 7 * 24 * 60 * 60 * 1000)

  .stepAsync("final-warning", async ({ input }) => {
    console.log(`[Day 118] Final warning — escalating to agent`);
    // await assignmentQueue.publish({ type: "renewal-escalation", policyId: input.policyId });
    return { escalated: true };
  })

  // Day 120: Check if customer renewed
  .sleep("wait-2-days", 2 * 24 * 60 * 60 * 1000)

  .stepAsync("check-renewal", async ({ input }) => {
    // const policy = await policyDb.get(input.policyId);
    const renewed = Math.random() > 0.3; // simulate 70% renewal rate
    console.log(`[Day 120] Policy ${input.policyId}: ${renewed ? "RENEWED" : "LAPSED"}`);
    return { renewed, policyId: input.policyId };
  })

  .branch("handle-outcome", {
    condition: (result) => result.renewed,
    ifTrue: ({ prev }): Pipeline<{ outcome: string; policyId: string }, never> =>
      Pipeline.succeed({ outcome: "renewed", policyId: prev.policyId }),
    ifFalse: ({ prev }): Pipeline<{ outcome: string; policyId: string }, never> => {
      console.log(`Policy ${prev.policyId} lapsed — notifying compliance`);
      return Pipeline.succeed({ outcome: "lapsed", policyId: prev.policyId });
    },
  })
  .build();

// ---------------------------------------------------------------------------
// Sleep scanner — resumes workflows after sleeps expire
// ---------------------------------------------------------------------------

function startSleepScanner() {
  const definitions = new Map<string, WorkflowDefinition<unknown, unknown>>([
    ["policy-renewal", policyRenewal],
  ]);

  const scanner = createSleepScanner({
    storage,
    scanIntervalMs: 10_000, // check every 10 seconds (production: 60s)
    resolveWorkflow: (name) => definitions.get(name),
    onResume: (id) => console.log(`Scanner resumed: ${id}`),
    onError: (id, err) => console.error(`Scanner error: ${id}`, err),
  });

  return scanner;
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

async function main() {
  // Start a renewal workflow (would be triggered by a "policy sold" event)
  console.log("Starting policy renewal workflow...\n");

  const { error } = await policyRenewal.runSafe({
    workflowId: "renewal-POL-2026-001",
    input: {
      policyId: "POL-2026-001",
      customerId: "CUST-42",
      email: "alice@example.com",
      expiresAt: "2026-08-01",
      premium: 1200,
    },
  });

  // Workflow suspends at first sleep — this is expected
  if (error && (error as any)._tag === "WorkflowSuspendedError") {
    console.log("\nWorkflow suspended — sleeping for 90 days.");
    console.log("The scanner will resume it when the sleep expires.");
    console.log("Process can restart, deploy, crash — sleep is in Postgres.\n");
  }

  // In production: start the scanner as a background service
  // const scanner = startSleepScanner();
  // await scanner.start();

  // Check workflow state
  const state = await storage.loadWorkflow("renewal-POL-2026-001");
  console.log("Current status:", state?.status);
  console.log("Steps:", Object.entries(state?.steps ?? {}).map(
    ([name, s]) => `${name}: ${s.status}${s.wakeAt ? ` (wake: ${s.wakeAt.toISOString()})` : ""}`
  ));
}

export { policyRenewal, startSleepScanner, main };
