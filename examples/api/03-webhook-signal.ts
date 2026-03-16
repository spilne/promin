/**
 * Webhook + signals — external system triggers workflow continuation
 *
 * Business flow:
 * 1. Borrower submits a loan application with desired amount and income details
 * 2. System pulls a credit report and calculates debt-to-income ratio
 * 3. Automated eligibility rules determine if the application qualifies
 * 4. Workflow pauses and waits for a human underwriter to make the final decision
 * 5. Underwriter reviews the application in an external system and submits their verdict
 * 6. External system sends a webhook that wakes up the waiting workflow
 * 7. Loan is created and borrower is notified of approval, or borrower is notified of denial
 *
 * The workflow can wait up to 7 days for the underwriter before timing out.
 */

import {
  workflow,
  InMemoryWorkflowStorage,
  WorkflowSuspendedError,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Loan application workflow
// ---------------------------------------------------------------------------

interface LoanInput {
  applicationId: string;
  userId: string;
  amount: number;
  purpose: string;
  annualIncome: number;
}

const loanApplication = workflow<LoanInput>({
  name: "loan-application",
  storage,
  type: "lending",
  metadata: { team: "credit" },
})
  // Automated credit check
  .stepAsync("credit-check", async ({ input }) => {
    // const report = await creditBureau.pull(input.userId);
    const creditScore = 720; // simulated
    return {
      creditScore,
      debtToIncome: 0.28,
      riskCategory: creditScore > 700 ? "low" : creditScore > 600 ? "medium" : "high",
    };
  })

  // Automated eligibility rules
  .stepAsync("eligibility-check", async ({ prev, input }) => {
    const eligible =
      prev.creditScore > 580 &&
      prev.debtToIncome < 0.43 &&
      input.amount <= input.annualIncome * 5;

    return {
      eligible,
      maxApproved: eligible ? Math.min(input.amount, input.annualIncome * 4) : 0,
      reasons: eligible ? [] : ["Does not meet credit criteria"],
    };
  })

  // If eligible, wait for underwriter decision (human in the loop)
  .waitForSignal<{ approved: boolean; approvedAmount: number; underwriterId: string }>(
    "underwriter-decision",
    {
      signalName: "underwriter-approval",
      timeoutMs: 7 * 24 * 60 * 60 * 1000, // 7 day timeout
    },
  )

  // Process the decision
  .stepAsync("process-decision", async ({ prev, input }) => {
    const decision = prev as any;
    if (decision.approved) {
      // await loanService.create({ applicationId: input.applicationId, amount: decision.approvedAmount });
      // await notificationService.send(input.userId, "loan-approved", { amount: decision.approvedAmount });
      return {
        status: "approved",
        amount: decision.approvedAmount,
        applicationId: input.applicationId,
      };
    } else {
      // await notificationService.send(input.userId, "loan-denied");
      return {
        status: "denied",
        amount: 0,
        applicationId: input.applicationId,
      };
    }
  })
  .build();

// ---------------------------------------------------------------------------
// API routes
// ---------------------------------------------------------------------------

// POST /loans/apply — submit loan application
async function handleApply(request: {
  user: { id: string };
  body: { amount: number; purpose: string; annualIncome: number };
}) {
  const applicationId = `loan_${request.user.id}_${Date.now()}`;
  const workflowId = `loan-${applicationId}`;

  const { error } = await loanApplication.runSafe({
    workflowId,
    input: {
      applicationId,
      userId: request.user.id,
      amount: request.body.amount,
      purpose: request.body.purpose,
      annualIncome: request.body.annualIncome,
    },
  });

  // Suspended at waitForSignal — waiting for underwriter
  if (error && (error as WorkflowSuspendedError).reason === "signal") {
    return {
      status: 202,
      body: {
        applicationId,
        workflowId,
        status: "pending_review",
        message: "Your application is under review. You'll be notified within 7 days.",
      },
    };
  }

  if (error) {
    return { status: 500, body: { error: "Application failed" } };
  }

  return { status: 200, body: { applicationId, status: "processed" } };
}

// POST /webhooks/underwriter — external underwriting system calls this
async function handleUnderwriterWebhook(request: {
  body: {
    applicationId: string;
    approved: boolean;
    approvedAmount: number;
    underwriterId: string;
    signature: string; // HMAC for verification
  };
}) {
  const { applicationId, approved, approvedAmount, underwriterId } = request.body;

  // Verify webhook signature
  // if (!verifyHmac(request.body, WEBHOOK_SECRET)) return { status: 401 };

  const workflowId = `loan-${applicationId}`;

  // Deliver the signal — this wakes up the waiting workflow
  await storage.deliverSignal(workflowId, "underwriter-approval", {
    approved,
    approvedAmount,
    underwriterId,
  });

  // Resume the workflow (the signal step will now complete)
  await loanApplication.runSafe({
    workflowId,
    input: {} as any, // input loaded from storage on resume
  });

  return { status: 200, body: { status: "signal_delivered", workflowId } };
}

// GET /loans/:applicationId/status — check application status
async function handleLoanStatus(request: { params: { applicationId: string } }) {
  const workflowId = `loan-${request.params.applicationId}`;
  const state = await storage.loadWorkflow(workflowId);

  if (!state) return { status: 404, body: { error: "Application not found" } };

  // Check which step we're at
  const steps = Object.entries(state.steps).map(([name, s]) => ({
    name,
    status: s.status,
  }));

  return {
    status: 200,
    body: {
      applicationId: request.params.applicationId,
      workflowStatus: state.status,
      result: state.result,
      steps,
    },
  };
}

export { loanApplication, handleApply, handleUnderwriterWebhook, handleLoanStatus };
