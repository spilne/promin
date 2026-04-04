/**
 * Async workflow from API — submit and wait for external signal
 *
 * Workflow DAG:
 *
 *   ┌─────────────────────┐
 *   │  validate-documents  │
 *   └──────────┬──────────┘
 *              ▼
 *   ┌──────────────────────┐
 *   │ submit-identity-check │──── kicks off external provider (Onfido)
 *   └──────────┬───────────┘
 *              ▼
 *   ┌──────────────────────────┐
 *   │ waitForSignal             │──── workflow SUSPENDS here (zero resources)
 *   │ "identity-check-result"   │
 *   └──────────┬───────────────┘
 *              │         ▲
 *              │         │  webhook: storage.deliverSignal(workflowId, ...)
 *              │         │
 *              │    ┌────┴──────────┐
 *              │    │ Onfido webhook │  (external — POST /webhooks/onfido)
 *              │    └───────────────┘
 *              ▼
 *   ┌─────────────────┐
 *   │ sanctions-check  │
 *   └────────┬────────┘
 *            ▼
 *   ┌─────────────────┐
 *   │    pep-check     │
 *   └────────┬────────┘
 *            ▼
 *   ┌─────────────────┐
 *   │    decision      │──── branch: approve or reject
 *   └─────────────────┘
 *
 * API endpoints:
 *
 *   POST /kyc/verify        → starts workflow, returns 202 (processing)
 *   GET  /kyc/status        → returns current workflow state
 *   POST /webhooks/onfido   → receives provider result, delivers signal
 *
 * The workflow uses `waitForSignal` — it suspends (uses zero resources) until the
 * external provider sends a webhook. No polling, no sleep loops.
 */

import {
  workflow,
  Pipeline,
  InMemoryWorkflowStorage,
  type WorkflowDefinition,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// KYC verification workflow — runs in background
// ---------------------------------------------------------------------------

interface KycInput {
  userId: string;
  fullName: string;
  dateOfBirth: string;
  documentType: "passport" | "drivers_license" | "national_id";
  documentUrl: string;
  selfieUrl: string;
  country: string;
}

interface CheckResult {
  checkId: string;
  passed: boolean;
  score: number;
  reasons: string[];
}

const kycVerification = workflow<KycInput>({
  name: "kyc-verification",
  storage,
  type: "compliance",
  metadata: { team: "trust-safety" },
  retry: { maxRetries: 2, baseDelayMs: 30_000 },
})
  // Validate submitted documents
  .stepAsync("validate-documents", async ({ input }) => {
    // await documentService.validate(input.documentUrl);
    // await documentService.validate(input.selfieUrl);
    return { documentsValid: true, submittedAt: new Date().toISOString() };
  })

  // Kick off identity verification via external provider (Onfido, Jumio, etc.)
  .stepAsync(
    "submit-identity-check",
    async ({ input }) => {
      // const result = await onfido.createCheck({
      //   applicantId: input.userId,
      //   document: input.documentUrl,
      //   selfie: input.selfieUrl,
      // });
      return {
        checkId: `check_${Date.now()}`,
        provider: "onfido",
        status: "processing",
      };
    },
    { retry: { maxRetries: 3, baseDelayMs: 5_000 } },
  )

  // Wait for the external provider to complete the check.
  // The workflow suspends here — uses zero resources — until a webhook
  // delivers the signal. Timeout after 30 minutes if no signal arrives.
  //
  // How it works:
  //   1. Workflow reaches this step and suspends (status: "waiting_for_signal")
  //   2. External provider finishes the check and sends a webhook to our API
  //   3. Webhook handler calls storage.deliverSignal(workflowId, "identity-check-result", payload)
  //   4. Next workflow resume picks up the signal and continues
  .waitForSignal<CheckResult>("wait-for-check-result", {
    signalName: "identity-check-result",
    timeoutMs: 30 * 60_000, // 30 minutes
  })

  // Sanctions screening (runs after identity check completes)
  .stepAsync("sanctions-check", async ({ input }) => {
    // await sanctionsDb.screen(input.fullName, input.dateOfBirth, input.country);
    return { sanctionsClean: true, screenedAt: new Date().toISOString() };
  })

  // PEP (Politically Exposed Person) check
  .stepAsync("pep-check", async ({ input }) => {
    // await pepDatabase.check(input.fullName, input.country);
    return { pepClean: true };
  })

  // Final decision
  .branch("decision", {
    condition: (prev) => prev.pepClean,
    ifTrue: ({ input }): Pipeline<{ approved: boolean; userId: string; tier: string }, never> =>
      Pipeline.fromPromise(async () => {
        // await userService.updateKycStatus(input.userId, "approved");
        // await notificationService.send(input.userId, "kyc-approved");
        return { approved: true, userId: input.userId, tier: "verified" };
      }),
    ifFalse: ({ input }): Pipeline<{ approved: boolean; userId: string; tier: string }, never> =>
      Pipeline.fromPromise(async () => {
        // await userService.updateKycStatus(input.userId, "rejected");
        // await complianceQueue.enqueue({ type: "manual-review", userId: input.userId });
        return { approved: false, userId: input.userId, tier: "restricted" };
      }),
  })
  .build();

// ---------------------------------------------------------------------------
// API routes
// ---------------------------------------------------------------------------

// POST /kyc/verify — submit KYC, return immediately
//
// Flow:
//   Browser → POST /kyc/verify → 202 { workflowId, status: "processing" }
//   Browser → GET /kyc/status (polls) → { state: "suspended", currentStep: "wait-for-check-result" }
//   Onfido → POST /webhooks/onfido → signal delivered, workflow resumes
//   Browser → GET /kyc/status (polls) → { state: "completed", result: { approved: true } }
//
async function handleSubmitKyc(request: {
  user: { id: string };
  body: Omit<KycInput, "userId">;
}) {
  const workflowId = `kyc-${request.user.id}`;

  // Idempotent: check if already submitted
  const existing = await kycVerification.getStatus(workflowId);
  if (existing?.state === "completed") {
    return { status: 200, body: existing };
  }
  if (existing) {
    return { status: 202, body: existing };
  }

  // Start workflow — will suspend at waitForSignal step
  const { error } = await kycVerification.runSafe({
    workflowId,
    input: { userId: request.user.id, ...request.body },
  });

  if (error && (error as any)._tag !== "WorkflowSuspendedError") {
    return { status: 500, body: { error: "Verification failed to start" } };
  }

  // Return status (running or suspended — both expected)
  const status = await kycVerification.getStatus(workflowId);
  return { status: 202, body: status };
}

// POST /webhooks/onfido — receives webhook from identity provider
//
// Onfido sends this when the identity check completes. We deliver the
// result as a signal to the waiting workflow, then resume it.
//
async function handleOnfidoWebhook(request: {
  body: {
    resource_type: string;
    action: string;
    object: { id: string; status: string; result: string };
  };
}) {
  const check = request.body.object;
  const passed = check.result === "clear";

  // In production: look up workflowId by checkId from a mapping table
  const workflowId = `kyc-lookup-by-check-${check.id}`;

  // Deliver the signal — wakes up the workflow on next resume
  await storage.deliverSignal(
    workflowId,
    "identity-check-result",
    JSON.stringify({
      checkId: check.id,
      passed,
      score: passed ? 0.95 : 0.3,
      reasons: passed ? [] : [check.result],
    }),
  );

  // Resume the workflow — picks up signal, runs remaining steps
  await kycVerification.runSafe({
    workflowId,
    input: {} as KycInput, // input already stored from initial run
  });

  return { status: 200, body: { received: true } };
}

// GET /kyc/status — poll for verification progress
//
// Frontend calls this on an interval (e.g. every 5 seconds).
// Returns typed status with current step, no heavy step results by default.
//
async function handleKycStatus(request: { user: { id: string } }) {
  const workflowId = `kyc-${request.user.id}`;
  const status = await kycVerification.getStatus(workflowId);

  if (!status) {
    return { status: 404, body: { error: "No verification found" } };
  }

  return { status: 200, body: status };
}

// GET /kyc/status?details=true — include step results (for debugging/admin)
async function handleKycStatusDetailed(request: { user: { id: string } }) {
  const workflowId = `kyc-${request.user.id}`;
  const status = await kycVerification.getStatus(workflowId, { includeStepResults: true });

  if (!status) {
    return { status: 404, body: { error: "No verification found" } };
  }

  return { status: 200, body: status };
}

// ---------------------------------------------------------------------------
// WorkflowHandle — cleanest API for server-side orchestration
// ---------------------------------------------------------------------------

// The handle combines start + status + signal + result in one object.
// Use this when you control both the workflow starter and the signal sender.
//
//   const handle = await kycVerification.start(workflowId, input);
//   // ... later, from webhook:
//   await handle.signal("identity-check-result", checkPayload);
//   // ... wait for completion:
//   const result = await handle.result({ timeoutMs: 30 * 60_000 });
//
async function processKycWithHandle(userId: string, input: KycInput) {
  const handle = await kycVerification.start(`kyc-${userId}`, input);

  // At this point the workflow is running or suspended at waitForSignal.
  const status = await handle.status();
  console.log(`KYC started: ${status?.state}, step: ${status?.currentStep}`);

  // In a real app, the signal comes from a webhook handler.
  // Here we simulate it for demonstration:
  await handle.signal("identity-check-result", JSON.stringify({
    checkId: "check_123",
    passed: true,
    score: 0.95,
    reasons: [],
  }));

  // Wait for the workflow to finish (resumes + completes after signal)
  const result = await handle.result({ timeoutMs: 60_000 });
  return result;
}

// Server-side: wait for workflow to finish without handle (alternative API)
//
// Uses runSafe + waitForResult directly. Same outcome as handle.result(),
// but requires passing input again.
//
async function processKycAndWait(userId: string, input: KycInput) {
  const workflowId = `kyc-${userId}`;

  await kycVerification.runSafe({ workflowId, input });

  const result = await kycVerification.waitForResult(workflowId, {
    input,
    intervalMs: 5_000,
    timeoutMs: 30 * 60_000,
  });

  return result;
}

export {
  kycVerification,
  handleSubmitKyc,
  handleOnfidoWebhook,
  handleKycStatus,
  handleKycStatusDetailed,
  processKycWithHandle,
  processKycAndWait,
};
