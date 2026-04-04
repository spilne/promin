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
async function handleSubmitKyc(request: {
  user: { id: string };
  body: Omit<KycInput, "userId">;
}) {
  const workflowId = `kyc-${request.user.id}`;

  // Check if already submitted
  const existing = await storage.loadWorkflow(workflowId);
  if (existing?.status === "completed") {
    return { status: 200, body: { status: "completed", result: existing.result } };
  }
  if (existing?.status === "running" || existing?.status === "suspended") {
    return { status: 202, body: { workflowId, status: "processing" } };
  }

  // Start workflow — will suspend at waitForSignal step
  const { error } = await kycVerification.runSafe({
    workflowId,
    input: { userId: request.user.id, ...request.body },
  });

  // Suspended at waitForSignal is expected — external check takes time
  if (error && (error as any)._tag === "WorkflowSuspendedError") {
    return {
      status: 202,
      body: {
        workflowId,
        status: "processing",
        message: "Verification in progress. You'll be notified when complete.",
      },
    };
  }

  if (error) {
    return { status: 500, body: { error: "Verification failed to start" } };
  }

  return { status: 200, body: { workflowId, status: "completed" } };
}

// POST /webhooks/onfido — receives webhook from identity provider
async function handleOnfidoWebhook(request: {
  body: { resource_type: string; action: string; object: { id: string; status: string; result: string } };
}) {
  const check = request.body.object;
  const passed = check.result === "clear";

  // Find the workflow waiting for this check
  // In production: look up workflowId by checkId from a mapping table
  const workflowId = `kyc-lookup-by-check-${check.id}`;

  // Deliver the signal — this wakes up the waiting workflow
  await storage.deliverSignal(workflowId, "identity-check-result", JSON.stringify({
    checkId: check.id,
    passed,
    score: passed ? 0.95 : 0.3,
    reasons: passed ? [] : [check.result],
  }));

  // Resume the workflow (picks up the signal on next execution)
  await kycVerification.runSafe({
    workflowId,
    input: {} as KycInput, // input already stored from initial run
  });

  return { status: 200, body: { received: true } };
}

// GET /kyc/status — check verification progress
async function handleKycStatus(request: { user: { id: string } }) {
  const workflowId = `kyc-${request.user.id}`;
  const state = await storage.loadWorkflow(workflowId);

  if (!state) {
    return { status: 404, body: { error: "No verification found" } };
  }

  switch (state.status) {
    case "completed":
      return { status: 200, body: { status: "completed", result: state.result } };
    case "failed":
      return { status: 200, body: { status: "failed", error: state.error } };
    case "suspended":
      return {
        status: 200,
        body: {
          status: "processing",
          message: "Waiting for identity verification provider...",
          step: Object.entries(state.steps).find(([, s]) => s.status === "waiting_for_signal")?.[0],
        },
      };
    default:
      return { status: 200, body: { status: "processing" } };
  }
}

export { kycVerification, handleSubmitKyc, handleOnfidoWebhook, handleKycStatus };
