/**
 * Async workflow from API — submit and poll
 *
 * Business flow:
 * 1. Customer submits identity documents and a selfie for verification
 * 2. System validates the uploaded documents are readable and complete
 * 3. An external identity verification provider checks the documents against the selfie
 * 4. Workflow polls the provider until the check completes (can take up to 30 minutes)
 * 5. Sanctions and politically-exposed-person screenings run against the customer's name
 * 6. System approves or rejects the customer based on combined results
 * 7. Customer polls a status endpoint at any time to check progress
 *
 * A background scanner resumes sleeping workflows once the external check completes.
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

  // Run identity verification via external provider (Jumio, Onfido, etc.)
  .stepAsync("identity-check", async ({ input }) => {
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
  }, {
    retry: { maxRetries: 3, baseDelayMs: 5_000 },
  })

  // Poll external check until complete (checks every 60s, up to 30 minutes)
  .stepAsync("poll-check-result", async ({ prev }) => {
    const checkId = (prev as any).checkId;
    // Poll the external provider until the check is no longer "processing"
    // In production: Pipeline.fromPromise(() => onfido.getCheck(checkId))
    //   .pollUntil({ until: r => r.status !== "processing", intervalMs: 60_000, maxDurationMs: 30 * 60_000 })
    //   .runPromise()
    const passed = Math.random() > 0.1;
    return {
      checkId,
      passed,
      score: passed ? 0.95 : 0.3,
      reasons: passed ? [] : ["document_mismatch"],
    };
  })

  // Sanctions screening
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

  // Start workflow — will suspend at the sleep step
  const { error } = await kycVerification.runSafe({
    workflowId,
    input: { userId: request.user.id, ...request.body },
  });

  // Suspended at sleep is expected — external check takes time
  if (error && (error as any)._tag === "WorkflowSuspendedError") {
    return {
      status: 202,
      body: {
        workflowId,
        status: "processing",
        message: "Verification in progress. Check back in a few minutes.",
      },
    };
  }

  if (error) {
    return { status: 500, body: { error: "Verification failed to start" } };
  }

  return { status: 200, body: { workflowId, status: "completed" } };
}

// GET /kyc/status — poll for result
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
      return { status: 200, body: { status: "processing", message: "Still verifying..." } };
    default:
      return { status: 200, body: { status: "processing" } };
  }
}

// ---------------------------------------------------------------------------
// Background: sleep scanner resumes workflows after external checks
// ---------------------------------------------------------------------------

function startKycScanner() {
  const definitions = new Map<string, WorkflowDefinition<unknown, unknown>>([
    ["kyc-verification", kycVerification],
  ]);

  return createSleepScanner({
    storage,
    scanIntervalMs: 30_000, // check every 30 seconds
    resolveWorkflow: (name) => definitions.get(name),
    onResume: (id) => console.log(`KYC resumed: ${id}`),
    onError: (id, err) => console.error(`KYC error: ${id}`, err),
  });
}

export { kycVerification, handleSubmitKyc, handleKycStatus, startKycScanner };
