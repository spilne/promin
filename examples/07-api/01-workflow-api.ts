/**
 * REST API for workflow management — submit, poll status, receive signals.
 * Shows how workflows integrate with HTTP endpoints.
 */

import { workflow, InMemoryWorkflowStorage } from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// KYC verification workflow
const kycVerification = workflow<{ userId: string; documentUrl: string }>({
  name: "kyc-verification",
  storage,
})
  .stepAsync("submit-check", async ({ input }) => {
    const checkId = await submitToProvider(input.documentUrl);
    return { checkId };
  })
  .waitForSignal<{ passed: boolean }>("verification-result", {
    signalName: "provider-callback",
    timeoutMs: 24 * 60 * 60_000,
  })
  .stepAsync("finalize", async ({ prev }) => {
    return { verified: prev.passed };
  })
  .build();

// POST /kyc — start verification
async function handleStartKyc(
  userId: string,
  documentUrl: string,
): Promise<{ status: number; body?: unknown }> {
  const workflowId = `kyc-${userId}`;

  // Idempotent — returns existing status if already started
  const existing = await kycVerification.getStatus(workflowId);
  if (existing) return { status: 200, body: existing };

  await kycVerification.start(workflowId, { userId, documentUrl });
  return { status: 202, body: { workflowId, status: "started" } };
}

// GET /kyc/:userId/status — poll for result
async function handleGetStatus(userId: string): Promise<{ status: number; body?: unknown }> {
  const status = await kycVerification.getStatus(`kyc-${userId}`);
  if (!status) return { status: 404 };
  return { status: 200, body: status };
}

// POST /webhooks/provider — receive verification callback
async function handleProviderWebhook(checkId: string, passed: boolean) {
  const workflowId = findWorkflowByCheckId(checkId);
  await storage.deliverSignal(workflowId, "provider-callback", { passed });
  return { status: 200 };
}

// Stubs
async function submitToProvider(_url: string) {
  return "check_123";
}
function findWorkflowByCheckId(_checkId: string) {
  return "kyc-user_1";
}

export { handleStartKyc, handleGetStatus, handleProviderWebhook };
