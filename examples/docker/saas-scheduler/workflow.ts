// ---------------------------------------------------------------------------
// SaaS scheduler workflow — priority worker tier demo.
//
// Two workflow definitions with different routing:
//
//   premium-job:
//     validate-job       (default — any worker)
//          │
//          ▼
//     execute-premium-job  (needs: premium — only premium workers)
//          │
//          ▼
//     store-result       (default)
//          │
//          ▼
//     notify-webhook     (default)
//
//   standard-job:
//     validate-job       (default)
//          │
//          ▼
//     execute-standard-job (default — default workers)
//          │
//          ▼
//     store-result       (default)
//          │
//          ▼
//     notify-webhook     (default)
//
// Routing: execute-premium-job is only claimed by worker-premium
// (capabilities=["premium"]). execute-standard-job is claimed by
// worker-default (capabilities=[]). Premium tenants never wait behind
// standard work.
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, type StepHandler } from "@promin/workflow";

export interface TenantJob {
  readonly tenantId: string;
  readonly jobId: string;
  readonly tier: "premium" | "standard";
  readonly payload: Record<string, unknown>;
}

export interface ValidationResult {
  readonly valid: boolean;
  readonly tenantId: string;
}

export interface ExecutionResult {
  readonly jobId: string;
  readonly output: string;
  readonly durationMs: number;
}

export interface StoreResult {
  readonly storedAt: string;
  readonly resultKey: string;
}

export interface WebhookResult {
  readonly notifiedAt: string;
  readonly webhookUrl: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function buildPremiumJobWorkflow() {
  return workflow<TenantJob>({
    name: "premium-job",
    version: "1",
  })
    .stepAsync("validate-job", async (): Promise<ValidationResult> => {
      throw new Error("placeholder — worker runs the real handler");
    })
    .stepAsync(
      "execute-premium-job",
      { dependsOn: ["validate-job"] },
      async (): Promise<ExecutionResult> => {
        throw new Error("placeholder");
      },
      { needs: ["premium"] },
    )
    .stepAsync(
      "store-result",
      { dependsOn: ["execute-premium-job"] },
      async (): Promise<StoreResult> => {
        throw new Error("placeholder");
      },
    )
    .stepAsync(
      "notify-webhook",
      { dependsOn: ["store-result"] },
      async (): Promise<WebhookResult> => {
        throw new Error("placeholder");
      },
    )
    .build();
}

export function buildStandardJobWorkflow() {
  return workflow<TenantJob>({
    name: "standard-job",
    version: "1",
  })
    .stepAsync("validate-job", async (): Promise<ValidationResult> => {
      throw new Error("placeholder");
    })
    .stepAsync(
      "execute-standard-job",
      { dependsOn: ["validate-job"] },
      async (): Promise<ExecutionResult> => {
        throw new Error("placeholder");
      },
    )
    .stepAsync(
      "store-result",
      { dependsOn: ["execute-standard-job"] },
      async (): Promise<StoreResult> => {
        throw new Error("placeholder");
      },
    )
    .stepAsync(
      "notify-webhook",
      { dependsOn: ["store-result"] },
      async (): Promise<WebhookResult> => {
        throw new Error("placeholder");
      },
    )
    .build();
}

// ---------------------------------------------------------------------------
// Step handlers — shared across tiers where step names match.
// ---------------------------------------------------------------------------

export const validateJobHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { tenantId, jobId, tier } = ctx.input as TenantJob;
    console.log(`[validate-job] ${ctx.workflowId} — tenant=${tenantId} job=${jobId} tier=${tier}`);
    await sleep(50);
    return { valid: true, tenantId } satisfies ValidationResult;
  });

export const executePremiumJobHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { tenantId, jobId } = ctx.input as TenantJob;
    const start = Date.now();
    console.log(`[execute-premium] ${ctx.workflowId} — [PREMIUM] tenant=${tenantId} job=${jobId}`);
    // Premium: fast dedicated worker, ~500ms execution
    await sleep(500);
    const durationMs = Date.now() - start;
    console.log(`[execute-premium] ${ctx.workflowId} — done in ${durationMs}ms`);
    return {
      jobId,
      output: `premium-result-${jobId}`,
      durationMs,
    } satisfies ExecutionResult;
  });

export const executeStandardJobHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { tenantId, jobId } = ctx.input as TenantJob;
    const start = Date.now();
    console.log(
      `[execute-standard] ${ctx.workflowId} — [standard] tenant=${tenantId} job=${jobId}`,
    );
    // Standard: shared worker pool, ~1.5s execution
    await sleep(1_500);
    const durationMs = Date.now() - start;
    console.log(`[execute-standard] ${ctx.workflowId} — done in ${durationMs}ms`);
    return {
      jobId,
      output: `standard-result-${jobId}`,
      durationMs,
    } satisfies ExecutionResult;
  });

export const storeResultHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { tenantId, jobId } = ctx.input as TenantJob;
    const storedAt = new Date().toISOString();
    const resultKey = `results/${tenantId}/${jobId}`;
    console.log(`[store-result] ${ctx.workflowId} — stored at ${resultKey}`);
    await sleep(80);
    return { storedAt, resultKey } satisfies StoreResult;
  });

export const notifyWebhookHandler: StepHandler = (ctx) =>
  Pipeline.fromPromise(async () => {
    const { tenantId, jobId } = ctx.input as TenantJob;
    const notifiedAt = new Date().toISOString();
    const webhookUrl = `https://webhooks.example.com/${tenantId}/jobs/${jobId}`;
    console.log(`[notify-webhook] ${ctx.workflowId} — POST ${webhookUrl}`);
    await sleep(60);
    return { notifiedAt, webhookUrl } satisfies WebhookResult;
  });
