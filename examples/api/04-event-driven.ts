/**
 * Event-driven workflows — stream triggers from queues
 *
 * Business flow:
 * 1. New user signup events arrive from a message queue
 * 2. Each event triggers an onboarding workflow: billing account is created in Stripe
 * 3. Resources are provisioned based on the customer's plan (free, pro, enterprise)
 * 4. A default workspace is created for the new user
 * 5. Referral credit is applied if the user signed up with a referral code (failure is non-blocking)
 * 6. Welcome email with a getting-started guide is sent
 * 7. Signup is tracked in analytics
 *
 * Multiple signups are processed concurrently, with deduplication to prevent double-onboarding.
 */

import {
  workflow,

  StreamPipeline,
  InMemoryWorkflowStorage,
  trigger,
  WorkflowResult,
} from "@promin/core";

const storage = new InMemoryWorkflowStorage();

// ---------------------------------------------------------------------------
// Onboarding workflow
// ---------------------------------------------------------------------------

interface OnboardInput {
  userId: string;
  email: string;
  plan: "free" | "pro" | "enterprise";
  referralCode?: string;
}

const onboardUser = workflow<OnboardInput>({
  name: "onboard-user",
  storage,
  type: "onboarding",
  retry: { maxRetries: 2, baseDelayMs: 5_000 },
})
  // Create account in billing system
  .stepAsync("setup-billing", async ({ input }) => {
    // await stripe.customers.create({ email: input.email, metadata: { plan: input.plan } });
    return { customerId: `cus_${input.userId}`, plan: input.plan };
  }, {
    compensate: async ({ result }) => {
      // await stripe.customers.del((result as any).customerId);
      console.log(`Deleted billing customer ${(result as any).customerId}`);
    },
  })

  // Provision resources based on plan
  .stepAsync("provision", async ({ prev, input }) => {
    const limits = {
      free: { storage: "1GB", apiCalls: 1000 },
      pro: { storage: "100GB", apiCalls: 100_000 },
      enterprise: { storage: "unlimited", apiCalls: -1 },
    };
    // await resourceService.provision(input.userId, limits[input.plan]);
    return { ...limits[input.plan], customerId: prev.customerId };
  })

  // Create default workspace
  .stepAsync("create-workspace", async ({ input }) => {
    // await workspaceService.create({ ownerId: input.userId, name: "My Workspace" });
    return { workspaceId: `ws_${input.userId}`, name: "My Workspace" };
  })

  // Apply referral credit if applicable
  .stepAsync("apply-referral", async ({ input }) => {
    if (!input.referralCode) return { credited: false };
    // await referralService.apply(input.userId, input.referralCode);
    return { credited: true, code: input.referralCode };
  }, {
    onFailure: "skip", // referral failure shouldn't block onboarding
  })

  // Send welcome email with getting-started guide
  .stepAsync("send-welcome", async ({ input, prev }) => {
    // await emailService.send({
    //   to: input.email,
    //   template: "welcome",
    //   data: { plan: input.plan, workspaceId: (prev as any).workspaceId },
    // });
    return { emailSent: true };
  })

  // Track in analytics
  .stepAsync("track-signup", async ({ input }) => {
    // await analytics.track("user_onboarded", {
    //   userId: input.userId,
    //   plan: input.plan,
    //   hasReferral: !!input.referralCode,
    // });
    return { tracked: true };
  }, {
    onFailure: "skip", // analytics failure shouldn't block onboarding
  })
  .build();

// ---------------------------------------------------------------------------
// Event-driven: process signup events from a queue
// ---------------------------------------------------------------------------

interface SignupEvent {
  type: "user.signup";
  userId: string;
  email: string;
  plan: "free" | "pro" | "enterprise";
  referralCode?: string;
  timestamp: string;
}

async function processSignupStream(events: SignupEvent[]) {
  const results = await StreamPipeline.fromIterable(events)
    .filter((e) => e.type === "user.signup")
    .through(
      trigger({
        workflow: onboardUser,
        toInput: (event) => ({
          userId: event.userId,
          email: event.email,
          plan: event.plan,
          referralCode: event.referralCode,
        }),
        toWorkflowId: (event) => `onboard-${event.userId}`,
        concurrency: 5,
        onDuplicate: "skip", // same userId won't trigger twice
      }),
    )
    .collect();

  // Report results
  const completed = results.filter(WorkflowResult.isCompleted);
  const failed = results.filter(WorkflowResult.isFailed);
  const skipped = results.filter(WorkflowResult.isSkipped);

  console.log(`Onboarding: ${completed.length} completed, ${failed.length} failed, ${skipped.length} skipped`);

  return { completed: completed.length, failed: failed.length, skipped: skipped.length };
}

// ---------------------------------------------------------------------------
// API route: manual trigger (for admin re-runs)
// ---------------------------------------------------------------------------

async function handleManualOnboard(request: {
  admin: { id: string };
  body: OnboardInput;
}) {
  const { data, error } = await onboardUser.runSafe({
    workflowId: `onboard-${request.body.userId}`,
    input: request.body,
  });

  if (error) {
    return { status: 500, body: { error: "Onboarding failed", details: (error as Error).message } };
  }

  return { status: 200, body: { status: "onboarded", result: data } };
}

export { onboardUser, processSignupStream, handleManualOnboard };
