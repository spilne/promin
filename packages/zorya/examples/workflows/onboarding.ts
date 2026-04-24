// ---------------------------------------------------------------------------
// onboarding workflow — diamond DAG: sequential setup, parallel provisioning,
// sequential finalize.
//
// DAG:
//                                ┌─▶ provision-db ──┐
//    validate-email ──▶ create-user ──▶ provision-storage ─┬─▶ send-welcome ─▶ audit
//                                └─▶ send-invites ──┘
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface OnboardingInput {
  email: string;
  orgId?: string;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

export const onboardingWorkflow = workflow<OnboardingInput>({
  name: "onboarding",
  type: "platform",
})
  .stepAsync("validate-email", async ({ input }) => {
    await sleep(delay(1_500, 4_000));
    if (!input.email.includes("@")) throw new Error("Invalid email");
    return { email: input.email };
  })
  .stepAsync("create-user", { dependsOn: ["validate-email"] }, async ({ deps }) => {
    await sleep(delay(3_000, 8_000));
    return {
      userId: `u-${Math.floor(Math.random() * 1_000_000)}`,
      email: deps["validate-email"].email,
    };
  })
  .stepAsync("provision-db", { dependsOn: ["create-user"] }, async ({ deps }) => {
    await sleep(delay(4_000, 12_000));
    if (Math.random() < 0.05) throw new Error("DB pool exhausted");
    return { userId: deps["create-user"].userId, dbShard: "shard-us-east-2" };
  })
  .stepAsync("provision-storage", { dependsOn: ["create-user"] }, async ({ deps }) => {
    await sleep(delay(3_000, 10_000));
    return { userId: deps["create-user"].userId, bucketName: `user-${deps["create-user"].userId}` };
  })
  .stepAsync("send-invites", { dependsOn: ["create-user"] }, async ({ deps }) => {
    await sleep(delay(2_000, 7_000));
    return { userId: deps["create-user"].userId, invitesSent: Math.floor(Math.random() * 5) };
  })
  .stepAsync(
    "send-welcome",
    { dependsOn: ["provision-db", "provision-storage", "send-invites"] },
    async ({ deps }) => {
      await sleep(delay(1_500, 5_000));
      return { userId: deps["provision-db"].userId, welcomeSent: true };
    },
  )
  .stepAsync("audit", { dependsOn: ["send-welcome"] }, async ({ deps }) => {
    await sleep(delay(1_000, 3_000));
    return { userId: deps["send-welcome"].userId, auditedAt: new Date().toISOString() };
  })
  .build();
