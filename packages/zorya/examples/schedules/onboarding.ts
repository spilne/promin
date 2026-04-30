import type { DurableScheduleConfig } from "@promin/workflow";

export const onboardingEvery60s: DurableScheduleConfig = {
  id: "onboarding-every-60s",
  name: "Onboarding every 60s",
  intervalMs: 60_000,
  enabled: true,
  namespace: "tenant-b",
  metadata: {
    workflowName: "onboarding",
    namespace: "tenant-b",
    input: { email: "demo-user@example.com" },
  },
};
