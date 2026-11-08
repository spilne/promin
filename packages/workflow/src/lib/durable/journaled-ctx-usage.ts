// ---------------------------------------------------------------------------
// ctx.reportUsage — a usage report recorded as a journal activity, so replay
// re-reads it. The budget check runs after the entry is recorded, on every
// run of the body, so a replay trips the same cap the live run did.
// ---------------------------------------------------------------------------

import {
  normalizeUsageReport,
  USAGE_ACTIVITY_NAME,
  type UsageReport,
  type UsageTotals,
} from "./budget.ts";
import type { ActivityFn } from "./journaled-ctx-activity.ts";
import type { ActivityYield, JournaledCtxEnv } from "./journaled-context.ts";

export function makeReportUsage(params: {
  env: JournaledCtxEnv;
  activity: ActivityFn;
}): (usage: UsageReport) => Generator<ActivityYield, UsageTotals, unknown> {
  const { env, activity } = params;
  return function* reportUsage(usage) {
    const report = normalizeUsageReport(usage);
    const recorded = (yield* activity(USAGE_ACTIVITY_NAME, () => report)) as Required<UsageReport>;
    return env.usage.add(recorded);
  };
}
