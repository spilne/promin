// ---------------------------------------------------------------------------
// Schedule folder-scan registry — thin wrapper around @promin/workflow's
// `ScheduleScanner` and `applyDiscoveredSchedules` so the legacy free-function
// API keeps working. New code should prefer the class directly.
// ---------------------------------------------------------------------------

import {
  ScheduleScanner,
  applyDiscoveredSchedules as applyDiscoveredSchedulesImpl,
  type ScheduleScannerOptions,
  type ScheduleScanResult,
  type ApplyDiscoveredSchedulesOptions as ApplyOptions,
  type ApplyDiscoveredSchedulesResult as ApplyResult,
  type SchedulerStorage,
  type DurableScheduleConfig,
} from "@promin/workflow";

export type ScheduleScanOptions = ScheduleScannerOptions;
export type {
  ScheduleScanResult,
  ApplyOptions as ApplyDiscoveredSchedulesOptions,
  ApplyResult as ApplyDiscoveredSchedulesResult,
};

export async function scanSchedulesFolder(
  root: string,
  options: ScheduleScanOptions = {},
): Promise<ScheduleScanResult> {
  return await ScheduleScanner.scanFolder(root, options);
}

export async function applyDiscoveredSchedules(
  storage: SchedulerStorage,
  schedules: readonly DurableScheduleConfig[],
  options: ApplyOptions = {},
): Promise<ApplyResult> {
  return await applyDiscoveredSchedulesImpl(storage, schedules, options);
}
