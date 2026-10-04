// ---------------------------------------------------------------------------
// @promin/workflow/discovery — find workflows and schedules on disk.
//
// The scanners walk a directory tree, import every module and collect the
// workflow definitions / schedule configs it exports. They need a runtime
// with Node's `fs`, `path` and `url` built-ins (Node, Bun, Deno), loaded on
// the first scan.
// ---------------------------------------------------------------------------

export {
  WorkflowScanner,
  type WorkflowScannerOptions,
  type WorkflowScanResult,
} from "./lib/discovery/workflow-scanner.ts";
export {
  ScheduleScanner,
  applyDiscoveredSchedules,
  type ScheduleScannerOptions,
  type ScheduleScanResult,
  type ApplyDiscoveredSchedulesParams,
  type ApplyDiscoveredSchedulesResult,
} from "./lib/discovery/schedule-scanner.ts";
