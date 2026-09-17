// ---------------------------------------------------------------------------
// Workflow folder-scan registry — thin wrapper around `@promin/workflow`'s
// `WorkflowScanner` so existing callers (and the dashboard's hint copy in
// workflow-list.tsx) keep using `scanWorkflowsFolder(...)` without change.
//
// New code should prefer `WorkflowScanner` directly — that lets the same
// configured instance be shared between a Zorya server and a worker
// process. See `@promin/workflow` for the class itself.
// ---------------------------------------------------------------------------

import {
  WorkflowScanner,
  type WorkflowScannerOptions,
  type WorkflowScanResult,
} from "@promin/workflow";

export type ScanOptions = WorkflowScannerOptions;
export type ScanResult = WorkflowScanResult;

export async function scanWorkflowsFolder(
  root: string,
  options: ScanOptions = {},
): Promise<ScanResult> {
  return await WorkflowScanner.scanFolder(root, options);
}
