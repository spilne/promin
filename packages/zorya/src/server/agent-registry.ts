// ---------------------------------------------------------------------------
// Agent folder-scan registry — thin wrapper around `@promin/agent`'s
// `AgentScanner` so demo + dashboard hosts can mirror the existing
// `scanWorkflowsFolder(...)` ergonomics. New consumers should prefer
// `AgentScanner` directly when they want to share one configured instance
// across server and worker processes.
// ---------------------------------------------------------------------------

import { AgentScanner, type AgentScannerOptions, type AgentScanResult } from "@promin/agent";

export type AgentScanOptions = AgentScannerOptions;
export type AgentScanFolderResult = AgentScanResult;

export async function scanAgentsFolder(
  root: string,
  options: AgentScanOptions = {},
): Promise<AgentScanFolderResult> {
  return await AgentScanner.scanFolder(root, options);
}
