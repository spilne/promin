// ---------------------------------------------------------------------------
// composeRunId — deterministic identity for a stored run.
// ---------------------------------------------------------------------------

import type { EvalRunSummary } from "../types.ts";

/**
 * Deterministic id for a run — `targetId::targetVersion::datasetId::ranAt`.
 * Two runs with the same identity collapse to one stored row, so saving is
 * idempotent and re-running a suite overwrites rather than duplicates.
 */
export function composeRunId(summary: EvalRunSummary): string {
  return [summary.targetId, summary.targetVersion ?? "", summary.datasetId, summary.ranAt].join(
    "::",
  );
}
