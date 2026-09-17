// ---------------------------------------------------------------------------
// compareRuns — diff two persisted runs. The store-backed twin of the
// in-memory `runMatrix` diff; both produce the same `RunDiff` type.
// ---------------------------------------------------------------------------

import { diffRuns } from "../diff.ts";
import type { RunDiff } from "../types.ts";
import type { EvalRunStore } from "./types.ts";

/** Fetch two stored runs and diff them. Throws when either id is absent. */
export async function compareRuns(
  store: EvalRunStore,
  baselineRunId: string,
  candidateRunId: string,
): Promise<RunDiff> {
  const [baseline, candidate] = await Promise.all([
    store.get(baselineRunId),
    store.get(candidateRunId),
  ]);
  if (baseline === null) {
    throw new Error(`compareRuns: baseline run "${baselineRunId}" not found`);
  }
  if (candidate === null) {
    throw new Error(`compareRuns: candidate run "${candidateRunId}" not found`);
  }
  return diffRuns(baseline.summary, candidate.summary);
}
