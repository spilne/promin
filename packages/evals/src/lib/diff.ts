// ---------------------------------------------------------------------------
// diffRuns — baseline vs candidate. The pure comparison behind both
// `runMatrix` (ephemeral) and `compareRuns` (P3, persisted).
// ---------------------------------------------------------------------------

import type { CaseDiff, EvalRunSummary, RunDiff } from "./types.ts";

/** Compare two run summaries case-by-case and scorer-by-scorer. */
export function diffRuns(baseline: EvalRunSummary, candidate: EvalRunSummary): RunDiff {
  const baselineByCase = new Map(baseline.caseResults.map((c) => [c.caseId, c.passed]));

  const perCase: CaseDiff[] = [];
  const regressions: string[] = [];
  for (const candidateCase of candidate.caseResults) {
    const baselinePassed = baselineByCase.get(candidateCase.caseId);
    if (baselinePassed === undefined) continue; // case absent from baseline
    const candidatePassed = candidateCase.passed;
    const status =
      baselinePassed === candidatePassed ? "unchanged" : candidatePassed ? "improved" : "regressed";
    perCase.push({ caseId: candidateCase.caseId, baselinePassed, candidatePassed, status });
    if (status === "regressed") regressions.push(candidateCase.caseId);
  }

  const perScorerDelta: Record<string, number> = {};
  const scorerIds = new Set([
    ...Object.keys(baseline.perScorer),
    ...Object.keys(candidate.perScorer),
  ]);
  for (const scorerId of scorerIds) {
    const base = baseline.perScorer[scorerId]?.passRate ?? 0;
    const cand = candidate.perScorer[scorerId]?.passRate ?? 0;
    perScorerDelta[scorerId] = cand - base;
  }

  return {
    baseline,
    candidate,
    passRateDelta: candidate.passRate - baseline.passRate,
    perScorerDelta,
    perCase,
    regressions,
  };
}
