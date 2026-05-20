// ---------------------------------------------------------------------------
// consoleReporter — prints per-case verdicts and a run summary.
// ---------------------------------------------------------------------------

import type { EvalReporter } from "../types.ts";

export interface ConsoleReporterConfig {
  /** Sink for each line. Default `console.log`. Tests pass a collector. */
  readonly log?: (line: string) => void;
}

/** A reporter that prints results line-by-line. */
export function consoleReporter(config: ConsoleReporterConfig = {}): EvalReporter {
  const log = config.log ?? ((line: string): void => console.log(line));
  return {
    onCaseResult(result) {
      const verdict = result.passed ? "PASS" : "FAIL";
      log(`  ${verdict}  ${result.caseId}  (passRate ${result.passRate.toFixed(2)})`);
    },
    onComplete(summary) {
      const tag = summary.targetVersion
        ? `${summary.targetId}@${summary.targetVersion}`
        : summary.targetId;
      log(`\n${tag} on ${summary.datasetId}`);
      log(
        `  ${summary.totalCases} case(s) · passRate ${summary.passRate.toFixed(2)} · ` +
          `${summary.samplesPerCase} sample(s)/case`,
      );
      for (const [scorerId, perScorer] of Object.entries(summary.perScorer)) {
        log(
          `  ${scorerId}: mean ${perScorer.meanValue.toFixed(2)} · ` +
            `pass ${perScorer.passRate.toFixed(2)}`,
        );
      }
    },
  };
}
