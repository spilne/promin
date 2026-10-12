// Run the demo agents' eval suites with scripted LLMs and fail on a score
// below the thresholds checked in next to this file. Deterministic and
// offline; CI runs it as `bun run evals:ci`.
//
//   bun run evals:ci                      # every suite
//   bun run evals:ci --suite weather-bot-trajectory
//   bun run evals:ci --report report.json # also write the run summaries

import path from "node:path";
import {
  consoleReporter,
  evalSummaryFailures,
  runEvalSuite,
  type EvalAssertion,
  type EvalRunSummary,
  type EvalSuite,
  type EvalTarget,
} from "@promin/evals";
import { EVAL_SUITE_KIND } from "@promin/evals";
import { agentEvalTarget, createEvalHarness, memoryFactsTarget } from "./harness.ts";
import {
  KNOWLEDGE_BOT_RETRIEVAL,
  MEMORY_FACT_PRECISION,
  WEATHER_BOT_TRAJECTORY,
} from "./suites.ts";
import type { EvalSuiteSeed } from "../../src/index.ts";

const thresholds = (await Bun.file(path.join(import.meta.dir, "thresholds.json")).json()) as Record<
  string,
  EvalAssertion
>;

const harness = await createEvalHarness();
const suites: ReadonlyArray<{ readonly seed: EvalSuiteSeed; readonly target: EvalTarget }> = [
  {
    seed: KNOWLEDGE_BOT_RETRIEVAL,
    target: agentEvalTarget({ harness, agentId: "knowledge-bot" }),
  },
  { seed: WEATHER_BOT_TRAJECTORY, target: agentEvalTarget({ harness, agentId: "weather-bot" }) },
  { seed: MEMORY_FACT_PRECISION, target: memoryFactsTarget({ harness }) },
];

const only = argValue("--suite");
const reportPath = argValue("--report");
const failures: string[] = [];
const summaries: Record<string, EvalRunSummary> = {};

for (const { seed, target } of suites) {
  if (only !== undefined && seed.id !== only) continue;
  const assertion = thresholds[seed.id];
  if (assertion === undefined) {
    failures.push(`${seed.id}: no thresholds in thresholds.json`);
    continue;
  }
  const { id, ...input } = seed;
  // Normalized and validated exactly as the suite registry would store it.
  const suite: EvalSuite = EVAL_SUITE_KIND.prepare({ id, input });
  console.log(`\n== ${id}`);
  const {
    summaries: [summary],
  } = await runEvalSuite({
    suite,
    targets: [target],
    reporters: [consoleReporter()],
  });
  if (summary === undefined) {
    failures.push(`${id}: the run produced no summary`);
    continue;
  }
  summaries[id] = summary;
  for (const failure of evalSummaryFailures({ summary, assertion })) {
    failures.push(`${id}: ${failure}`);
  }
}

if (Object.keys(summaries).length === 0 && failures.length === 0) {
  failures.push(only !== undefined ? `no suite named "${only}"` : "no eval suite ran");
}
if (reportPath !== undefined) {
  await Bun.write(reportPath, `${JSON.stringify(summaries, null, 2)}\n`);
}
if (failures.length > 0) {
  console.error(`\nEval regressions:\n  - ${failures.join("\n  - ")}`);
  process.exit(1);
}
console.log(`\nAll ${Object.keys(summaries).length} eval suites meet their thresholds.`);

function argValue(flag: string): string | undefined {
  const at = process.argv.indexOf(flag);
  return at >= 0 ? process.argv[at + 1] : undefined;
}
