# @promin/evals

Agent evaluation for promin: run a dataset of cases against a target, score
every output, keep the run history, version eval suites like agent recipes,
and gate agent promotion on the results.

Four seams, each an interface with defaults:

| Seam           | Built in                                                                                                       |
| -------------- | -------------------------------------------------------------------------------------------------------------- |
| `EvalDataset`  | `inlineDataset`, `jsonlDataset`, `storedDataset`, `traceDataset` (production threads from a `MemoryStore`)     |
| `EvalTarget`   | `fnTarget`, `agentTarget` (any `Agent`), `recipeTarget` (a registered recipe via `resolveLocalAgent`)          |
| `Scorer`       | `exactMatch`, `regexMatch`, `jsonMatch`, `setMatch`, `toolTrajectory`, `retrievalRecall`, `budget`, `llmJudge` |
| `EvalReporter` | `consoleReporter`, `jsonReporter`                                                                              |

A score is a 0..1 `value`; the runner derives `passed` from the scorer's
`threshold` (default 1), and a case passes when its pass rate across samples
reaches `passThreshold`.

## Running an eval

```ts
import { exactMatch, fnTarget, inlineDataset, runEval } from "@promin/evals";

const summary = await runEval({
  dataset: inlineDataset({
    id: "capitals",
    cases: [{ id: "fr", input: "Capital of France?", expected: "Paris" }],
  }),
  target: fnTarget({ id: "lookup", fn: (input) => answer(String(input)) }),
  scorers: [exactMatch],
  concurrency: 4, // 1..MAX_EVAL_CONCURRENCY concurrent target runs
  samplesPerCase: 3, // nondeterministic targets: a per-case pass rate
  passThreshold: 2 / 3,
  caseTimeoutMs: 30_000, // a sample past this is aborted and scored as failed
  signal, // aborting stops the run: EvalRunAbortedError with the finished cases
  reporters: [consoleReporter()], // onCaseResult fires as each case finishes
});
```

Out-of-range knobs (`concurrency: 0`, `NaN`, a threshold above 1) throw
`RangeError` instead of quietly running nothing. Timeouts go through the
`clock` (`WallClock`), so tests drive them with `FakeWallClock`.

`runMatrix` runs one dataset against several targets and diffs each against
the first (`RunDiff`: pass-rate delta, per-scorer deltas, per-case
improved / regressed). `defineEval({ name, data, target, scorers })` is the
short form for bare case arrays and functions.

### Agents as targets

```ts
const target = agentTarget({
  id: "support-bot",
  version: "v3",
  agent: () => resolveLocalAgent(recipe, deps), // a fresh agent per case
  invoke: { namespaceId: "evals", resourceId: "ci" },
});
```

The output is the agent's answer (structured output when it has a schema),
with the run's trace and token / cost metrics. `toolTrajectory` scores the
tool-call sequence; `retrievalRecall` scores the knowledge-base sources the
run retrieved against `expected.sources`; `budget` scores latency / cost /
token ceilings.

### Judges

`llmJudge({ judge, rubric })` (or `createLLMScorer` for a custom prompt)
asks an LLM for `{"value": 0..1, "reason": ...}`. The reply parser accepts
JSON, or a rating with its scale in the text (`85/100`, `8 out of 10`,
`85%`, a lone 0..1 number). Anything else — a bare `7` has no scale —
throws `JudgeParseError`, which the runner records as a failed score with
the reason instead of guessing.

## Run history

Pass `persist: { store, runId?, suiteId?, suiteVersion? }` and the run is
written as it happens: `start` (status `running`), `recordCase` as each case
finishes, `finish` with the totals (`completed`, or `cancelled` / `failed`).
`EvalRunStore` backends: `InMemoryEvalRunStore`, `SqliteEvalRunStore`
(`@promin/sqlite`), `PostgresEvalRunStore` (`@promin/postgres`). The first
ending wins: a cancelled run is never overwritten by a late completion.
`compareRuns({ store, baselineRunId, candidateRunId })` diffs two stored
runs.

## Eval suites

An `EvalSuite` is the serializable form of an eval — dataset, scorers and
run knobs as JSON — stored in a `VersionedRegistry` of kind
`EVAL_SUITE_KIND` (immutable versions, an active pointer, promote,
compare-and-set; `InMemoryEvalSuiteRegistry`, `SqliteEvalSuiteRegistry`,
`PostgresEvalSuiteRegistry`). A suite says what to measure; the target is
chosen when it runs:

```ts
const suite = await suites.register({
  id: "support-faq",
  dataset: { kind: "inline", id: "faq", cases },
  scorers: [{ kind: "llmJudge", judgeRef: "haiku", rubric: "Is it correct?" }],
  metadata: { agentIds: ["support-bot"] },
});
await runEvalSuite({ suite, targets: [v2, v3], deps: { judges }, persist: { store } });
```

## Promotion gates

An `EvalGate` (`{ suiteId, minScore, suiteVersion?, scorerId? }`, stored per
agent id in a registry of kind `EVAL_GATE_KIND`) says "a version of this
agent may become active only once the suite scored at least `minScore` on
it". `checkEvalGate({ gate, runs, agentId, version, activeSuiteVersion })`
reads the newest completed run of ONE suite version — the gate's pinned
`suiteVersion`, else the suite's currently active one — on that agent
version; that run must score at least `minScore`. A draft suite version
never satisfies a gate, and a newer failing run outweighs an older passing
one. Zorya enforces gates on every route that activates an agent version
(see its README).

A suite's `idempotentCases` (default `false`) tells a durable host whether a
case a crash interrupted may run again; `allowedTools` narrows the tools the
agent under test may use. A `stored` dataset is read by name at run time, so
a suite version does not pin its cases.

## Live scoring

`liveScored({ agent, scorers, sink, sampling: { rate }, maxConcurrent })`
scores a sample of an agent's production runs off the request path. The
rate must be in 0..1; at most `maxConcurrent` runs are scored at once and
the rest are dropped (`onDropped`), so a traffic spike never queues scorer
calls. `liveScoredFromRecipe` builds the same from a JSON recipe block.

## CI

`assertEvalSummary({ summary, assertion })` / `evalSummaryFailures` check a
run against `minPassRate`, `minPerScorer`, `minMeanPerScorer` and
`maxRegression`. The repository's own CI job (`bun run evals:ci`) runs the
demo agents' suites with scripted LLMs against checked-in thresholds — see
`packages/zorya/examples/evals`.
