// ---------------------------------------------------------------------------
// Conformance fixtures for the eval registry kinds. A `VersionedRegistry`
// backend proves it stores eval suites and gates by running
// `versionedRegistryKindTestSuite` from `@promin/agent/testing` with these.
// ---------------------------------------------------------------------------

import type { VersionedRegistryKindFixture } from "@promin/agent/testing";
import { EVAL_GATE_KIND, type EvalGate, type EvalGateFilter } from "./gate.ts";
import { EVAL_SUITE_KIND } from "./kind.ts";
import type { EvalSuite, EvalSuiteFilter, EvalSuiteInput } from "./types.ts";

function suiteInput(params: { n: number; agentIds?: ReadonlyArray<string> }): EvalSuiteInput {
  return {
    description: `suite ${params.n}`,
    dataset: {
      kind: "inline",
      id: "ds",
      cases: [{ id: "c1", input: `question ${params.n}`, expected: `answer ${params.n}` }],
    },
    scorers: [{ kind: "exactMatch" }],
    ...(params.agentIds !== undefined && { metadata: { agentIds: params.agentIds } }),
  };
}

export const EVAL_SUITE_FIXTURE: VersionedRegistryKindFixture<
  EvalSuite,
  EvalSuiteFilter,
  EvalSuiteInput
> = {
  kind: EVAL_SUITE_KIND,
  sample: (n) => suiteInput({ n }),
  filtering: {
    matching: (n) => suiteInput({ n, agentIds: ["support-bot"] }),
    filter: { agentId: "support-bot" },
  },
};

export const EVAL_GATE_FIXTURE: VersionedRegistryKindFixture<EvalGate, EvalGateFilter, EvalGate> = {
  kind: EVAL_GATE_KIND,
  sample: (n) => ({ suiteId: "suite", minScore: n / 1_000 }),
  filtering: {
    matching: (n) => ({ suiteId: "release-suite", minScore: n / 1_000 }),
    filter: { suiteId: "release-suite" },
  },
};
