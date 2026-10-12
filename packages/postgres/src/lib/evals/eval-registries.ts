// ---------------------------------------------------------------------------
// Postgres eval registries — `PostgresVersionedRegistry` bound to the eval
// suite and promotion-gate kinds. They share the registry tables with
// agents, roles, skills, DAGs and fragments; rows are keyed by kind.
// ---------------------------------------------------------------------------

import {
  EVAL_GATE_KIND,
  EVAL_SUITE_KIND,
  type EvalGate,
  type EvalGateFilter,
  type EvalSuite,
  type EvalSuiteFilter,
  type EvalSuiteInput,
} from "@promin/evals";
import type { PostgresRegistryConfig } from "../agent/registries.ts";
import { PostgresVersionedRegistry } from "../agent/versioned-registry.ts";

export class PostgresEvalSuiteRegistry extends PostgresVersionedRegistry<
  EvalSuite,
  EvalSuiteFilter,
  EvalSuiteInput
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: EVAL_SUITE_KIND });
  }
}

export class PostgresEvalGateRegistry extends PostgresVersionedRegistry<
  EvalGate,
  EvalGateFilter,
  EvalGate
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: EVAL_GATE_KIND });
  }
}
