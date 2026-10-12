// ---------------------------------------------------------------------------
// SQLite eval registries — `SqliteVersionedRegistry` bound to the eval
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
import type { SqliteRegistryConfig } from "../agent/registries.ts";
import { SqliteVersionedRegistry } from "../agent/versioned-registry.ts";

export class SqliteEvalSuiteRegistry extends SqliteVersionedRegistry<
  EvalSuite,
  EvalSuiteFilter,
  EvalSuiteInput
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: EVAL_SUITE_KIND });
  }
}

export class SqliteEvalGateRegistry extends SqliteVersionedRegistry<
  EvalGate,
  EvalGateFilter,
  EvalGate
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: EVAL_GATE_KIND });
  }
}
