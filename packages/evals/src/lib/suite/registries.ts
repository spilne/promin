// ---------------------------------------------------------------------------
// In-memory eval registries — `InMemoryVersionedRegistry` with the eval
// suite / gate kind bound. SQLite and Postgres hosts bind the same kinds to
// their `VersionedRegistry` (`SqliteEvalSuiteRegistry`, …).
// ---------------------------------------------------------------------------

import { InMemoryVersionedRegistry } from "@promin/agent";
import type { WallClock } from "@promin/workflow";
import { EVAL_GATE_KIND, type EvalGate, type EvalGateFilter } from "./gate.ts";
import { EVAL_SUITE_KIND } from "./kind.ts";
import type { EvalSuite, EvalSuiteFilter, EvalSuiteInput } from "./types.ts";

export interface InMemoryEvalRegistryConfig {
  /** Time source for `createdAt`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryEvalSuiteRegistry extends InMemoryVersionedRegistry<
  EvalSuite,
  EvalSuiteFilter,
  EvalSuiteInput
> {
  constructor(config: InMemoryEvalRegistryConfig = {}) {
    super({ ...config, kind: EVAL_SUITE_KIND });
  }
}

export class InMemoryEvalGateRegistry extends InMemoryVersionedRegistry<
  EvalGate,
  EvalGateFilter,
  EvalGate
> {
  constructor(config: InMemoryEvalRegistryConfig = {}) {
    super({ ...config, kind: EVAL_GATE_KIND });
  }
}
