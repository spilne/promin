// ---------------------------------------------------------------------------
// SQLite eval stores — run the @promin/evals storage conformance suites and
// the VersionedRegistry conformance for the eval kinds against a fresh
// in-memory SQLite db per test.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { versionedRegistryKindTestSuite } from "@promin/agent/testing";
import {
  EVAL_GATE_FIXTURE,
  EVAL_SUITE_FIXTURE,
  evalDatasetStoreTestSuite,
  evalRunStoreTestSuite,
} from "@promin/evals/testing";
import { SqliteVersionedRegistry } from "../../agent/versioned-registry.ts";
import { SqliteEvalDatasetStore } from "../eval-dataset-store.ts";
import { SqliteEvalGateRegistry, SqliteEvalSuiteRegistry } from "../eval-registries.ts";
import { SqliteEvalRunStore } from "../eval-run-store.ts";

evalRunStoreTestSuite(() => SqliteEvalRunStore.make({ db: new Database(":memory:") }));
evalDatasetStoreTestSuite(() => SqliteEvalDatasetStore.make({ db: new Database(":memory:") }));
versionedRegistryKindTestSuite({
  name: "SqliteVersionedRegistry (eval suites)",
  factory: ({ kind, clock }) =>
    new SqliteVersionedRegistry({ db: new Database(":memory:"), kind, clock }),
  fixture: EVAL_SUITE_FIXTURE,
});
versionedRegistryKindTestSuite({
  name: "SqliteVersionedRegistry (eval gates)",
  factory: ({ kind, clock }) =>
    new SqliteVersionedRegistry({ db: new Database(":memory:"), kind, clock }),
  fixture: EVAL_GATE_FIXTURE,
});

describe("SqliteEvalSuiteRegistry / SqliteEvalGateRegistry", () => {
  it("share one db without seeing each other's ids", async () => {
    const db = new Database(":memory:");
    const suites = new SqliteEvalSuiteRegistry({ db });
    const gates = new SqliteEvalGateRegistry({ db });
    await suites.register({
      id: "bot",
      dataset: { kind: "inline", id: "ds", cases: [{ id: "1", input: "a" }] },
      scorers: [{ kind: "exactMatch" }],
    });
    await gates.register({ id: "bot", suiteId: "bot", minScore: 0.9 });
    expect((await suites.get({ id: "bot" }))?.dataset.kind).toBe("inline");
    expect((await gates.get({ id: "bot" }))?.minScore).toBe(0.9);
  });
});
