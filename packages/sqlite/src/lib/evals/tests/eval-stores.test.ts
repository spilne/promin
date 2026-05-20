// ---------------------------------------------------------------------------
// SqliteEvalRunStore / SqliteEvalDatasetStore — run the @promin/evals
// storage conformance suites against a fresh in-memory SQLite db per test.
// ---------------------------------------------------------------------------

import { Database } from "bun:sqlite";
import { evalDatasetStoreTestSuite, evalRunStoreTestSuite } from "@promin/evals/testing";
import { SqliteEvalDatasetStore } from "../eval-dataset-store.ts";
import { SqliteEvalRunStore } from "../eval-run-store.ts";

evalRunStoreTestSuite(() => SqliteEvalRunStore.make({ db: new Database(":memory:") }));
evalDatasetStoreTestSuite(() => SqliteEvalDatasetStore.make({ db: new Database(":memory:") }));
