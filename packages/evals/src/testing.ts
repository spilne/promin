// @promin/evals/testing — shared storage conformance suites.
//
// Every EvalRunStore / EvalDatasetStore backend (InMemory / Sqlite /
// Postgres) runs the same suite, so a backend is correct by construction.

export { evalRunStoreTestSuite } from "./lib/storage/run-store-test-suite.ts";
export { evalDatasetStoreTestSuite } from "./lib/storage/dataset-store-test-suite.ts";
