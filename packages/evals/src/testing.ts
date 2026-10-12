// @promin/evals/testing — shared storage conformance suites.
//
// Every EvalRunStore / EvalDatasetStore backend (InMemory / Sqlite /
// Postgres) runs the same suite, so a backend is correct by construction.
// Eval suite and gate registries are `VersionedRegistry`s: their backends
// run `versionedRegistryKindTestSuite` from `@promin/agent/testing` with
// `EVAL_SUITE_FIXTURE` / `EVAL_GATE_FIXTURE`.

export { evalRunStoreTestSuite } from "./lib/storage/run-store-test-suite.ts";
export { evalDatasetStoreTestSuite } from "./lib/storage/dataset-store-test-suite.ts";
export { EVAL_GATE_FIXTURE, EVAL_SUITE_FIXTURE } from "./lib/suite/registry-fixtures.ts";
