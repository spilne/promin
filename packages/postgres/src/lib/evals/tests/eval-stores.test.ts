// ---------------------------------------------------------------------------
// PostgresEvalRunStore / PostgresEvalDatasetStore — run the @promin/evals
// storage conformance suites against a real Postgres container.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll } from "bun:test";
import { evalDatasetStoreTestSuite, evalRunStoreTestSuite } from "@promin/evals/testing";
import { migrate } from "../../migrate.ts";
import { PostgresTestContainer } from "../../test-utils.ts";
import { PostgresEvalDatasetStore } from "../eval-dataset-store.ts";
import { PostgresEvalRunStore } from "../eval-run-store.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE eval_run`;
  await pg.sql`TRUNCATE eval_dataset`;
});

evalRunStoreTestSuite(() => new PostgresEvalRunStore({ db: pg.db }));
evalDatasetStoreTestSuite(() => new PostgresEvalDatasetStore({ db: pg.db }));
