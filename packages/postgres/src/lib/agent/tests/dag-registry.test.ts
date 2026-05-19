// ---------------------------------------------------------------------------
// PostgresDagRegistry — runs the @promin/agent dagRegistryTestSuite
// against a real PG container.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll } from "bun:test";
import { dagRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresDagRegistry } from "../dag-registry.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_dag`;
});

dagRegistryTestSuite(() => new PostgresDagRegistry({ db: pg.db }));
