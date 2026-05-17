// ---------------------------------------------------------------------------
// PostgresToolHistoryStore — runs the @promin/agent toolHistoryStoreTestSuite
// against a real PG container.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll } from "bun:test";
import { toolHistoryStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresToolHistoryStore } from "../tool-history-store.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_tool_history`;
});

toolHistoryStoreTestSuite(() => new PostgresToolHistoryStore({ db: pg.db }));
