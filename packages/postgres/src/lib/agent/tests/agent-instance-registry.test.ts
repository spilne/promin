// ---------------------------------------------------------------------------
// PostgresAgentInstanceRegistry — runs the @promin/agent
// agentInstanceRegistryTestSuite against a real PG container.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll } from "bun:test";
import { agentInstanceRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresAgentInstanceRegistry } from "../agent-instance-registry.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_instance`;
});

agentInstanceRegistryTestSuite(() => new PostgresAgentInstanceRegistry({ db: pg.db }));
