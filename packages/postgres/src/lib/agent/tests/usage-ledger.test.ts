// ---------------------------------------------------------------------------
// PostgresUsageLedger — runs the @promin/agent usageLedgerTestSuite against
// a real PG container. Rows are stamped by the database clock, so `age`
// shifts stored stamps back instead of advancing a fake clock.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { EMPTY_USAGE } from "@promin/agent";
import { usageLedgerTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresUsageLedger } from "../usage-ledger.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_usage_ledger`;
});

usageLedgerTestSuite(() => ({
  ledger: new PostgresUsageLedger({ db: pg.db }),
  age: async (ms) => {
    await pg.sql`UPDATE agent_usage_ledger SET recorded_at = recorded_at - (${ms} * INTERVAL '1 millisecond')`;
  },
}));

describe("PostgresUsageLedger — Postgres-specific", () => {
  it("stamps rows with the database clock", async () => {
    const ledger = new PostgresUsageLedger({ db: pg.db });
    await ledger.record({ turnId: "t1", namespaceId: "ns", usage: EMPTY_USAGE });
    const [row] = await pg.sql<Array<{ skew: number }>>`
      SELECT abs(extract(epoch FROM (NOW() - recorded_at))) AS skew FROM agent_usage_ledger`;
    expect(Number(row!.skew)).toBeLessThan(5);
  });
});
