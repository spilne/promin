import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { namespaceRegistryTestSuite } from "@promin/core/testing";
import { migrate } from "../migrate.ts";
import { PostgresNamespaceRegistry } from "../postgres-namespace-registry.ts";
import { PostgresTestContainer } from "../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.db.execute(sql`TRUNCATE zorya_namespace`);
});

namespaceRegistryTestSuite(() => new PostgresNamespaceRegistry({ db: pg.db }));

describe("PostgresNamespaceRegistry — Postgres-specific", () => {
  it("rows persist across multiple registry instances against the same db", async () => {
    const r1 = new PostgresNamespaceRegistry({ db: pg.db });
    await r1.create({
      id: "acme",
      displayName: "Acme",
      capabilities: { workflows: { maxConcurrentRuns: 3 } },
    });

    const r2 = new PostgresNamespaceRegistry({ db: pg.db });
    const got = await r2.get("acme");
    expect(got?.displayName).toBe("Acme");
    expect(got?.capabilities.workflows?.maxConcurrentRuns).toBe(3);
  });
});
