// PostgresFragmentStore — runs the conformance suite against a real
// Postgres container, plus a PG-specific cross-instance persistence check.

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { fragmentStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresFragmentStore } from "../fragment-store.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE fragment_store`;
});

fragmentStoreTestSuite(() => new PostgresFragmentStore({ db: pg.db }));

describe("PostgresFragmentStore — Postgres-specific", () => {
  it("two stores against the same db see each other's writes", async () => {
    const writer = new PostgresFragmentStore({ db: pg.db });
    const reader = new PostgresFragmentStore({ db: pg.db });
    await writer.set("shared", "## Shared layer\nbody");
    const all = await reader.loadAll();
    expect(all).toHaveLength(1);
    expect(all[0]!.content).toContain("Shared layer");
    await writer.delete("shared");
    expect(await reader.loadAll()).toEqual([]);
  });
});
