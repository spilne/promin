// ---------------------------------------------------------------------------
// PostgresSkillRegistry — runs the @promin/agent conformance suite against a
// real Postgres container, then a couple of PG-specific persistence checks.
// Confirms it behaves identically to InMemory and SqliteSkillRegistry.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { skillRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresSkillRegistry } from "../skill-registry.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE skill_registry`;
});

skillRegistryTestSuite(() => new PostgresSkillRegistry({ db: pg.db }));

describe("PostgresSkillRegistry — Postgres-specific", () => {
  it("rows persist across multiple registry instances against the same db", async () => {
    const r1 = new PostgresSkillRegistry({ db: pg.db });
    await r1.register({
      id: "structured-debugging",
      description: "A disciplined debugging loop.",
      whenToUse: "A bug resists a quick fix.",
      body: "# Debug\nReproduce first.",
      metadata: { tags: ["engineering"], capabilities: [] },
    });

    const r2 = new PostgresSkillRegistry({ db: pg.db });
    const got = await r2.get("structured-debugging");
    expect(got?.id).toBe("structured-debugging");
    expect(got?.body).toContain("Reproduce first.");
    expect(got?.metadata.tags).toEqual(["engineering"]);
  });

  it("falls back whenToUse to description when omitted", async () => {
    const r = new PostgresSkillRegistry({ db: pg.db });
    const row = await r.register({ id: "x", description: "the description", body: "# body" });
    expect(row.whenToUse).toBe("the description");
  });

  it("two registries pointed at the same db see each other's writes", async () => {
    const writer = new PostgresSkillRegistry({ db: pg.db });
    const reader = new PostgresSkillRegistry({ db: pg.db });
    await writer.register({ id: "shared", description: "d", body: "# b" });
    expect(await reader.get("shared")).not.toBeNull();
    await writer.unregister("shared");
    expect(await reader.get("shared")).toBeNull();
  });
});
