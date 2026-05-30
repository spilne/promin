// ---------------------------------------------------------------------------
// PostgresRoleRegistry — runs the @promin/agent conformance suite against a
// real Postgres container (testcontainers). Confirms the Postgres
// implementation behaves identically to InMemory and SqliteRoleRegistry.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { roleRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresRoleRegistry } from "../role-registry.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE role_registry`;
});

roleRegistryTestSuite(() => new PostgresRoleRegistry({ db: pg.db }));

describe("PostgresRoleRegistry — Postgres-specific", () => {
  it("rows persist across multiple registry instances against the same db", async () => {
    const r1 = new PostgresRoleRegistry({ db: pg.db });
    await r1.register({
      id: "git-master",
      definition: {
        systemPrompt: { base: "you are a git master", layers: ["findings-table"] },
        tools: ["bash"],
        skills: [{ id: "structured-debugging" }],
        capabilities: ["chat"],
      },
      metadata: { description: "Git expert", tags: ["role"], suggestedSecrets: ["GH_TOKEN"] },
    });

    const r2 = new PostgresRoleRegistry({ db: pg.db });
    const got = await r2.get("git-master");
    expect(got?.id).toBe("git-master");
    expect(got?.metadata.tags).toEqual(["role"]);
    expect(got?.metadata.suggestedSecrets).toEqual(["GH_TOKEN"]);
    expect(got?.definition.tools).toEqual(["bash"]);
    expect(got?.definition.systemPrompt).toEqual({
      base: "you are a git master",
      layers: ["findings-table"],
    });
  });

  it("two registries pointed at the same db see each other's writes", async () => {
    const writer = new PostgresRoleRegistry({ db: pg.db });
    const reader = new PostgresRoleRegistry({ db: pg.db });

    await writer.register({ id: "shared", definition: { systemPrompt: null, tools: [] } });
    expect(await reader.get("shared")).not.toBeNull();

    await writer.unregister("shared");
    expect(await reader.get("shared")).toBeNull();
  });
});
