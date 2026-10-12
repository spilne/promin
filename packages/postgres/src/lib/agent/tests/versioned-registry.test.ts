// ---------------------------------------------------------------------------
// PostgresVersionedRegistry — the @promin/agent conformance suite (every
// kind) against a real Postgres container, plus what only a shared database
// shows: several registry instances (replicas) racing on one id.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { isRegistryConflict } from "@promin/agent";
import { versionedRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresTestContainer } from "../../test-utils.ts";
import { PostgresAgentRegistry, PostgresRoleRegistry } from "../registries.ts";
import { PostgresVersionedRegistry } from "../versioned-registry.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE registry_head, registry_version`;
});

versionedRegistryTestSuite({
  name: "PostgresVersionedRegistry",
  factory: ({ kind, clock }) => new PostgresVersionedRegistry({ db: pg.db, kind, clock }),
});

const BACKEND = {
  type: "local" as const,
  model: { provider: "mock", id: "m" },
  role: { inline: { systemPrompt: null, tools: [] } },
};

describe("PostgresVersionedRegistry — replicas", () => {
  it("registries on different instances mint distinct versions for concurrent writes", async () => {
    const replicas = [1, 2, 3, 4].map(() => new PostgresAgentRegistry({ db: pg.db }));
    const entries = await Promise.all(
      replicas.map((r, n) =>
        r.register({
          id: "shared",
          backend: { ...BACKEND, model: { provider: "mock", id: `m${n}` } },
        }),
      ),
    );
    expect(new Set(entries.map((e) => e.version)).size).toBe(4);
    const versions = await replicas[0]!.versions({ id: "shared" });
    expect(versions.map((v) => v.version)).toEqual(["v1", "v2", "v3", "v4"]);
  });

  it('lists and pages in byte order (ids are COLLATE "C"), whatever the database locale', async () => {
    const r = new PostgresRoleRegistry({ db: pg.db });
    for (const id of ["a", "B", "_x", "Z"]) {
      await r.register({ id, definition: { systemPrompt: id, tools: [] } });
    }
    const first = await r.list({ limit: 2 });
    expect(first.items.map((e) => e.id)).toEqual(["B", "Z"]);
    const second = await r.list({ limit: 2, cursor: first.nextCursor! });
    expect(second.items.map((e) => e.id)).toEqual(["_x", "a"]);
    const [column] = await pg.sql`
      SELECT collation_name FROM information_schema.columns
      WHERE table_name = 'registry_version' AND column_name = 'id'`;
    expect(column?.collation_name).toBe("C");
  });

  it("only one replica's create-only register wins", async () => {
    const replicas = [1, 2, 3].map(() => new PostgresRoleRegistry({ db: pg.db }));
    const results = await Promise.allSettled(
      replicas.map((r, n) =>
        r.register({
          id: "reviewer",
          definition: { systemPrompt: `persona ${n}`, tools: [] },
          expectedActive: null,
        }),
      ),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    for (const r of results) {
      if (r.status === "rejected") expect(isRegistryConflict(r.reason)).toBe(true);
    }
  });

  it("a write on one replica is visible to another", async () => {
    const writer = new PostgresAgentRegistry({ db: pg.db });
    const reader = new PostgresAgentRegistry({ db: pg.db });
    await writer.register({
      id: "remote-bot",
      backend: {
        type: "remote",
        endpoint: "https://b",
        remoteAgentId: "x",
        auth: { kind: "bearer", token: "secret" },
      },
    });
    const got = await reader.get({ id: "remote-bot" });
    expect(got?.backend).toEqual({
      type: "remote",
      endpoint: "https://b",
      remoteAgentId: "x",
      auth: { kind: "bearer", token: "secret" },
    });
    await writer.setEnabled({ id: "remote-bot", enabled: false });
    expect((await reader.get({ id: "remote-bot" }))?.enabled).toBe(false);
  });
});
