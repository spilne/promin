import { describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { versionedRegistryTestSuite } from "@promin/agent/testing";
import { FakeWallClock } from "@promin/workflow";
import { SqliteAgentRegistry, SqliteFragmentRegistry, SqliteRoleRegistry } from "../registries.ts";
import { SqliteVersionedRegistry } from "../versioned-registry.ts";

versionedRegistryTestSuite({
  name: "SqliteVersionedRegistry",
  factory: ({ kind, clock }) =>
    new SqliteVersionedRegistry({ db: new Database(":memory:"), kind, clock }),
});

const BACKEND = {
  type: "local" as const,
  model: { provider: "mock", id: "m" },
  role: { inline: { systemPrompt: null, tools: [] } },
};

describe("SqliteVersionedRegistry — several processes on one file", () => {
  it("every concurrent write succeeds and mints its own sequence number", async () => {
    const dir = await mkdtemp(join(tmpdir(), "registry-procs-"));
    const file = join(dir, "registry.db");
    try {
      const setup = new Database(file);
      setup.exec("PRAGMA journal_mode = WAL");
      new SqliteFragmentRegistry({ db: setup }); // creates the tables
      setup.close();
      const fixture = new URL("./fixtures/registry-writer.ts", import.meta.url).pathname;
      const writers = ["A", "B", "C", "D"].map((tag) =>
        Bun.spawn([process.execPath, "--conditions=@promin/source", fixture, file, tag, "40"], {
          stdout: "pipe",
          stderr: "inherit",
        }),
      );
      const tallies = await Promise.all(
        writers.map(
          async (p) =>
            JSON.parse(await new Response(p.stdout).text()) as { ok: number; errors: string[] },
        ),
      );
      expect(tallies.flatMap((t) => t.errors)).toEqual([]);
      expect(tallies.map((t) => t.ok)).toEqual([40, 40, 40, 40]);
      const db = new Database(file);
      const row = db
        .query<{ n: number; seqs: number; labels: number }, []>(
          `SELECT count(*) AS n, count(DISTINCT seq) AS seqs, count(DISTINCT version) AS labels
           FROM promin_registry_version`,
        )
        .get();
      expect(row).toEqual({ n: 160, seqs: 160, labels: 160 });
      db.close();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 60_000);
});

describe("SqliteVersionedRegistry — one database", () => {
  it("survives re-opening the registry on the same database", async () => {
    const db = new Database(":memory:");
    const clock = FakeWallClock.create(1_000);
    await new SqliteAgentRegistry({ db, clock }).register({
      id: "support",
      backend: BACKEND,
      metadata: { tags: ["beta"] },
    });
    const reopened = await new SqliteAgentRegistry({ db, clock }).get({ id: "support" });
    expect(reopened).toMatchObject({ id: "support", version: "v1", createdAt: 1_000 });
    expect(reopened?.metadata.tags).toEqual(["beta"]);
  });

  it("keeps kinds apart in the shared tables", async () => {
    const db = new Database(":memory:");
    const agents = new SqliteAgentRegistry({ db });
    const roles = new SqliteRoleRegistry({ db });
    const fragments = new SqliteFragmentRegistry({ db });
    await agents.register({ id: "same", backend: BACKEND });
    await roles.register({ id: "same", definition: { systemPrompt: "x", tools: [] } });
    expect((await agents.list()).items.map((e) => e.id)).toEqual(["same"]);
    expect((await roles.versions({ id: "same" })).map((v) => v.version)).toEqual(["v1"]);
    expect(await fragments.get({ id: "same" })).toBeNull();
    await roles.unregister({ id: "same" });
    expect(await agents.get({ id: "same" })).not.toBeNull();
  });
});
