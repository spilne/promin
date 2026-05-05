// ---------------------------------------------------------------------------
// PostgresAgentRegistry — runs the @promin/agent conformance suite against
// a real Postgres container (testcontainers). Confirms that the Postgres
// implementation behaves identically to InMemory and SqliteAgentRegistry.
//
// The suite covers register/get/list/versions/unregister and the
// pagination + filter semantics. PG-specific concerns (round-trip JSONB
// blobs, multi-process visibility) are tested below the suite.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { agentRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../migrate.ts";
import { PostgresAgentRegistry } from "../postgres-agent-registry.ts";
import { PostgresTestContainer } from "../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_registry`;
});

agentRegistryTestSuite(() => new PostgresAgentRegistry({ db: pg.db }));

describe("PostgresAgentRegistry — Postgres-specific", () => {
  it("rows persist across multiple registry instances against the same db", async () => {
    const r1 = new PostgresAgentRegistry({ db: pg.db });
    await r1.register({
      id: "support",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        systemPrompt: "Helpful assistant",
        tools: ["search"],
      },
      metadata: { capabilities: ["chat"], tags: ["beta"] },
    });

    const r2 = new PostgresAgentRegistry({ db: pg.db });
    const got = await r2.get("support");
    expect(got?.id).toBe("support");
    expect(got?.metadata.tags).toEqual(["beta"]);
    if (got?.backend.type === "local") {
      expect(got.backend.model.id).toBe("claude-sonnet-4-6");
    }
  });

  it("round-trips full backend JSON for cursor + remote backends", async () => {
    const r = new PostgresAgentRegistry({ db: pg.db });
    await r.register({
      id: "remote-bot",
      backend: {
        type: "remote",
        endpoint: "https://other.example.com",
        remoteAgentId: "billing-agent",
        auth: { kind: "bearer", token: "secret" },
        timeoutMs: 5_000,
      },
    });
    await r.register({
      id: "cursor-bot",
      backend: {
        type: "cursor",
        model: "auto",
        workspace: "/tmp/wk",
        worktree: true,
        sandbox: "enabled",
        extraArgs: ["--debug"],
      },
    });

    const remote = await r.get("remote-bot");
    expect(remote?.backend.type).toBe("remote");
    if (remote?.backend.type === "remote") {
      expect(remote.backend.endpoint).toBe("https://other.example.com");
      expect(remote.backend.auth?.token).toBe("secret");
      expect(remote.backend.timeoutMs).toBe(5_000);
    }

    const cursor = await r.get("cursor-bot");
    if (cursor?.backend.type === "cursor") {
      expect(cursor.backend.workspace).toBe("/tmp/wk");
      expect(cursor.backend.extraArgs).toEqual(["--debug"]);
    }
  });

  it("two registries pointed at the same db see each other's writes", async () => {
    const writer = new PostgresAgentRegistry({ db: pg.db });
    const reader = new PostgresAgentRegistry({ db: pg.db });

    await writer.register({
      id: "shared",
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        systemPrompt: null,
        tools: [],
      },
    });
    expect(await reader.get("shared")).not.toBeNull();

    await writer.unregister("shared");
    expect(await reader.get("shared")).toBeNull();
  });
});
