// ---------------------------------------------------------------------------
// PostgresMemoryStore — runs the @promin/agent memoryStoreTestSuite
// against a real Postgres container (testcontainers). Catches divergence
// from the Sqlite/InMemory implementations as soon as it ships.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { memoryStoreTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresMemoryStore } from "../memory-store.ts";
import { PostgresTestContainer } from "../../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  // Truncate in dependency order. CASCADE not strictly needed since
  // we don't FK-link the tables, but spelled out explicitly for safety.
  await pg.sql`TRUNCATE agent_message, agent_episode, agent_fact, agent_thread, agent_resource, agent_namespace`;
});

memoryStoreTestSuite(() => new PostgresMemoryStore({ db: pg.db }));

describe("PostgresMemoryStore — Postgres-specific", () => {
  it("two store instances pointed at the same db see each other's writes", async () => {
    const writer = new PostgresMemoryStore({ db: pg.db });
    const reader = new PostgresMemoryStore({ db: pg.db });

    await writer.appendNamespaceFact("acme", "shared-fact");
    const facts = await reader.listNamespaceFacts("acme");
    expect(facts.map((f) => f.text)).toEqual(["shared-fact"]);
  });

  it("messages roundtrip JSONB payloads with nested structures", async () => {
    const s = new PostgresMemoryStore({ db: pg.db });
    await s.createThread({ namespaceId: "acme", threadId: "t-1" });
    const stored = await s.appendMessages({ namespaceId: "acme", threadId: "t-1" }, [
      {
        role: "user",
        content: [
          { type: "text", text: "Hello" },
          { type: "image", url: "https://example.com/x.png" },
        ],
      } as never,
      { role: "assistant", content: "Hi back" } as never,
    ]);
    expect(stored).toHaveLength(2);
    const back = await s.getMessages({ namespaceId: "acme", threadId: "t-1" });
    expect(back).toHaveLength(2);
    expect(back[0]?.role).toBe("user");
    expect(back[1]?.role).toBe("assistant");
  });
});
