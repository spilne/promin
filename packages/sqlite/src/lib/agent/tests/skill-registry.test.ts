// ---------------------------------------------------------------------------
// `SqliteSkillRegistry` — runs the @promin/agent conformance suite plus
// SQLite-specific persistence sanity tests.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { skillRegistryTestSuite } from "@promin/agent/testing";
import { SqliteSkillRegistry } from "../skill-registry.ts";

function makeRegistry() {
  return SqliteSkillRegistry.make({ db: new Database(":memory:") });
}

skillRegistryTestSuite(makeRegistry);

describe("SqliteSkillRegistry — persistence", () => {
  it("skills persist across instances sharing one db", async () => {
    const db = new Database(":memory:");
    const r1 = SqliteSkillRegistry.make({ db });
    await r1.register({
      id: "structured-debugging",
      description: "A disciplined debugging loop.",
      whenToUse: "A bug resists a quick fix.",
      body: "# Debug\nReproduce first.",
      metadata: { tags: ["engineering"], capabilities: [] },
    });

    const r2 = SqliteSkillRegistry.make({ db });
    const got = await r2.get("structured-debugging");
    expect(got?.id).toBe("structured-debugging");
    expect(got?.body).toContain("Reproduce first.");
    expect(got?.metadata.tags).toEqual(["engineering"]);
  });

  it("falls back whenToUse to description when omitted", async () => {
    const r = SqliteSkillRegistry.make({ db: new Database(":memory:") });
    const row = await r.register({
      id: "x",
      description: "the description",
      body: "# body",
    });
    expect(row.whenToUse).toBe("the description");
  });
});
