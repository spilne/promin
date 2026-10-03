// ---------------------------------------------------------------------------
// Draft recipe routes — the opportunistic stale-draft sweep and the draft id
// read the injected clock, so the 1h TTL is driven by FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { createDraft } from "../agent-drafts.ts";

const HOUR_MS = 60 * 60 * 1000;

const draftBackend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "draft prompt", tools: [] as string[] } },
};

async function postDraft(handler: ReturnType<typeof createDraft>, sourceId: string) {
  const res = await handler(
    new Request("http://test/api/agents/_draft", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ backend: draftBackend, sourceId }),
    }),
  );
  expect(res.status).toBe(201);
  return ((await res.json()) as { recipe: { id: string } }).recipe.id;
}

describe("createDraft — TTL sweep on an injected clock", () => {
  it("stamps the draft id from the clock", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryAgentRegistry({ clock });
    const id = await postDraft(createDraft({ registry, clock }), "support");
    expect(id).toBe(`__draft__${clock.currentTimeMs().toString(36)}__support`);
  });

  it("reaps a draft only once it is older than the TTL by the clock", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryAgentRegistry({ clock });
    const handler = createDraft({ registry, clock });

    const first = await postDraft(handler, "a");

    // Exactly one TTL later the first draft is not yet stale (strict `<`).
    clock.advance(HOUR_MS);
    const second = await postDraft(handler, "b");
    expect(await registry.get(first)).not.toBeNull();

    clock.advance(1);
    const third = await postDraft(handler, "c");
    expect(await registry.get(first)).toBeNull();
    expect(await registry.get(second)).not.toBeNull();
    expect(await registry.get(third)).not.toBeNull();
  });
});
