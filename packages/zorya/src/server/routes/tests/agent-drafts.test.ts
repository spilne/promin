// ---------------------------------------------------------------------------
// Draft recipe routes — the opportunistic stale-draft sweep and the draft id
// read the injected clock, so the 1h TTL is driven by FakeWallClock.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { createDraft } from "../agent-drafts.ts";

const HOUR_MS = 60 * 60 * 1000;
const SWEEP_INTERVAL_MS = 60 * 1000;

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
  it("stamps the draft id from the clock plus a random suffix", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryAgentRegistry({ clock });
    const handler = createDraft({ registry, clock });
    const a = await postDraft(handler, "support");
    const b = await postDraft(handler, "support");
    const ts = clock.currentTimeMs().toString(36);
    expect(a).toMatch(new RegExp(`^__draft__${ts}[0-9a-f]{12}__support$`));
    // Same millisecond, same source — still two distinct drafts.
    expect(b).not.toBe(a);
    expect(await registry.get(a)).not.toBeNull();
    expect(await registry.get(b)).not.toBeNull();
  });

  it("strips unsafe characters from the source id", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryAgentRegistry({ clock });
    const id = await postDraft(createDraft({ registry, clock }), "../evil/<x> y");
    expect(id).toMatch(/__\.\.evilxy$/);
  });

  it("rejects an invalid backend with 400", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const registry = new InMemoryAgentRegistry({ clock });
    const res = await createDraft({ registry, clock })(
      new Request("http://test/api/agents/_draft", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ backend: { type: "local" } }),
      }),
    );
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: string }).error).toBe("invalid_backend");
    expect(await registry.list({})).toEqual([]);
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

    // Sweeps run at most once a minute; the next one reaps the first draft.
    clock.advance(SWEEP_INTERVAL_MS);
    const third = await postDraft(handler, "c");
    expect(await registry.get(first)).toBeNull();
    expect(await registry.get(second)).not.toBeNull();
    expect(await registry.get(third)).not.toBeNull();
  });
});
