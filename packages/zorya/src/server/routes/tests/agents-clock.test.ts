// ---------------------------------------------------------------------------
// Agent gateway time reads go through the injected clock — the distill
// rate-limit Retry-After and the default thread `archivedAt` stamp. Calls
// the handlers directly with a stub agent.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { ConsolidatorRateLimitError, InMemoryAgentRegistry } from "@promin/agent";
import { FakeWallClock } from "@promin/workflow";
import { archiveAgentThread, distillThread, type AgentGatewayDeps } from "../agents.ts";

const backend = {
  type: "local" as const,
  model: { provider: "anthropic", id: "claude-sonnet-4-6" },
  role: { inline: { systemPrompt: "hi", tools: [] as string[] } },
};

async function setup(clock: FakeWallClock) {
  const registry = new InMemoryAgentRegistry({ clock });
  await registry.register({ id: "bot", backend });
  const archived: Array<number | null> = [];
  const stubAgent = {
    withScope: () => stubAgent,
    distillThread: async () => {
      // Window opened 10s ago (clock time) and lasts 60s → 50s to wait.
      throw new ConsolidatorRateLimitError({
        scope: { kind: "namespace", namespaceId: "ns" },
        windowMs: 60_000,
        max: 1,
        seen: 1,
        oldestInWindowAt: clock.currentTimeMs() - 10_000,
      });
    },
    thread: async (id: string) => ({
      id,
      setArchived: async (at: number | null) => {
        archived.push(at);
      },
    }),
  };
  const deps = {
    registry,
    resolve: () => stubAgent,
    clock,
  } as unknown as AgentGatewayDeps;
  return { deps, archived };
}

function post(body: unknown): Request {
  return new Request("http://x", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("agent gateway — time on an injected clock", () => {
  it("computes the distill Retry-After against the clock", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { deps } = await setup(clock);

    const res = await distillThread(deps)(post({ namespaceId: "ns", resourceId: "r-1" }), {
      id: "bot",
      threadId: "t-1",
    });
    expect(res.status).toBe(429);
    expect(res.headers.get("retry-after")).toBe("50");
    expect(((await res.json()) as { retryAfterSeconds: number }).retryAfterSeconds).toBe(50);
  });

  it("defaults archivedAt to the clock's now when the body omits it", async () => {
    const clock = FakeWallClock.create("2026-01-01T00:00:00Z");
    const { deps, archived } = await setup(clock);

    const res = await archiveAgentThread(deps)(post({ namespaceId: "ns" }), {
      id: "bot",
      threadId: "t-1",
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ threadId: "t-1", archivedAt: clock.currentTimeMs() });
    expect(archived).toEqual([clock.currentTimeMs()]);
  });
});
