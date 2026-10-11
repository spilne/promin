import { describe, expect, it } from "bun:test";
import { OutboundRequestError, createOutboundFetch } from "../../outbound/outbound-fetch.ts";
import { resolveRemoteAgent } from "../resolve-remote-agent.ts";
import type { RegisteredAgent } from "../types.ts";

function remoteRecipe(endpoint: string): RegisteredAgent {
  return {
    id: "peer",
    backend: { type: "remote", endpoint, remoteAgentId: "peer" },
  } as unknown as RegisteredAgent;
}

describe("resolveRemoteAgent", () => {
  it("sends every call through the injected transport", async () => {
    const seen: string[] = [];
    const agent = resolveRemoteAgent({
      agent: remoteRecipe("https://peer.example/"),
      fetch: async (url) => {
        seen.push(url);
        return Response.json({ text: "hi", steps: [], usage: {}, finishReason: "stop" });
      },
    });
    await agent.invoke({ task: "hello" });
    expect(seen).toEqual(["https://peer.example/api/agents/peer/invoke"]);
  });

  it("refuses an endpoint that re-resolves to a private address under an outbound policy", async () => {
    const agent = resolveRemoteAgent({
      agent: remoteRecipe("https://peer.example"),
      fetch: createOutboundFetch({ policy: { resolveHost: async () => ["169.254.169.254"] } }),
    });
    const err = await agent.invoke({ task: "hello" }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(OutboundRequestError);
  });
});
