// ---------------------------------------------------------------------------
// FederatedAgentRegistry — covers manifest gating semantics + pass-through
// behavior for non-remote backends and read paths.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemoryAgentRegistry } from "../../registry/in-memory-agent-registry.ts";
import { FederatedAgentRegistry } from "../federated-agent-registry.ts";
import {
  AllowAllFederationManifest,
  FederationManifestError,
  StaticFederationManifest,
} from "../types.ts";
import type { RegisterAgentInput } from "../../registry/types.ts";

function localInput(id: string): RegisterAgentInput {
  return {
    id,
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: null, tools: [] } },
    },
  };
}

function remoteInput(id: string, endpoint: string): RegisterAgentInput {
  return {
    id,
    backend: { type: "remote", endpoint, remoteAgentId: id },
  };
}

describe("FederatedAgentRegistry — local backends pass through", () => {
  it("registers a local recipe regardless of manifest contents", async () => {
    const inner = new InMemoryAgentRegistry();
    const empty = StaticFederationManifest.fromConfig([]);
    const reg = new FederatedAgentRegistry(inner, empty);
    const r = await reg.register(localInput("local-bot"));
    expect(r.id).toBe("local-bot");
    expect((await inner.get("local-bot"))?.id).toBe("local-bot");
  });
});

describe("FederatedAgentRegistry — remote backends gated by manifest", () => {
  it("allows when (endpoint, agentId) is in the manifest", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([
      { endpoint: "https://org-b.example", agentIds: ["writer", "analyst"] },
    ]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    const r = await reg.register(remoteInput("writer", "https://org-b.example"));
    expect(r.id).toBe("writer");
  });

  it("allows wildcard '*' for any agentId at a given endpoint", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([
      { endpoint: "https://org-c.example", agentIds: ["*"] },
    ]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    const r = await reg.register(remoteInput("any-name", "https://org-c.example"));
    expect(r.id).toBe("any-name");
  });

  it("rejects when endpoint isn't in the manifest", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([
      { endpoint: "https://org-b.example", agentIds: ["writer"] },
    ]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    expect(reg.register(remoteInput("writer", "https://hostile.example"))).rejects.toThrow(
      FederationManifestError,
    );
  });

  it("rejects when agentId isn't allowed for an otherwise-allowed endpoint", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([
      { endpoint: "https://org-b.example", agentIds: ["writer"] },
    ]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    expect(reg.register(remoteInput("smuggled", "https://org-b.example"))).rejects.toThrow(
      FederationManifestError,
    );
  });

  it("normalizes trailing slashes in endpoint comparison", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([
      { endpoint: "https://org-b.example", agentIds: ["writer"] },
    ]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    // Trailing-slash variant hits the same entry.
    const r = await reg.register(remoteInput("writer", "https://org-b.example/"));
    expect(r.id).toBe("writer");
  });

  it("AllowAllFederationManifest passes everything (trusted-env opt-out)", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = new AllowAllFederationManifest();
    const reg = new FederatedAgentRegistry(inner, manifest);
    const r = await reg.register(remoteInput("any-id", "https://anywhere"));
    expect(r.id).toBe("any-id");
  });

  it("does NOT register when manifest rejects (no partial state in inner registry)", async () => {
    const inner = new InMemoryAgentRegistry();
    const manifest = StaticFederationManifest.fromConfig([]);
    const reg = new FederatedAgentRegistry(inner, manifest);
    try {
      await reg.register(remoteInput("rejected", "https://nope"));
    } catch {
      // expected
    }
    expect(await inner.get("rejected")).toBeNull();
  });
});

describe("FederatedAgentRegistry — read paths pass through unchanged", () => {
  it("get / list / versions / unregister forward to inner", async () => {
    const inner = new InMemoryAgentRegistry();
    const reg = new FederatedAgentRegistry(inner, new AllowAllFederationManifest());
    await reg.register(localInput("a"));
    await reg.register(localInput("b"));
    expect((await reg.list()).map((r) => r.id).sort()).toEqual(["a", "b"]);
    expect(await reg.get("a")).not.toBeNull();
    expect((await reg.versions("a")).map((r) => r.version)).toEqual(["v1"]);
    await reg.unregister("a");
    expect(await reg.get("a")).toBeNull();
  });
});
