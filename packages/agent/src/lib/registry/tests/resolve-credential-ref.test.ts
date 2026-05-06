// ---------------------------------------------------------------------------
// `resolveCredentialRef` — BYOK credential resolution. Pre-step that
// callers run before resolveLocalAgent when a recipe declares
// `model.credentialRef`. Pinned cases:
//   1. No credentialRef → undefined (host-default path)
//   2. credentialRef + secrets at global → returns global value
//   3. credentialRef + secrets at namespace → returns ns value (cascade)
//   4. credentialRef + secrets at resource → returns res value (cascade)
//   5. credentialRef but no SecretsStorage → throws clearly
//   6. credentialRef but key not in any scope → throws clearly
//   7. Cascade ordering: resource > namespace > global
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemorySecretsStorage } from "../../secrets/in-memory-secrets-storage.ts";
import { SecretScope } from "../../secrets/types.ts";
import { resolveCredentialRef } from "../resolve-local-agent.ts";
import type { RegisteredAgent } from "../types.ts";

function recipeWithRef(credentialRef?: string): RegisteredAgent {
  return {
    id: "byok-test",
    version: "v1",
    backend: {
      type: "local",
      model: {
        provider: "anthropic",
        id: "claude-sonnet-4-6",
        ...(credentialRef !== undefined && { credentialRef }),
      },
      systemPrompt: null,
      tools: [],
    },
    metadata: { description: null, capabilities: [], tags: [] },
    createdAt: 0,
    updatedAt: 0,
  };
}

describe("resolveCredentialRef", () => {
  it("returns undefined when recipe has no credentialRef", async () => {
    const secrets = new InMemorySecretsStorage();
    const out = await resolveCredentialRef({ recipe: recipeWithRef(), secrets });
    expect(out).toBeUndefined();
  });

  it("returns undefined when backend type is not 'local'", async () => {
    // Only local backends carry a credentialRef field; remote/cursor
    // get their auth elsewhere. Helper short-circuits without trying
    // to read the field.
    const recipe: RegisteredAgent = {
      id: "remote-bot",
      version: "v1",
      backend: {
        type: "remote",
        endpoint: "https://other.example.com",
        remoteAgentId: "x",
      },
      metadata: { description: null, capabilities: [], tags: [] },
      createdAt: 0,
      updatedAt: 0,
    };
    const secrets = new InMemorySecretsStorage();
    const out = await resolveCredentialRef({ recipe, secrets });
    expect(out).toBeUndefined();
  });

  it("resolves from global scope when no scope is provided", async () => {
    const secrets = new InMemorySecretsStorage();
    await secrets.set({
      scope: SecretScope.global(),
      key: "anthropic_api_key",
      value: "sk-global",
    });
    const out = await resolveCredentialRef({
      recipe: recipeWithRef("anthropic_api_key"),
      secrets,
    });
    expect(out).toBe("sk-global");
  });

  it("cascades resource → namespace → global, returning most-specific value", async () => {
    const secrets = new InMemorySecretsStorage();
    await secrets.set({
      scope: SecretScope.global(),
      key: "anthropic_api_key",
      value: "sk-global",
    });
    await secrets.set({
      scope: SecretScope.namespace("acme"),
      key: "anthropic_api_key",
      value: "sk-ns",
    });
    await secrets.set({
      scope: SecretScope.resource("acme", "alice"),
      key: "anthropic_api_key",
      value: "sk-res",
    });

    // Resource wins.
    const res = await resolveCredentialRef({
      recipe: recipeWithRef("anthropic_api_key"),
      secrets,
      scope: { namespaceId: "acme", resourceId: "alice" },
    });
    expect(res).toBe("sk-res");

    // Namespace wins when no resource scope.
    const ns = await resolveCredentialRef({
      recipe: recipeWithRef("anthropic_api_key"),
      secrets,
      scope: { namespaceId: "acme" },
    });
    expect(ns).toBe("sk-ns");

    // Global is the fallback.
    const glob = await resolveCredentialRef({
      recipe: recipeWithRef("anthropic_api_key"),
      secrets,
    });
    expect(glob).toBe("sk-global");
  });

  it("throws clearly when credentialRef is set but no SecretsStorage is supplied", async () => {
    expect(resolveCredentialRef({ recipe: recipeWithRef("anthropic_api_key") })).rejects.toThrow(
      /no SecretsStorage was supplied/,
    );
  });

  it("throws clearly when credentialRef is set but the key isn't in any scope", async () => {
    const secrets = new InMemorySecretsStorage();
    expect(
      resolveCredentialRef({
        recipe: recipeWithRef("anthropic_api_key"),
        secrets,
        scope: { namespaceId: "acme", resourceId: "alice" },
      }),
    ).rejects.toThrow(/not found in any scope/);
  });
});
