// ---------------------------------------------------------------------------
// createScopedTool — declarative secret auto-injection (3gjv Phase 1).
// Pinned cases:
//   1. No secrets declared → ctx.secrets is empty record
//   2. Declared + resolvable → ctx.secrets.<name> = stored value
//   3. Cascade ordering: resource > namespace > global respected
//   4. Required + unresolved → throws clearly with tool + ref name
//   5. Optional + unresolved → silently absent from ctx.secrets
//   6. Multiple refs → all resolved, distinct values
//   7. Tool body cannot leak the storage primitive (ctx.secrets is values only)
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { createScopedTool } from "../tool.ts";
import { InMemorySecretsStorage } from "../secrets/in-memory-secrets-storage.ts";
import { SecretScope } from "../secrets/types.ts";

const fixedScope = { namespaceId: "acme", resourceId: "alice" };

describe("createScopedTool — declarative secrets", () => {
  it("ctx.secrets is empty record when no secrets declared", async () => {
    const t = createScopedTool({
      name: "noSecrets",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => Object.keys(ctx.secrets).length,
    });
    const out = await t.execute({}, { scope: fixedScope });
    expect(out).toBe(0);
  });

  it("resolves declared refs and exposes them under the declared name", async () => {
    const storage = new InMemorySecretsStorage();
    await storage.set({
      scope: SecretScope.global(),
      key: "SLACK_BOT_TOKEN",
      value: "xoxb-123",
    });

    const t = createScopedTool({
      name: "slack_post",
      description: "test",
      parameters: z.object({}),
      secrets: {
        storage,
        refs: { slackToken: { ref: "SLACK_BOT_TOKEN" } },
      },
      execute: async (_input, ctx) => ctx.secrets.slackToken,
    });
    const out = await t.execute({}, { scope: fixedScope });
    expect(out).toBe("xoxb-123");
  });

  it("respects cascade resolution: resource > namespace > global", async () => {
    const storage = new InMemorySecretsStorage();
    await storage.set({ scope: SecretScope.global(), key: "K", value: "global-v" });
    await storage.set({ scope: SecretScope.namespace("acme"), key: "K", value: "ns-v" });
    await storage.set({
      scope: SecretScope.resource("acme", "alice"),
      key: "K",
      value: "res-v",
    });

    const t = createScopedTool({
      name: "cascade",
      description: "test",
      parameters: z.object({}),
      secrets: { storage, refs: { v: { ref: "K" } } },
      execute: async (_input, ctx) => ctx.secrets.v,
    });
    expect(await t.execute({}, { scope: fixedScope })).toBe("res-v");
    expect(await t.execute({}, { scope: { namespaceId: "acme", resourceId: "bob" } })).toBe("ns-v");
    expect(await t.execute({}, { scope: { namespaceId: "globex", resourceId: "x" } })).toBe(
      "global-v",
    );
  });

  it("required + unresolved → throws with tool name + ref name", async () => {
    const storage = new InMemorySecretsStorage();
    const t = createScopedTool({
      name: "missingSecret",
      description: "test",
      parameters: z.object({}),
      secrets: { storage, refs: { token: { ref: "MISSING_TOKEN" } } },
      execute: async () => "ok",
    });
    expect(t.execute({}, { scope: fixedScope })).rejects.toThrow(
      /Tool 'missingSecret' requires secret 'MISSING_TOKEN'/,
    );
  });

  it("optional + unresolved → silently absent from ctx.secrets", async () => {
    const storage = new InMemorySecretsStorage();
    const t = createScopedTool({
      name: "optionalSecret",
      description: "test",
      parameters: z.object({}),
      secrets: {
        storage,
        refs: {
          required: { ref: "REQUIRED_K" },
          maybeMissing: { ref: "OPTIONAL_K", required: false },
        },
      },
      execute: async (_input, ctx) => ({
        hasRequired: "required" in ctx.secrets,
        hasMaybe: "maybeMissing" in ctx.secrets,
      }),
    });
    await storage.set({ scope: SecretScope.global(), key: "REQUIRED_K", value: "yep" });
    const out = (await t.execute({}, { scope: fixedScope })) as {
      hasRequired: boolean;
      hasMaybe: boolean;
    };
    expect(out.hasRequired).toBe(true);
    expect(out.hasMaybe).toBe(false);
  });

  it("resolves multiple refs into distinct values", async () => {
    const storage = new InMemorySecretsStorage();
    await storage.set({ scope: SecretScope.global(), key: "A", value: "alpha" });
    await storage.set({ scope: SecretScope.global(), key: "B", value: "beta" });

    const t = createScopedTool({
      name: "multi",
      description: "test",
      parameters: z.object({}),
      secrets: {
        storage,
        refs: { a: { ref: "A" }, b: { ref: "B" } },
      },
      execute: async (_input, ctx) => `${ctx.secrets.a}/${ctx.secrets.b}`,
    });
    expect(await t.execute({}, { scope: fixedScope })).toBe("alpha/beta");
  });

  it("storage is captured at construction; tool body never sees it on ctx", async () => {
    const storage = new InMemorySecretsStorage();
    await storage.set({ scope: SecretScope.global(), key: "K", value: "v" });

    const t = createScopedTool({
      name: "isolation",
      description: "test",
      parameters: z.object({}),
      secrets: { storage, refs: { k: { ref: "K" } } },
      // ctx.secrets is Record<string, string> — values only, not the
      // storage primitive. Type system prevents accessing .storage from ctx.
      execute: async (_input, ctx) => {
        // @ts-expect-error — ctx.secrets has no `.storage` field
        const _leak = ctx.secrets.storage;
        return ctx.secrets.k;
      },
    });
    expect(await t.execute({}, { scope: fixedScope })).toBe("v");
  });
});
