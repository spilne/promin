// ---------------------------------------------------------------------------
// Portable `SecretsStorage` conformance suite. Every implementation
// must pass: in-memory, Postgres, future variants.
//
// Usage:
//   import { secretsStorageTestSuite } from "@promin/agent/testing";
//   secretsStorageTestSuite(() => new InMemorySecretsStorage());
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { SecretScope, type SecretsStorage } from "./types.ts";

export function secretsStorageTestSuite(factory: () => SecretsStorage | Promise<SecretsStorage>) {
  async function make(): Promise<SecretsStorage> {
    return factory();
  }

  describe("SecretsStorage conformance", () => {
    describe("set + get (exact scope)", () => {
      it("round-trips a value at global scope", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "anthropic_api_key", value: "sk-ant" });
        const v = await s.get({ scope: SecretScope.global(), key: "anthropic_api_key" });
        expect(v).toBe("sk-ant");
      });

      it("round-trips a value at namespace scope", async () => {
        const s = await make();
        await s.set({
          scope: SecretScope.namespace("acme"),
          key: "anthropic_api_key",
          value: "ns-key",
        });
        const v = await s.get({
          scope: SecretScope.namespace("acme"),
          key: "anthropic_api_key",
        });
        expect(v).toBe("ns-key");
      });

      it("round-trips a value at resource scope", async () => {
        const s = await make();
        await s.set({
          scope: SecretScope.resource("acme", "alice"),
          key: "anthropic_api_key",
          value: "alice-key",
        });
        const v = await s.get({
          scope: SecretScope.resource("acme", "alice"),
          key: "anthropic_api_key",
        });
        expect(v).toBe("alice-key");
      });

      it("returns null for an unknown key at the requested scope", async () => {
        const s = await make();
        const v = await s.get({ scope: SecretScope.global(), key: "missing" });
        expect(v).toBeNull();
      });

      it("get is exact-scope: a global key is invisible at namespace scope", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "anthropic_api_key", value: "global" });
        const v = await s.get({
          scope: SecretScope.namespace("acme"),
          key: "anthropic_api_key",
        });
        expect(v).toBeNull();
      });

      it("set replaces an existing value at the same scope", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "first" });
        await s.set({ scope: SecretScope.global(), key: "k", value: "second" });
        const v = await s.get({ scope: SecretScope.global(), key: "k" });
        expect(v).toBe("second");
      });
    });

    describe("resolve (cascade)", () => {
      it("returns the resource-scoped value when present", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        await s.set({ scope: SecretScope.namespace("acme"), key: "k", value: "ns-v" });
        await s.set({ scope: SecretScope.resource("acme", "alice"), key: "k", value: "res-v" });
        const r = await s.resolve({ namespaceId: "acme", resourceId: "alice", key: "k" });
        expect(r?.value).toBe("res-v");
        expect(r?.scope.kind).toBe("resource");
      });

      it("falls through to namespace when resource scope is empty", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        await s.set({ scope: SecretScope.namespace("acme"), key: "k", value: "ns-v" });
        const r = await s.resolve({ namespaceId: "acme", resourceId: "alice", key: "k" });
        expect(r?.value).toBe("ns-v");
        expect(r?.scope.kind).toBe("namespace");
      });

      it("falls through to global when neither resource nor namespace scope has the key", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        const r = await s.resolve({ namespaceId: "acme", resourceId: "alice", key: "k" });
        expect(r?.value).toBe("global-v");
        expect(r?.scope.kind).toBe("global");
      });

      it("returns null when no scope has the key", async () => {
        const s = await make();
        const r = await s.resolve({ namespaceId: "acme", resourceId: "alice", key: "missing" });
        expect(r).toBeNull();
      });

      it("walks namespace → global when only namespaceId is provided", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        const r = await s.resolve({ namespaceId: "acme", key: "k" });
        expect(r?.value).toBe("global-v");
        expect(r?.scope.kind).toBe("global");
      });

      it("global-only when no scope params provided", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        const r = await s.resolve({ key: "k" });
        expect(r?.value).toBe("global-v");
      });

      it("ignores resourceId when namespaceId is missing (no orphan resource lookups)", async () => {
        const s = await make();
        await s.set({
          scope: SecretScope.resource("acme", "alice"),
          key: "k",
          value: "alice-v",
        });
        const r = await s.resolve({ resourceId: "alice", key: "k" });
        expect(r).toBeNull();
      });

      it("namespace boundary isolates: namespace 'acme' does not see namespace 'globex'", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.namespace("globex"), key: "k", value: "globex-v" });
        const r = await s.resolve({ namespaceId: "acme", key: "k" });
        expect(r).toBeNull();
      });
    });

    describe("delete", () => {
      it("removes the value at exact scope, leaves other scopes untouched", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "k", value: "global-v" });
        await s.set({ scope: SecretScope.namespace("acme"), key: "k", value: "ns-v" });
        await s.delete({ scope: SecretScope.namespace("acme"), key: "k" });
        expect(await s.get({ scope: SecretScope.namespace("acme"), key: "k" })).toBeNull();
        expect(await s.get({ scope: SecretScope.global(), key: "k" })).toBe("global-v");
      });

      it("is a no-op when the key isn't there", async () => {
        const s = await make();
        await s.delete({ scope: SecretScope.global(), key: "never" });
      });
    });

    describe("list", () => {
      it("returns keys at exact scope (NOT cascaded)", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "global_only", value: "g" });
        await s.set({ scope: SecretScope.namespace("acme"), key: "ns_only", value: "n" });
        await s.set({ scope: SecretScope.namespace("acme"), key: "shared", value: "n2" });
        await s.set({ scope: SecretScope.global(), key: "shared", value: "g2" });

        const globalKeys = await s.list({ scope: SecretScope.global() });
        expect(globalKeys.sort()).toEqual(["global_only", "shared"]);

        const nsKeys = await s.list({ scope: SecretScope.namespace("acme") });
        expect(nsKeys.sort()).toEqual(["ns_only", "shared"]);
      });

      it("returns an empty array for a scope with no entries", async () => {
        const s = await make();
        const keys = await s.list({ scope: SecretScope.namespace("empty") });
        expect(keys).toEqual([]);
      });

      it("does NOT leak values — list returns names only", async () => {
        const s = await make();
        await s.set({ scope: SecretScope.global(), key: "anthropic_api_key", value: "sk-secret" });
        const keys = await s.list({ scope: SecretScope.global() });
        // The result is plain strings (key names) — no value field, no joining.
        for (const k of keys) {
          expect(typeof k).toBe("string");
          expect(k).not.toContain("sk-secret");
        }
      });
    });
  });
}
