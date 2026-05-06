// ---------------------------------------------------------------------------
// InMemorySecretsStorage — single-process SecretsStorage. No encryption
// at rest (lives in JS heap); prod path is PostgresSecretsStorage in
// @promin/postgres. Both pass `secretsStorageTestSuite`.
// ---------------------------------------------------------------------------

import type { ResolvedSecret, SecretScope, SecretsStorage } from "./types.ts";

export class InMemorySecretsStorage implements SecretsStorage {
  /** Keyed by `${kind}|${ns ?? ""}|${res ?? ""}|${key}`. */
  private readonly entries = new Map<string, string>();

  async get(params: { scope: SecretScope; key: string }): Promise<string | null> {
    return this.entries.get(encode(params.scope, params.key)) ?? null;
  }

  async resolve(params: {
    namespaceId?: string;
    resourceId?: string;
    key: string;
  }): Promise<ResolvedSecret | null> {
    // resource → namespace → global, return first hit.
    if (params.namespaceId && params.resourceId) {
      const scope: SecretScope = {
        kind: "resource",
        namespaceId: params.namespaceId,
        resourceId: params.resourceId,
      };
      const v = this.entries.get(encode(scope, params.key));
      if (v !== undefined) return { value: v, scope };
    }
    if (params.namespaceId) {
      const scope: SecretScope = { kind: "namespace", namespaceId: params.namespaceId };
      const v = this.entries.get(encode(scope, params.key));
      if (v !== undefined) return { value: v, scope };
    }
    const globalScope: SecretScope = { kind: "global" };
    const v = this.entries.get(encode(globalScope, params.key));
    if (v !== undefined) return { value: v, scope: globalScope };
    return null;
  }

  async set(params: { scope: SecretScope; key: string; value: string }): Promise<void> {
    this.entries.set(encode(params.scope, params.key), params.value);
  }

  async delete(params: { scope: SecretScope; key: string }): Promise<void> {
    this.entries.delete(encode(params.scope, params.key));
  }

  async list(params: { scope: SecretScope }): Promise<string[]> {
    const prefix = encodeScopePrefix(params.scope);
    const out: string[] = [];
    for (const fullKey of this.entries.keys()) {
      if (fullKey.startsWith(prefix)) {
        out.push(fullKey.slice(prefix.length));
      }
    }
    return out.sort();
  }
}

function encode(scope: SecretScope, key: string): string {
  return `${encodeScopePrefix(scope)}${key}`;
}

function encodeScopePrefix(scope: SecretScope): string {
  switch (scope.kind) {
    case "global":
      return "global||";
    case "namespace":
      return `namespace|${scope.namespaceId}|`;
    case "resource":
      return `resource|${scope.namespaceId}|${scope.resourceId}|`;
  }
}
