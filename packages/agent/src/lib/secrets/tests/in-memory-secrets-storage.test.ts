// ---------------------------------------------------------------------------
// InMemorySecretsStorage — runs the conformance suite + a single
// in-process specifics test (cross-instance isolation).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { InMemorySecretsStorage } from "../in-memory-secrets-storage.ts";
import { secretsStorageTestSuite } from "../secrets-storage-test-suite.ts";
import { SecretScope } from "../types.ts";

secretsStorageTestSuite(() => new InMemorySecretsStorage());

describe("InMemorySecretsStorage — in-process specifics", () => {
  it("two instances don't share state", async () => {
    const a = new InMemorySecretsStorage();
    const b = new InMemorySecretsStorage();
    await a.set({ scope: SecretScope.global(), key: "k", value: "from-a" });
    expect(await b.get({ scope: SecretScope.global(), key: "k" })).toBeNull();
  });
});
