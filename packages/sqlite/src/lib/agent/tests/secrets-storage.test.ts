// ---------------------------------------------------------------------------
// SqliteSecretsStorage — runs the conformance suite + SQLite-specific
// (encryption-at-rest, IV freshness, custom-table sanity).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { SecretScope } from "@promin/agent";
import { secretsStorageTestSuite } from "@promin/agent/testing";
import { SqliteSecretsStorage } from "../secrets-storage.ts";

const TEST_PASSPHRASE = "test-only-passphrase";

secretsStorageTestSuite(() =>
  SqliteSecretsStorage.make({
    db: new Database(":memory:"),
    passphrase: TEST_PASSPHRASE,
  }),
);

describe("SqliteSecretsStorage — SQLite-specific", () => {
  it("ciphertext at rest doesn't contain plaintext", async () => {
    const db = new Database(":memory:");
    const s = SqliteSecretsStorage.make({ db, passphrase: TEST_PASSPHRASE });
    await s.set({
      scope: SecretScope.global(),
      key: "anthropic_api_key",
      value: "sk-ant-very-secret",
    });
    const rows = db.query("SELECT ciphertext FROM promin_secrets").all() as Array<{
      ciphertext: string;
    }>;
    expect(rows).toHaveLength(1);
    expect(rows[0]?.ciphertext).not.toContain("sk-ant");
    expect(rows[0]?.ciphertext).not.toContain("very-secret");
  });

  it("a different passphrase cannot decrypt secrets written by another", async () => {
    const db = new Database(":memory:");
    const writer = SqliteSecretsStorage.make({ db, passphrase: "passphrase-A" });
    await writer.set({ scope: SecretScope.global(), key: "k", value: "the-value" });

    const wrongReader = SqliteSecretsStorage.make({ db, passphrase: "passphrase-B" });
    expect(wrongReader.get({ scope: SecretScope.global(), key: "k" })).rejects.toThrow();
  });

  it("each set generates a fresh IV (no key-reuse vulnerability)", async () => {
    const db = new Database(":memory:");
    const s = SqliteSecretsStorage.make({ db, passphrase: TEST_PASSPHRASE });
    await s.set({ scope: SecretScope.global(), key: "k1", value: "same" });
    await s.set({ scope: SecretScope.global(), key: "k2", value: "same" });
    const rows = db.query("SELECT iv FROM promin_secrets ORDER BY secret_key").all() as Array<{
      iv: string;
    }>;
    expect(rows[0]?.iv).not.toBe(rows[1]?.iv);
  });

  it("respects a custom table name", async () => {
    const db = new Database(":memory:");
    const s = SqliteSecretsStorage.make({
      db,
      passphrase: TEST_PASSPHRASE,
      table: "my_secrets",
    });
    await s.set({ scope: SecretScope.global(), key: "k", value: "v" });
    const rows = db.query("SELECT secret_key FROM my_secrets").all() as Array<{
      secret_key: string;
    }>;
    expect(rows).toEqual([{ secret_key: "k" }]);
  });

  it("two storage instances against the same db see each other's writes", async () => {
    const db = new Database(":memory:");
    const writer = SqliteSecretsStorage.make({ db, passphrase: TEST_PASSPHRASE });
    const reader = SqliteSecretsStorage.make({ db, passphrase: TEST_PASSPHRASE });
    await writer.set({
      scope: SecretScope.namespace("acme"),
      key: "anthropic_api_key",
      value: "sk-ant-shared",
    });
    expect(
      await reader.get({ scope: SecretScope.namespace("acme"), key: "anthropic_api_key" }),
    ).toBe("sk-ant-shared");
  });
});
