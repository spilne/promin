// ---------------------------------------------------------------------------
// PostgresSecretsStorage — runs the @promin/agent secretsStorageTestSuite
// against a real PG container, plus crypto-specific tests.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { SecretScope } from "@promin/agent";
import { secretsStorageTestSuite } from "@promin/agent/testing";
import { migrate } from "../migrate.ts";
import { PostgresSecretsStorage } from "../postgres-secrets-storage.ts";
import { PostgresTestContainer } from "../test-utils.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE agent_secret`;
});

const TEST_PASSPHRASE = "test-only-passphrase-do-not-use-in-prod";

secretsStorageTestSuite(
  () => new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE }),
);

describe("PostgresSecretsStorage — Postgres-specific", () => {
  it("ciphertext at rest doesn't contain plaintext", async () => {
    const s = new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE });
    await s.set({
      scope: SecretScope.global(),
      key: "anthropic_api_key",
      value: "sk-ant-very-secret-value-12345",
    });
    const rows = await pg.sql`SELECT ciphertext FROM agent_secret`;
    expect(rows).toHaveLength(1);
    const cipher = (rows[0] as { ciphertext: string }).ciphertext;
    expect(cipher).not.toContain("sk-ant");
    expect(cipher).not.toContain("very-secret");
  });

  it("a different passphrase cannot decrypt secrets written by another", async () => {
    const writer = new PostgresSecretsStorage({ db: pg.db, passphrase: "passphrase-A" });
    await writer.set({ scope: SecretScope.global(), key: "k", value: "the-value" });

    const wrongReader = new PostgresSecretsStorage({ db: pg.db, passphrase: "passphrase-B" });
    // Decrypt should fail (auth tag mismatch). Surface as an exception
    // — better than silently returning garbage.
    expect(wrongReader.get({ scope: SecretScope.global(), key: "k" })).rejects.toThrow();
  });

  it("set is upsert: re-writing the same (scope, key) replaces the value", async () => {
    const s = new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE });
    await s.set({ scope: SecretScope.global(), key: "k", value: "first" });
    await s.set({ scope: SecretScope.global(), key: "k", value: "second" });
    expect(await s.get({ scope: SecretScope.global(), key: "k" })).toBe("second");
    const rows = await pg.sql`SELECT count(*)::int AS c FROM agent_secret`;
    expect((rows[0] as { c: number }).c).toBe(1);
  });

  it("each set generates a fresh IV (no key-reuse vulnerability)", async () => {
    const s = new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE });
    // Different keys, same value → IVs MUST differ to avoid the
    // GCM key-reuse-with-same-IV catastrophic break.
    await s.set({ scope: SecretScope.global(), key: "k1", value: "same-value" });
    await s.set({ scope: SecretScope.global(), key: "k2", value: "same-value" });
    const rows = await pg.sql`SELECT iv FROM agent_secret ORDER BY secret_key`;
    expect((rows[0] as { iv: string }).iv).not.toBe((rows[1] as { iv: string }).iv);
  });

  it("two storage instances against the same db see each other's writes", async () => {
    const writer = new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE });
    const reader = new PostgresSecretsStorage({ db: pg.db, passphrase: TEST_PASSPHRASE });
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
