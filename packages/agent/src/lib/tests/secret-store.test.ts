import { describe, it, expect, afterEach } from "bun:test";
import { rm } from "node:fs/promises";
import { join } from "node:path";
import {
  InMemorySecretStore,
  EnvSecretStore,
  FileSecretStore,
  CompositeSecretStore,
} from "../secret-store.ts";

describe("InMemorySecretStore", () => {
  it("get returns undefined for unknown key", async () => {
    const store = new InMemorySecretStore();
    expect(await store.get("MISSING")).toBeUndefined();
  });

  it("set / get round-trips", async () => {
    const store = new InMemorySecretStore();
    await store.set("API_KEY", "secret123");
    expect(await store.get("API_KEY")).toBe("secret123");
  });

  it("has returns true after set", async () => {
    const store = new InMemorySecretStore();
    await store.set("TOKEN", "abc");
    expect(await store.has("TOKEN")).toBe(true);
  });

  it("has returns false for unknown key", async () => {
    const store = new InMemorySecretStore();
    expect(await store.has("NOPE")).toBe(false);
  });

  it("delete removes the key", async () => {
    const store = new InMemorySecretStore();
    await store.set("KEY", "val");
    await store.delete("KEY");
    expect(await store.has("KEY")).toBe(false);
    expect(await store.get("KEY")).toBeUndefined();
  });

  it("delete is a no-op for missing key", async () => {
    const store = new InMemorySecretStore();
    await store.delete("NEVER_SET"); // should not throw
  });

  it("overwrite replaces value", async () => {
    const store = new InMemorySecretStore();
    await store.set("KEY", "old");
    await store.set("KEY", "new");
    expect(await store.get("KEY")).toBe("new");
  });
});

describe("EnvSecretStore", () => {
  it("reads from process.env", async () => {
    process.env["TEST_SECRET_VAR"] = "from-env";
    const store = new EnvSecretStore();
    expect(await store.get("TEST_SECRET_VAR")).toBe("from-env");
    delete process.env["TEST_SECRET_VAR"];
  });

  it("has returns false for absent env var", async () => {
    delete process.env["DEFINITELY_ABSENT_XYZ"];
    const store = new EnvSecretStore();
    expect(await store.has("DEFINITELY_ABSENT_XYZ")).toBe(false);
  });

  it("set throws", async () => {
    const store = new EnvSecretStore();
    await expect(store.set("K", "v")).rejects.toThrow("read-only");
  });

  it("delete throws", async () => {
    const store = new EnvSecretStore();
    await expect(store.delete("K")).rejects.toThrow("read-only");
  });
});

describe("CompositeSecretStore", () => {
  it("reads from first store that has the key", async () => {
    const primary = new InMemorySecretStore();
    const secondary = new InMemorySecretStore();
    await secondary.set("KEY", "from-secondary");

    const store = new CompositeSecretStore([primary, secondary]);
    expect(await store.get("KEY")).toBe("from-secondary");
  });

  it("primary takes precedence over secondary", async () => {
    const primary = new InMemorySecretStore();
    const secondary = new InMemorySecretStore();
    await primary.set("KEY", "from-primary");
    await secondary.set("KEY", "from-secondary");

    const store = new CompositeSecretStore([primary, secondary]);
    expect(await store.get("KEY")).toBe("from-primary");
  });

  it("writes go to primary only", async () => {
    const primary = new InMemorySecretStore();
    const secondary = new InMemorySecretStore();

    const store = new CompositeSecretStore([primary, secondary]);
    await store.set("KEY", "val");

    expect(await primary.get("KEY")).toBe("val");
    expect(await secondary.get("KEY")).toBeUndefined();
  });

  it("has returns true if any store has the key", async () => {
    const a = new InMemorySecretStore();
    const b = new InMemorySecretStore();
    await b.set("KEY", "x");

    const store = new CompositeSecretStore([a, b]);
    expect(await store.has("KEY")).toBe(true);
  });

  it("get returns undefined when no store has the key", async () => {
    const store = new CompositeSecretStore([new InMemorySecretStore()]);
    expect(await store.get("MISSING")).toBeUndefined();
  });

  it("skips read-only primary and writes to next writable store", async () => {
    const mem = new InMemorySecretStore();
    const store = new CompositeSecretStore([new EnvSecretStore(), mem]);
    await store.set("COMPOSITE_WRITE_TEST", "written");
    expect(await mem.get("COMPOSITE_WRITE_TEST")).toBe("written");
    expect(process.env["COMPOSITE_WRITE_TEST"]).toBeUndefined();
  });

  it("throws when all stores reject writes", async () => {
    const store = new CompositeSecretStore([new EnvSecretStore()]);
    await expect(store.set("K", "v")).rejects.toThrow("no writable store");
  });

  it("skips read-only primary and deletes from next writable store", async () => {
    const mem = new InMemorySecretStore();
    await mem.set("DEL_KEY", "val");
    const store = new CompositeSecretStore([new EnvSecretStore(), mem]);
    await store.delete("DEL_KEY");
    expect(await mem.has("DEL_KEY")).toBe(false);
  });
});

describe("CompositeSecretStore — env-first pattern", () => {
  const KEY = "COMPOSITE_ENV_TEST_KEY";

  it("reads from env when present", async () => {
    process.env[KEY] = "from-env";
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    expect(await store.get(KEY)).toBe("from-env");
    delete process.env[KEY];
  });

  it("falls back to in-memory when env var is absent", async () => {
    delete process.env[KEY];
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    await store.set(KEY, "in-memory-value");
    expect(await store.get(KEY)).toBe("in-memory-value");
  });

  it("env var takes precedence over written in-memory value", async () => {
    process.env[KEY] = "env-value";
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    await store.set(KEY, "mem-value");
    expect(await store.get(KEY)).toBe("env-value");
    delete process.env[KEY];
  });

  it("set writes to in-memory, not to process.env", async () => {
    delete process.env[KEY];
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    await store.set(KEY, "written");
    expect(process.env[KEY]).toBeUndefined();
    expect(await store.get(KEY)).toBe("written");
  });
});

describe("FileSecretStore", () => {
  const path = join("/tmp", `test-secrets-${process.pid}.json`);

  afterEach(async () => {
    await rm(path, { force: true });
  });

  it("returns undefined for missing key before any write", async () => {
    const store = new FileSecretStore({ path, passphrase: "test-pass" });
    expect(await store.get("NOPE")).toBeUndefined();
  });

  it("set / get round-trips across separate instances (same passphrase)", async () => {
    const store1 = new FileSecretStore({ path, passphrase: "my-pass" });
    await store1.set("API_KEY", "super-secret");

    const store2 = new FileSecretStore({ path, passphrase: "my-pass" });
    expect(await store2.get("API_KEY")).toBe("super-secret");
  });

  it("wrong passphrase cannot decrypt", async () => {
    const store1 = new FileSecretStore({ path, passphrase: "correct" });
    await store1.set("KEY", "value");

    const store2 = new FileSecretStore({ path, passphrase: "wrong" });
    await expect(store2.get("KEY")).rejects.toThrow();
  });

  it("delete removes key and persists", async () => {
    const store1 = new FileSecretStore({ path, passphrase: "pass" });
    await store1.set("KEY", "val");
    await store1.delete("KEY");

    const store2 = new FileSecretStore({ path, passphrase: "pass" });
    expect(await store2.has("KEY")).toBe(false);
  });

  it("stores multiple keys independently", async () => {
    const store = new FileSecretStore({ path, passphrase: "pass" });
    await store.set("A", "alpha");
    await store.set("B", "beta");

    expect(await store.get("A")).toBe("alpha");
    expect(await store.get("B")).toBe("beta");
  });
});
