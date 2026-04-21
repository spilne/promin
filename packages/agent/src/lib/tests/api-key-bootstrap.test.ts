import { describe, it, expect } from "bun:test";
import { createApiKeyBootstrap } from "../api-key-bootstrap.ts";
import { InMemorySecretStore } from "../secret-store.ts";

function makeStore() {
  return new InMemorySecretStore();
}

function makeCollect(value: string, calls: string[] = []) {
  return (prompt: string) => {
    calls.push(prompt);
    return Promise.resolve(value);
  };
}

const noMessages = { task: "hi", messages: [] };

describe("createApiKeyBootstrap — single key", () => {
  it("returns void and skips collection when key is already in store", async () => {
    const store = makeStore();
    await store.set("MY_KEY", "existing");
    const collected: string[] = [];
    const hook = createApiKeyBootstrap({ key: "MY_KEY", store, collectSecret: makeCollect("x", collected) });
    const result = await hook(noMessages);
    expect(result).toBeUndefined();
    expect(collected).toHaveLength(0);
  });

  it("calls collectSecret and stores the value when key is absent", async () => {
    const store = makeStore();
    const hook = createApiKeyBootstrap({ key: "MY_KEY", store, collectSecret: makeCollect("secret-val") });
    await hook(noMessages);
    expect(await store.get("MY_KEY")).toBe("secret-val");
  });

  it("uses default prompt 'Enter <key>' when no prompt supplied", async () => {
    const store = makeStore();
    const prompts: string[] = [];
    const hook = createApiKeyBootstrap({ key: "API_KEY", store, collectSecret: makeCollect("v", prompts) });
    await hook(noMessages);
    expect(prompts[0]).toBe("Enter API_KEY");
  });

  it("uses a fixed string prompt when prompt is a string", async () => {
    const store = makeStore();
    const prompts: string[] = [];
    const hook = createApiKeyBootstrap({ key: "X", store, prompt: "Custom prompt here", collectSecret: makeCollect("v", prompts) });
    await hook(noMessages);
    expect(prompts[0]).toBe("Custom prompt here");
  });

  it("uses a function prompt per key", async () => {
    const store = makeStore();
    const prompts: string[] = [];
    const hook = createApiKeyBootstrap({
      key: "SOME_KEY",
      store,
      prompt: (k) => `Please provide ${k} for the service`,
      collectSecret: makeCollect("v", prompts),
    });
    await hook(noMessages);
    expect(prompts[0]).toBe("Please provide SOME_KEY for the service");
  });

  it("sets process.env[key] by default", async () => {
    const envKey = `TEST_BOOTSTRAP_${Math.random().toString(36).slice(2).toUpperCase()}`;
    const store = makeStore();
    const hook = createApiKeyBootstrap({ key: envKey, store, collectSecret: makeCollect("env-val") });
    await hook(noMessages);
    expect(process.env[envKey]).toBe("env-val");
    delete process.env[envKey];
  });

  it("does not set process.env when setEnv is false", async () => {
    const envKey = `TEST_BOOTSTRAP_${Math.random().toString(36).slice(2).toUpperCase()}`;
    const store = makeStore();
    const hook = createApiKeyBootstrap({ key: envKey, store, setEnv: false, collectSecret: makeCollect("val") });
    await hook(noMessages);
    expect(process.env[envKey]).toBeUndefined();
  });

  it("skips collection on subsequent calls once key is stored", async () => {
    const store = makeStore();
    const collected: string[] = [];
    const hook = createApiKeyBootstrap({ key: "K", store, collectSecret: makeCollect("v", collected) });
    await hook(noMessages); // first turn — collects
    await hook(noMessages); // second turn — already stored
    await hook(noMessages); // third turn — still stored
    expect(collected).toHaveLength(1);
  });
});

describe("createApiKeyBootstrap — multiple keys", () => {
  it("collects each missing key in order", async () => {
    const store = makeStore();
    const prompts: string[] = [];
    let i = 0;
    const hook = createApiKeyBootstrap({
      key: ["KEY_A", "KEY_B"],
      store,
      collectSecret: (p) => { prompts.push(p); return Promise.resolve(`val-${i++}`); },
    });
    await hook(noMessages);
    expect(await store.get("KEY_A")).toBe("val-0");
    expect(await store.get("KEY_B")).toBe("val-1");
    expect(prompts).toEqual(["Enter KEY_A", "Enter KEY_B"]);
  });

  it("only collects keys that are missing, skips present ones", async () => {
    const store = makeStore();
    await store.set("KEY_A", "already-set");
    const collected: string[] = [];
    const hook = createApiKeyBootstrap({
      key: ["KEY_A", "KEY_B"],
      store,
      collectSecret: makeCollect("new-val", collected),
    });
    await hook(noMessages);
    expect(collected).toHaveLength(1);
    expect(await store.get("KEY_A")).toBe("already-set");
    expect(await store.get("KEY_B")).toBe("new-val");
  });
});
