import { describe, it, expect } from "bun:test";
import { createRequireSecretTool } from "../tools/require-secret-tool.ts";
import { CompositeSecretStore, EnvSecretStore, InMemorySecretStore } from "../secret-store.ts";

describe("createRequireSecretTool", () => {
  it("calls readSecret with the provided prompt", async () => {
    const prompts: string[] = [];
    const t = createRequireSecretTool({
      readSecret: (p) => {
        prompts.push(p);
        return Promise.resolve("val");
      },
    });
    await t.execute({ key: "MY_KEY", prompt: "Enter your key" });
    expect(prompts).toEqual(["Enter your key"]);
  });

  it("defaults prompt to 'Enter <key>' when omitted", async () => {
    const prompts: string[] = [];
    const t = createRequireSecretTool({
      readSecret: (p) => {
        prompts.push(p);
        return Promise.resolve("val");
      },
    });
    await t.execute({ key: "MY_KEY" });
    expect(prompts[0]).toBe("Enter MY_KEY");
  });

  it("returns confirmation string, never the secret value", async () => {
    const SECRET = "super-secret-value-12345";
    const t = createRequireSecretTool({
      readSecret: () => Promise.resolve(SECRET),
    });
    const result = await t.execute({ key: "API_KEY", prompt: "Enter key" });
    expect(result).not.toContain(SECRET);
    expect(result).toBe("API_KEY has been set.");
  });

  it("calls custom store with key and value", async () => {
    const stored: Array<[string, string]> = [];
    const t = createRequireSecretTool({
      readSecret: () => Promise.resolve("secret-value"),
      store: (key, value) => {
        stored.push([key, value]);
        return Promise.resolve();
      },
    });
    await t.execute({ key: "VAULT_TOKEN", prompt: "Token" });
    expect(stored).toEqual([["VAULT_TOKEN", "secret-value"]]);
  });

  it("stores in process.env by default", async () => {
    const key = `TEST_SECRET_${Math.random().toString(36).slice(2).toUpperCase()}`;
    const t = createRequireSecretTool({ readSecret: () => Promise.resolve("env-value") });
    await t.execute({ key });
    expect(process.env[key]).toBe("env-value");
    delete process.env[key];
  });

  it("tool name is 'requireSecret'", () => {
    const t = createRequireSecretTool({ readSecret: () => Promise.resolve("x") });
    expect(t.name).toBe("requireSecret");
  });

  it("validates key must be UPPER_SNAKE_CASE", () => {
    const t = createRequireSecretTool({ readSecret: () => Promise.resolve("x") });
    expect(() => t.parameters.parse({ key: "lower-case" })).toThrow();
    expect(() => t.parameters.parse({ key: "VALID_KEY" })).not.toThrow();
  });
});

describe("requireSecret + CompositeSecretStore integration", () => {
  it("secret written via requireSecret is readable from the shared store", async () => {
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    const t = createRequireSecretTool({
      readSecret: () => Promise.resolve("my-gemini-key"),
      store: (key, value) => store.set(key, value),
    });

    await t.execute({ key: "GEMINI_API_KEY" });
    expect(await store.get("GEMINI_API_KEY")).toBe("my-gemini-key");
  });

  it("env var takes precedence over secret written via requireSecret", async () => {
    const envKey = "OPENAI_API_KEY_TEST_INTEGRATION";
    process.env[envKey] = "env-key";
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    const t = createRequireSecretTool({
      readSecret: () => Promise.resolve("user-provided-key"),
      store: (key, value) => store.set(key, value),
    });

    await t.execute({ key: envKey });
    expect(await store.get(envKey)).toBe("env-key");
    delete process.env[envKey];
  });

  it("second tool reads secret written by requireSecret without re-prompting", async () => {
    const store = new CompositeSecretStore([new EnvSecretStore(), new InMemorySecretStore()]);
    const requireSecret = createRequireSecretTool({
      readSecret: () => Promise.resolve("chatgpt-key"),
      store: (key, value) => store.set(key, value),
    });

    await requireSecret.execute({ key: "OPENAI_API_KEY" });

    // Simulates getSecret() in console-tools: reads from shared store, no prompt needed
    const promptCalls: string[] = [];
    async function getSecret(envKey: string): Promise<string> {
      const value = await store.get(envKey);
      if (!value) {
        promptCalls.push(envKey);
        return "fallback";
      }
      return value;
    }

    const key = await getSecret("OPENAI_API_KEY");
    expect(key).toBe("chatgpt-key");
    expect(promptCalls).toHaveLength(0);
  });
});
