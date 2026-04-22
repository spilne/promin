import { describe, it, expect } from "bun:test";
import { createRequireSecretTool } from "../tools/require-secret-tool.ts";

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
