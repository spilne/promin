import { z } from "zod";
import { tool } from "../tool.ts";
import type { AgentTool } from "../tool.ts";

export interface RequireSecretToolConfig {
  /**
   * Injected by the host — captures the secret value out-of-band (REPL prompt,
   * TUI, HTTP form, test mock, etc.). Return the raw value; it is never sent to the LLM.
   */
  readSecret: (prompt: string) => Promise<string>;
  /**
   * Where to store the captured value. Defaults to mutating process.env[key].
   * Override to persist to a SecretStore, Vault, etc.
   */
  store?: (key: string, value: string) => Promise<void>;
}

/**
 * Returns a tool that lets the agent pause and request a secret from the user.
 *
 * The captured value is stored via `store` (default: process.env mutation) and
 * only a confirmation string is returned to the LLM — the value itself never
 * enters the conversation history.
 *
 * Typical flow:
 *   Agent: "I need GOOGLE_API_KEY to proceed."
 *     → calls requireSecret({ key: "GOOGLE_API_KEY" })
 *   Host:  prompts user out-of-band, stores result
 *   Agent: sees "GOOGLE_API_KEY has been set." and continues
 */
export function createRequireSecretTool(
  config: RequireSecretToolConfig,
): AgentTool<{ key: string; prompt?: string }, string> {
  const store: (key: string, value: string) => Promise<void> =
    config.store ??
    ((key, value) => {
      process.env[key] = value;
      return Promise.resolve();
    });

  return tool({
    name: "requireSecret",
    description:
      "Request a secret value (API key, token, password) from the user. " +
      "The user is prompted directly — the value is stored securely and never returned to you. " +
      "After this call, the secret is available as process.env[key] in tool code.",
    usage:
      "Call before writing or using a tool that needs a credential not yet available. " +
      "Use UPPER_SNAKE_CASE for key names matching standard env var conventions.",
    examples: [
      {
        input: { key: "GOOGLE_API_KEY", prompt: "Enter your Google Custom Search API key" },
        output: "GOOGLE_API_KEY has been set.",
      },
    ],
    parameters: z.object({
      key: z
        .string()
        .regex(/^[A-Z][A-Z0-9_]*$/, "UPPER_SNAKE_CASE env var name, e.g. GOOGLE_API_KEY")
        .describe("The environment variable name the secret will be stored under."),
      prompt: z
        .string()
        .optional()
        .describe(
          "Human-readable prompt shown to the user. Defaults to 'Enter <key>:' if omitted.",
        ),
    }),
    execute: async ({ key, prompt }) => {
      const value = await config.readSecret(prompt ?? `Enter ${key}`);
      await store(key, value);
      return `${key} has been set.`;
    },
  });
}
