import { z } from "zod";
import { tool } from "../tool.ts";
import type { AgentTool } from "../tool.ts";
import type { SecretStore } from "../secret-store.ts";

export interface SecretToolConfig {
  store: SecretStore;
}

export interface SetSecretToolConfig extends SecretToolConfig {
  /** Whether to require human approval before storing. Default: false. */
  requireApproval?: boolean;
}

/**
 * Lets the agent check whether a named secret is available.
 * Returns only availability — never the actual value.
 */
export function createGetSecretTool(config: SecretToolConfig): AgentTool<{ key: string }, string> {
  return tool({
    name: "getSecret",
    description:
      "Check whether a named secret (API key, token, credential) is available in the store.",
    usage:
      "Call before any operation that requires a credential. " +
      "If the secret is not found, inform the user and ask them to provide it.",
    examples: [
      { input: { key: "GITHUB_TOKEN" }, output: 'Secret "GITHUB_TOKEN" is available.' },
      {
        input: { key: "SLACK_TOKEN" },
        output: 'Secret "SLACK_TOKEN" not found. Ask the user to provide it.',
      },
    ],
    parameters: z.object({
      key: z.string().describe("Secret key name, e.g. GITHUB_TOKEN"),
    }),
    execute: async ({ key }) => {
      return (await config.store.has(key))
        ? `Secret "${key}" is available.`
        : `Secret "${key}" not found. Ask the user to provide it.`;
    },
  });
}

/**
 * Lets the agent store a secret provided by the user during conversation.
 * The raw value is persisted in the store but masked in the model output
 * so it does not reappear in subsequent LLM context.
 *
 * Note: if the user typed the secret in the conversation, it is already
 * in the message history. This tool prevents it from appearing in additional
 * places (journal results, future model context).
 */
export function createSetSecretTool(
  config: SetSecretToolConfig,
): AgentTool<{ key: string; value: string }, string> {
  return tool({
    name: "setSecret",
    description: "Store a named secret provided by the user. Persists across sessions.",
    usage:
      "Use after the user provides a credential in the conversation. " +
      "The value is stored and masked — it will not appear in future responses.",
    examples: [
      {
        input: { key: "GITHUB_TOKEN", value: "ghp_xxxx" },
        output: 'Secret "GITHUB_TOKEN" stored.',
      },
    ],
    parameters: z.object({
      key: z.string().describe("Secret key name, e.g. GITHUB_TOKEN"),
      value: z.string().describe("The secret value provided by the user."),
    }),
    requireApproval: config.requireApproval ?? false,
    toModelOutput: (key) => `Secret "${key}" stored.`,
    execute: async ({ key, value }) => {
      await config.store.set(key, value);
      return key; // raw value never returned — toModelOutput uses the key for confirmation
    },
  });
}
