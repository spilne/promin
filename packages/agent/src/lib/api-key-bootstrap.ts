import type { HooksConfig } from "./agent-loop.ts";
import type { SecretStore } from "./secret-store.ts";

export interface ApiKeyBootstrapConfig {
  /**
   * The store key (and env var name) to check, e.g. "OPENAI_API_KEY".
   * Supply an array to require multiple keys — each missing key is collected in order.
   */
  key: string | string[];
  store: SecretStore;
  /**
   * Out-of-band collection — injected by the host, same pattern as createRequireSecretTool.
   * Called once per missing key; return the raw value. It is stored but never sent to the LLM.
   */
  collectSecret: (prompt: string) => Promise<string>;
  /**
   * Prompt template. Supply a string for a fixed message, or a function for per-key prompts.
   * Default: `Enter <key>:`.
   */
  prompt?: string | ((key: string) => string);
  /**
   * Also write the collected value to process.env[key]. Default: true.
   * Disable when the host resolves secrets another way (e.g. injecting via SecretStore only).
   */
  setEnv?: boolean;
}

/**
 * Returns a beforeTurn hook that gates the session on one or more required secrets.
 *
 * On each turn, checks the store for every required key. For any missing key it
 * calls collectSecret() out-of-band (no LLM involvement), persists the value via
 * the store, and optionally sets process.env[key]. The conversation then proceeds
 * normally — the LLM never sees the collection prompt or the secret value.
 *
 * Typical usage with FileSecretStore so secrets survive process restarts:
 *
 *   agentLoop({
 *     hooks: {
 *       beforeTurn: createApiKeyBootstrap({
 *         key: "OPENAI_API_KEY",
 *         store: new FileSecretStore({ path: ".secrets", passphrase: hostPassphrase }),
 *         collectSecret: (prompt) => readLineHidden(prompt),
 *       }),
 *     },
 *   })
 */
export function createApiKeyBootstrap(
  config: ApiKeyBootstrapConfig,
): NonNullable<HooksConfig["beforeTurn"]> {
  const keys = Array.isArray(config.key) ? config.key : [config.key];
  const setEnv = config.setEnv !== false;

  function promptFor(key: string): string {
    if (!config.prompt) return `Enter ${key}`;
    if (typeof config.prompt === "function") return config.prompt(key);
    return config.prompt;
  }

  return async () => {
    for (const key of keys) {
      if (await config.store.has(key)) continue;
      const value = await config.collectSecret(promptFor(key));
      await config.store.set(key, value);
      if (setEnv) process.env[key] = value;
    }
  };
}
