import { openai } from "./openai.ts";
import type { LLMProvider } from "../llm-provider.ts";
import type { LLMHttpOptions } from "./llm-http.ts";

export interface LlamaCppOptions extends LLMHttpOptions {
  /** Default: "local" (llama.cpp ignores this; it serves whatever model was loaded at startup) */
  model?: string;
  /** Server root. Default: http://localhost:8080 */
  baseURL?: string;
  defaultHeaders?: Record<string, string>;
}

/**
 * LLM adapter for llama.cpp server (https://github.com/ggerganov/llama.cpp).
 *
 * llama.cpp exposes an OpenAI-compatible /v1/chat/completions endpoint, so this
 * is a thin convenience wrapper around the openai() adapter with sensible defaults.
 *
 * Start a llama.cpp server with:
 *   llama-server --model ./your-model.gguf --port 8080
 *
 * Then wire it into an agent:
 *   agentAction({ llm: llamacpp({ baseURL: "http://localhost:8080" }), ... })
 */
export function llamacpp(options: LlamaCppOptions = {}): LLMProvider {
  const { model, baseURL, ...rest } = options;
  return openai(model ?? "local", {
    ...rest,
    baseUrl: `${(baseURL ?? "http://localhost:8080").replace(/\/$/, "")}/v1`,
    apiKey: "no-key",
    providerName: "llamacpp",
  });
}
