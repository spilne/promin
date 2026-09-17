import { openai } from "./openai.ts";
import type { LLMProvider } from "../llm-provider.ts";

export interface LlamaCppOptions {
  /** Default: "local" (llama.cpp ignores this; it serves whatever model was loaded at startup) */
  model?: string;
  /** Default: http://localhost:8080 */
  baseURL?: string;
  temperature?: number;
  maxTokens?: number;
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
 *   agentLoop({ llm: llamacpp({ baseURL: "http://localhost:8080" }), ... })
 */
export function llamacpp(options: LlamaCppOptions = {}): LLMProvider {
  return openai(options.model ?? "local", {
    baseUrl: options.baseURL ?? "http://localhost:8080",
    apiKey: "no-key",
    ...(options.defaultHeaders ? { defaultHeaders: options.defaultHeaders } : {}),
  });
}
