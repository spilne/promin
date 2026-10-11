import { openai } from "./openai.ts";
import type { LLMProvider } from "../llm-provider.ts";
import type { LLMHttpOptions } from "./llm-http.ts";

export interface GeminiOptions extends LLMHttpOptions {
  apiKey?: string;
  /** Default: Google's OpenAI-compatible endpoint, `https://generativelanguage.googleapis.com/v1beta/openai`. */
  baseUrl?: string;
}

/**
 * Google Gemini adapter via the OpenAI-compatible endpoint
 * (`POST https://generativelanguage.googleapis.com/v1beta/openai/chat/completions`).
 * Supports all models accessible through the Gemini API (e.g. "gemini-2.5-flash").
 *
 * Set GEMINI_API_KEY in the environment or pass `apiKey` explicitly.
 */
export function gemini(model: string, options: GeminiOptions = {}): LLMProvider {
  const { apiKey, baseUrl, ...http } = options;
  return openai(model, {
    ...http,
    apiKey: apiKey ?? process.env["GEMINI_API_KEY"],
    baseUrl: baseUrl ?? "https://generativelanguage.googleapis.com/v1beta/openai",
    providerName: "gemini",
  });
}
