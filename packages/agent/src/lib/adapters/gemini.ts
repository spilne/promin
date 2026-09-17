import { openai } from "./openai.ts";
import type { LLMProvider } from "../llm-provider.ts";

export interface GeminiOptions {
  apiKey?: string;
}

/**
 * Google Gemini adapter via the OpenAI-compatible endpoint.
 * Supports all models accessible through the Gemini API (e.g. "gemini-2.0-flash").
 *
 * Set GEMINI_API_KEY in the environment or pass `apiKey` explicitly.
 */
export function gemini(model: string, options: GeminiOptions = {}): LLMProvider {
  const apiKey = options.apiKey ?? process.env["GEMINI_API_KEY"];
  return openai(model, {
    apiKey,
    baseUrl: "https://generativelanguage.googleapis.com/v1beta/openai",
  });
}
