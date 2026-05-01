// ---------------------------------------------------------------------------
// ollama-bot — local LLM via the Ollama HTTP API.
//
// Defaults to llama3.2:3b — the smallest model that reliably emits
// tool calls with the right schema in this demo. Smaller models
// (qwen2.5:0.5b / 1.5b) either invent parameter names or skip tool
// calls. Pull it first:
//
//   ollama pull llama3.2:3b
//
// Override via `OLLAMA_MODEL=...` env var. The recipe's `model.id` is a
// label only — the live mapping lives in demo.ts and reads OLLAMA_MODEL.
// ---------------------------------------------------------------------------

import type { RegisterAgentInput } from "@promin/agent";

export const OLLAMA_BOT: RegisterAgentInput = {
  id: "ollama-bot",
  backend: {
    type: "local",
    model: { provider: "ollama", id: "llama3.2:3b" },
    systemPrompt:
      "You are a concise, helpful assistant running on a local Llama 3.2 3B model via " +
      "Ollama. Keep replies short — one or two sentences unless the user asks for more. " +
      "If you do not know something, say so plainly instead of guessing.",
    tools: ["scheduler"],
  },
  metadata: {
    description:
      "Llama 3.2 3B via local Ollama (http://localhost:11434). Free, no API key. " +
      "Override OLLAMA_MODEL for a different local model.",
    capabilities: ["chat", "tools"],
    tags: ["demo", "local", "ollama"],
  },
};
