// ---------------------------------------------------------------------------
// ollama-bot — local LLM via the Ollama HTTP API.
//
// Defaults to qwen2.5:3b — best tool-call discipline at ~2GB. It chats
// in plain text on greetings / casual messages and reaches for the
// scheduler tool only when intent is clear. Other models tested at the
// same size (llama3.2:3b) misfire on ambiguous prompts (emit malformed
// tool calls or leak JSON into the content field). Pull it first:
//
//   ollama pull qwen2.5:3b
//
// Override via `OLLAMA_MODEL=...` env var. The recipe's `model.id` is a
// label only — the live mapping lives in demo.ts and reads OLLAMA_MODEL.
// ---------------------------------------------------------------------------

import type { RegisterAgentInput } from "@promin/agent";

export const OLLAMA_BOT: RegisterAgentInput = {
  id: "ollama-bot",
  backend: {
    type: "local",
    model: { provider: "ollama", id: "qwen2.5:3b" },
    systemPrompt:
      "You are a concise, helpful assistant running on a local Qwen 2.5 3B model via " +
      "Ollama. Keep replies short — one or two sentences unless the user asks for more. " +
      "If you do not know something, say so plainly instead of guessing.",
    tools: ["schedulerCreate", "schedulerList", "schedulerCancel"],
  },
  metadata: {
    description:
      "Qwen 2.5 3B via local Ollama (http://localhost:11434). Free, no API key. " +
      "Override OLLAMA_MODEL for a different local model.",
    capabilities: ["chat", "tools"],
    tags: ["demo", "local", "ollama"],
  },
};
