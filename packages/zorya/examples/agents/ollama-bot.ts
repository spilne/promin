// ---------------------------------------------------------------------------
// ollama-bot — local LLM via the Ollama HTTP API.
//
// Wraps qwen2.5:0.5b running on http://localhost:11434. Free, local, and
// fast enough for demos. Pull the model first:
//
//   ollama pull qwen2.5:0.5b
//
// The recipe declares `provider: "ollama"` so the demo's per-agent LLM
// map (in demo.ts) wires `ollama({ model: "qwen2.5:0.5b" })` from
// @promin/agent's adapter at boot.
// ---------------------------------------------------------------------------

import type { RegisterAgentInput } from "@promin/agent";

export const OLLAMA_BOT: RegisterAgentInput = {
  id: "ollama-bot",
  backend: {
    type: "local",
    model: { provider: "ollama", id: "qwen2.5:0.5b" },
    systemPrompt:
      "You are a concise, helpful assistant powered by a local Qwen 2.5 0.5B model running " +
      "via Ollama. Keep replies short — one or two sentences unless the user asks for more. " +
      "If you do not know something, say so plainly instead of guessing.\n\n" +
      "You have a `scheduler` tool that lets you create durable schedules (cron / interval / " +
      "RRULE). When the user asks to be reminded, schedule a recurring task, or fire something " +
      "on a cadence, call `scheduler.create(...)`. Confirm cadence + timezone before creating.",
    tools: ["scheduler"],
  },
  metadata: {
    description:
      "Qwen 2.5 0.5B via local Ollama (http://localhost:11434). Free, fast, no API key needed.",
    capabilities: ["chat"],
    tags: ["demo", "local", "ollama"],
  },
};
