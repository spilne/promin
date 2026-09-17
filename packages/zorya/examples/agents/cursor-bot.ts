import type { RegisterAgentInput } from "@promin/agent";

// Cursor CLI-backed agent. Each invocation spawns `agent -p ...` (the
// Cursor CLI binary) and routes the conversation through Cursor's
// stream-json output.
//
// Requirements at the host machine:
//   1. `agent` (or `cursor-agent` legacy alias) on PATH —
//      `curl https://cursor.com/install -fsS | bash`
//   2. `CURSOR_API_KEY` set (or be logged in via `agent login`)
//
// The demo's boot wiring filters this recipe out when the env var
// isn't set — it stays in the registry but `resolveCursorAgent` will
// throw at materialisation time, so the dashboard skips it cleanly.
export const CURSOR_BOT: RegisterAgentInput = {
  id: "cursor-bot",
  backend: {
    type: "cursor",
    // `auto` lets Cursor's router pick the best available model for
    // each turn. Override per environment if you want to pin a
    // specific one (e.g. "sonnet-4.5-thinking", "composer-2") — Cursor
    // accepts a dynamic set; query `agent --list-models` for the live
    // list at any time.
    model: "auto",
    // Skip Cursor's interactive trust prompt when spawning; required
    // for headless server use.
    trust: true,
    requiredEnv: ["CURSOR_API_KEY"],
  },
  metadata: {
    description:
      "Cursor's CLI agent (`agent -p`) wrapped as a chat backend. Strong at coding tasks (file reads/writes, refactors, multi-file implementation). Auth via CURSOR_API_KEY; binary install: curl https://cursor.com/install -fsS | bash.",
    capabilities: ["chat", "code", "edit", "shell", "live"],
    tags: ["live", "cursor", "code"],
  },
};
