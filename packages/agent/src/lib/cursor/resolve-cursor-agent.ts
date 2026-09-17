// ---------------------------------------------------------------------------
// `resolveCursorAgent` — turn a `RegisteredAgent` recipe with a `cursor`
// backend into a live `CursorAgent`.
//
// The registry only stores JSON-serializable config (binary name, model,
// workspace, env-var names…). This factory verifies required env vars
// are set and constructs a `CursorAgent` with the merged config — same
// pattern as `resolveLocalAgent` / `resolveRemoteAgent`.
// ---------------------------------------------------------------------------

import { CursorAgent } from "./cursor-agent.ts";
import type { CursorAgentConfig } from "./cursor-agent.ts";
import type { CursorTransport } from "./session.ts";
import type { RegisteredAgent } from "../registry/types.ts";

const DEFAULT_REQUIRED_ENV: ReadonlyArray<string> = ["CURSOR_API_KEY"];

export interface ResolveCursorAgentDeps {
  /**
   * Test seam — inject a fake `CursorTransport`. Production code leaves
   * this unset to use the real `Bun.spawn` transport.
   */
  readonly transport?: CursorTransport;
  /** Override `process.env` lookup (tests). */
  readonly env?: Readonly<Record<string, string | undefined>>;
}

export function resolveCursorAgent(
  agent: RegisteredAgent,
  deps: ResolveCursorAgentDeps = {},
): CursorAgent {
  if (agent.backend.type !== "cursor") {
    throw new Error(
      `resolveCursorAgent: expected backend.type "cursor", got "${agent.backend.type}" (agent: ${agent.id})`,
    );
  }
  const b = agent.backend;
  const env = deps.env ?? (process.env as Record<string, string | undefined>);
  const required = b.requiredEnv ?? DEFAULT_REQUIRED_ENV;
  const missing = required.filter((name) => !env[name]);
  if (missing.length > 0) {
    throw new Error(
      `resolveCursorAgent: agent "${agent.id}" requires env var(s) ${missing.join(", ")} which are not set.`,
    );
  }

  const cfg: CursorAgentConfig = {
    ...(b.command !== undefined && { command: b.command }),
    ...(b.model !== undefined && { model: b.model }),
    ...(b.workspace !== undefined && { workspace: b.workspace }),
    ...(b.worktree !== undefined && { worktree: b.worktree }),
    ...(b.trust !== undefined && { trust: b.trust }),
    ...(b.sandbox !== undefined && { sandbox: b.sandbox }),
    ...(b.extraArgs !== undefined && { extraArgs: b.extraArgs }),
    ...(deps.transport !== undefined && { transport: deps.transport }),
  };
  return new CursorAgent(cfg);
}
