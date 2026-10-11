// ---------------------------------------------------------------------------
// Agent-facing names for MCP tools.
//
// Providers accept tool names matching `^[a-zA-Z0-9_-]{1,64}$` (Anthropic,
// OpenAI); MCP tool and server names may hold dots, slashes, spaces or run
// longer. `mcpToolName` maps `(server, tool)` to `<server>__<tool>`, and
// when that is not already a valid name it replaces the illegal characters,
// truncates, and appends a hash of the original pair — so two tools that
// sanitize to the same text still get different names, and the result is
// the same on every process.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";

/** Names every provider accepts. */
export const PROVIDER_TOOL_NAME = /^[a-zA-Z0-9_-]{1,64}$/;

const MAX_LENGTH = 64;
const SEPARATOR = "__";
const HASH_LENGTH = 8;

/** The provider-safe agent name of MCP tool `tool` on server `server`. */
export function mcpToolName(params: { readonly server: string; readonly tool: string }): string {
  const plain = `${params.server}${SEPARATOR}${params.tool}`;
  if (PROVIDER_TOOL_NAME.test(plain)) return plain;
  const hash = createHash("sha256")
    .update(`${params.server}\u0000${params.tool}`)
    .digest("hex")
    .slice(0, HASH_LENGTH);
  const sanitized = plain.replace(/[^a-zA-Z0-9_-]/g, "_");
  const room = MAX_LENGTH - HASH_LENGTH - 1;
  return `${sanitized.slice(0, room)}_${hash}`;
}
