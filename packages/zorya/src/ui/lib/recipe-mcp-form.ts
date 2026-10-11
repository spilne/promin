// ---------------------------------------------------------------------------
// recipe-mcp-form — pure form-state ⇄ recipe mapping for the agent edit
// drawer's "MCP servers" section (`backend.mcpServers[]`).
//
// A row either attaches an operator-allowlisted server by name or names a
// Streamable HTTP endpoint (no credential: only allowlisted servers get
// one). `tools` is a comma-separated narrowing list; blank means every tool
// the server allows.
// ---------------------------------------------------------------------------

import type { McpServerRecipe } from "@promin/agent/browser";

export type McpServerForm =
  | { kind: "server"; server: string; tools: string }
  | { kind: "http"; name: string; url: string; tools: string };

/** Form rows for a recipe's `mcpServers`. */
export function initMcpServers(
  recipes: ReadonlyArray<McpServerRecipe> | undefined,
): McpServerForm[] {
  return (recipes ?? []).map((r) =>
    "server" in r
      ? { kind: "server", server: r.server, tools: (r.tools ?? []).join(", ") }
      : { kind: "http", name: r.name, url: r.url, tools: (r.tools ?? []).join(", ") },
  );
}

/** The recipe's `mcpServers` for the form rows; blank rows are dropped. */
export function buildMcpServers(forms: ReadonlyArray<McpServerForm>): McpServerRecipe[] {
  const out: McpServerRecipe[] = [];
  for (const f of forms) {
    const tools = splitList(f.tools);
    if (f.kind === "server") {
      if (!f.server.trim()) continue;
      out.push({ server: f.server.trim(), ...(tools.length > 0 && { tools }) });
      continue;
    }
    if (!f.name.trim() || !f.url.trim()) continue;
    out.push({
      name: f.name.trim(),
      transport: "http",
      url: f.url.trim(),
      ...(tools.length > 0 && { tools }),
    });
  }
  return out;
}

function splitList(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}
