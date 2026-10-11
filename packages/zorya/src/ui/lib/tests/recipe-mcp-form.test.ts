// ---------------------------------------------------------------------------
// recipe-mcp-form — form-state ⇄ recipe round-trips for the agent editor's
// MCP servers section.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { McpServerRecipe } from "@promin/agent/browser";
import { buildMcpServers, initMcpServers } from "../recipe-mcp-form.ts";

describe("recipe-mcp-form", () => {
  it("round-trips allowlisted servers and http endpoints", () => {
    const recipes: McpServerRecipe[] = [
      { server: "github", tools: ["search", "read"] },
      { server: "docs" },
      {
        name: "wiki",
        transport: "http",
        url: "https://wiki.example.com/mcp",
      },
    ];
    expect(buildMcpServers(initMcpServers(recipes))).toEqual(recipes);
  });

  it("drops blank rows and trims fields", () => {
    expect(
      buildMcpServers([
        { kind: "server", server: "  ", tools: "" },
        { kind: "http", name: "wiki", url: "", tools: "" },
        { kind: "server", server: " github ", tools: " search , , read " },
      ]),
    ).toEqual([{ server: "github", tools: ["search", "read"] }]);
  });
});
