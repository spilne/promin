// ---------------------------------------------------------------------------
// system-tools — which auto-injected ("system") tools a recipe surfaces in
// the agent UI. Pins the loadSkill (skill-count), knowledge search-tool,
// network (findAgent / callAgent) derivation, and the empty case.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { systemToolsFor } from "../system-tools.tsx";

describe("systemToolsFor", () => {
  it("returns nothing when there are no skills and no network", () => {
    expect(systemToolsFor({ skillCount: 0, hasNetwork: false })).toEqual([]);
  });

  it("surfaces loadSkill when the agent has skills, with a count-aware reason", () => {
    const tools = systemToolsFor({ skillCount: 2, hasNetwork: false });
    expect(tools.map((t) => t.name)).toEqual(["loadSkill"]);
    expect(tools[0]!.reason).toContain("2 skills");
  });

  it("singularizes the loadSkill reason for one skill", () => {
    const tools = systemToolsFor({ skillCount: 1, hasNetwork: false });
    expect(tools[0]!.reason).toContain("1 skill");
    expect(tools[0]!.reason).not.toContain("1 skills");
  });

  it("surfaces findAgent + callAgent when network is enabled", () => {
    const tools = systemToolsFor({ skillCount: 0, hasNetwork: true });
    expect(tools.map((t) => t.name)).toEqual(["findAgent", "callAgent"]);
  });

  it("surfaces one search tool per knowledge binding exposed as a tool", () => {
    const tools = systemToolsFor({
      skillCount: 0,
      knowledge: [
        { id: "org-handbook" },
        { id: "default", name: "search_default_kb", mode: "tool-and-context" },
        { id: "ambient", mode: "context" },
        { id: "  " },
      ],
      hasNetwork: false,
    });
    expect(tools.map((t) => t.name)).toEqual(["search_org_handbook", "search_default_kb"]);
    expect(tools[0]!.reason).toContain('knowledge binding "org-handbook"');
  });

  it("combines skills + network", () => {
    const tools = systemToolsFor({ skillCount: 3, hasNetwork: true });
    expect(tools.map((t) => t.name)).toEqual(["loadSkill", "findAgent", "callAgent"]);
  });
});
