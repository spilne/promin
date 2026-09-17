// ---------------------------------------------------------------------------
// exportRecipeAsTs — pure transform from RegisteredAgent to a TS
// snippet. Pinned cases:
//   1. Renders a `await agentRegistry.register(...)` call
//   2. Strips undefined fields (no `undefined` literals in output)
//   3. Stable key order (id/version/backend/metadata; backend keys
//      ordered too)
//   4. Round-trippable: JSON-parsing the bracketed body re-yields the
//      same shape the input had after stripping
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { exportRecipeAsTs } from "../export-recipe-ts.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

function makeAgent(overrides: Partial<RegisteredAgent> = {}): RegisteredAgent {
  return {
    id: "support",
    version: "v1",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      role: { inline: { systemPrompt: "Helpful", tools: ["search", "summarise"] } },
      maxStepsPerTurn: 8,
    },
    metadata: {
      description: "Support agent",
      capabilities: ["chat", "search"],
      tags: ["beta"],
      enabled: true,
    },
    createdAt: 1_000_000,
    updatedAt: 1_000_000,
    ...overrides,
  } as RegisteredAgent;
}

describe("exportRecipeAsTs", () => {
  it("emits a `await agentRegistry.register(...)` call", () => {
    const out = exportRecipeAsTs(makeAgent());
    expect(out).toContain("await agentRegistry.register(");
    expect(out.trim().endsWith(");")).toBe(true);
  });

  it("does not emit `undefined` literals (undefined fields stripped)", () => {
    const agent = makeAgent({
      backend: {
        type: "local",
        model: { provider: "anthropic", id: "claude-sonnet-4-6" },
        role: { inline: { systemPrompt: null, tools: [] } },
        // maxTurns intentionally omitted → must not appear as `"maxTurns": undefined`
      },
    });
    const out = exportRecipeAsTs(agent);
    expect(out).not.toContain("undefined");
  });

  it("orders keys stably: id, version, then backend (type, model, role { systemPrompt, tools })", () => {
    const out = exportRecipeAsTs(makeAgent());
    // Top-level: id should appear before version, version before backend, backend before metadata.
    const idIdx = out.indexOf('"id"');
    const versionIdx = out.indexOf('"version"');
    const backendIdx = out.indexOf('"backend"');
    const metadataIdx = out.indexOf('"metadata"');
    expect(idIdx).toBeLessThan(versionIdx);
    expect(versionIdx).toBeLessThan(backendIdx);
    expect(backendIdx).toBeLessThan(metadataIdx);

    // Inside backend: type before model before role; inside the inline role,
    // systemPrompt before tools.
    const typeIdx = out.indexOf('"type"');
    const modelIdx = out.indexOf('"model"');
    const roleIdx = out.indexOf('"role"');
    const sysIdx = out.indexOf('"systemPrompt"');
    const toolsIdx = out.indexOf('"tools"');
    expect(typeIdx).toBeLessThan(modelIdx);
    expect(modelIdx).toBeLessThan(roleIdx);
    expect(roleIdx).toBeLessThan(sysIdx);
    expect(sysIdx).toBeLessThan(toolsIdx);
  });

  it("strips server-assigned fields (createdAt, updatedAt)", () => {
    const out = exportRecipeAsTs(makeAgent());
    expect(out).not.toContain("createdAt");
    expect(out).not.toContain("updatedAt");
  });

  it("round-trips: extracted JSON parses back to a recipe-shaped object", () => {
    const out = exportRecipeAsTs(makeAgent());
    const start = out.indexOf("(") + 1;
    const end = out.lastIndexOf(")");
    const body = out.slice(start, end).trim();
    const parsed = JSON.parse(body);
    expect(parsed.id).toBe("support");
    expect(parsed.version).toBe("v1");
    expect(parsed.backend.type).toBe("local");
    expect(parsed.backend.role.inline.tools).toEqual(["search", "summarise"]);
    expect(parsed.metadata.tags).toEqual(["beta"]);
  });
});
