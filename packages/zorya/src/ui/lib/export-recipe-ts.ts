// ---------------------------------------------------------------------------
// Pure client-side transform: turn a `RegisteredAgent` recipe into a TS
// snippet that reproduces it via `agentRegistry.register(...)`.
//
// Round-trips recipes from the Designer into VCS-tracked deployment
// code. Generated snippet is opinionated: drops server-assigned fields
// (createdAt / updatedAt), prefers a stable key order, and serialises
// with `JSON.stringify(obj, null, 2)` so the diff against a hand-edited
// version stays readable.
//
// Imports stay loose (no `@promin/agent` import in the generated
// snippet — operators paste it into wherever they already import the
// registry from) and the snippet is a single `await` call so it drops
// into any async setup function unchanged.
// ---------------------------------------------------------------------------

import type { RegisteredAgent } from "../../server/routes/agents.ts";

export function exportRecipeAsTs(agent: RegisteredAgent): string {
  // Strip server-assigned + non-stable fields. Keep the recipe shape
  // operators care about: id, version, backend, metadata.
  const recipe = {
    id: agent.id,
    version: agent.version,
    backend: stripUndefined(agent.backend),
    metadata: stripUndefined(agent.metadata),
  };
  const body = JSON.stringify(recipe, stableReplacer, 2);
  return `// Generated from ${agent.id}@${agent.version}.
// Paste into your registry-bootstrap module. Adjust imports as needed.
await agentRegistry.register(${body});\n`;
}

// JSON.stringify replacer that orders keys deterministically per scope
// so the diff against a hand-edited version is small. Stays a pure
// transform — no host context needed.
function stableReplacer(_key: string, value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    const ordered: Record<string, unknown> = {};
    for (const k of Object.keys(obj).sort(orderedKeyCompare)) {
      ordered[k] = obj[k];
    }
    return ordered;
  }
  return value;
}

// Field priority for readability: identity → backend type → model →
// systemPrompt → tools → metadata-ish, then alpha for the long tail.
const KEY_PRIORITY: Readonly<Record<string, number>> = {
  id: 0,
  version: 1,
  type: 2,
  provider: 3,
  model: 4,
  systemPrompt: 5,
  tools: 6,
  description: 7,
  capabilities: 8,
  tags: 9,
};

function orderedKeyCompare(a: string, b: string): number {
  const pa = KEY_PRIORITY[a] ?? 100;
  const pb = KEY_PRIORITY[b] ?? 100;
  if (pa !== pb) return pa - pb;
  return a.localeCompare(b);
}

function stripUndefined<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((v) => stripUndefined(v)) as unknown as T;
  }
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      if (v === undefined) continue;
      out[k] = stripUndefined(v);
    }
    return out as T;
  }
  return value;
}
