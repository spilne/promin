// ---------------------------------------------------------------------------
// `@promin/agent/browser` — the pure helpers and types a browser bundle (the
// zorya dashboard, a custom UI) can use. Nothing reachable from here loads a
// Node or Bun built-in; `src/tests/entry-points.test.ts` enforces that.
// ---------------------------------------------------------------------------

export { inlineRoleDefinition } from "./lib/role/resolve-role.ts";
export { DEFAULT_ROLE_VERSION, DEFAULT_ROLE_METADATA } from "./lib/role/types.ts";
export type {
  RoleBinding,
  RoleDefinition,
  RoleMetadata,
  RegisteredRole,
} from "./lib/role/types.ts";
export {
  MAX_FACT_CHARS,
  MAX_STATIC_RULES_CHARS,
  MAX_WORKING_MEMORY_CHARS,
} from "./lib/memory/types.ts";
export type { FactSource } from "./lib/memory/types.ts";
export { DEFAULT_RETRIEVER_TOOL_NAME, retrieverToolName } from "./lib/rag/retriever-tool-name.ts";
export type { AgentKnowledgeMode, AgentKnowledgeRecipe } from "./lib/registry/types.ts";
