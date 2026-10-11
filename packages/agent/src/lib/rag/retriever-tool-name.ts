/** Tool name a retriever bound under `id` gets when the binding sets no `name`. */
export const DEFAULT_RETRIEVER_TOOL_NAME = "search_knowledge_base";

/**
 * Derive the LLM-facing tool name for a retriever binding id: the id
 * `default` maps to `search_knowledge_base`, anything else to
 * `search_<id>` with non-identifier characters collapsed to `_`.
 *
 * Pure (no Node built-ins) so the zorya dashboard can preview the same
 * names the runtime attaches.
 */
export function retrieverToolName(id: string): string {
  if (id === "default") return DEFAULT_RETRIEVER_TOOL_NAME;
  const slug = id.replace(/[^A-Za-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  return `search_${slug || "knowledge"}`;
}
