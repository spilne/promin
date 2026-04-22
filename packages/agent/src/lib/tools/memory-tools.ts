import { z } from "zod";
import { tool } from "../tool.ts";
import type { MemoryStore, MemoryScope } from "../memory-store.ts";

export interface MemoryToolConfig {
  store: MemoryStore;
  /** Scope applied to all read and write operations. */
  scope?: MemoryScope;
}

/**
 * Gives the LLM the ability to search its long-term memory store.
 * Results are returned as a numbered list so the model can cite entries by index.
 */
export function createSearchMemoryTool(config: MemoryToolConfig) {
  return tool({
    name: "searchMemory",
    description:
      "Search long-term memory for entries relevant to a query. " +
      "Returns the top matching entries. Use this to recall facts from previous sessions.",
    parameters: z.object({
      query: z.string().describe("Natural-language search query"),
      limit: z.number().int().min(1).max(20).default(5).describe("Max entries to return"),
    }),
    execute: async ({ query, limit }) => {
      const entries = await config.store.search(query, limit, config.scope);
      if (entries.length === 0) return "No memories found matching that query.";
      return entries.map((e, i) => `${i + 1}. [${e.id.slice(0, 8)}] ${e.content}`).join("\n");
    },
  });
}

/**
 * Gives the LLM the ability to save a fact to long-term memory.
 * The LLM supplies the content; the host supplies the store and scope.
 */
export function createSaveMemoryTool(config: MemoryToolConfig) {
  return tool({
    name: "saveMemory",
    description:
      "Save a fact or insight to long-term memory so it can be recalled in future sessions. " +
      "Use this for information that should persist beyond this conversation.",
    parameters: z.object({
      content: z.string().min(1).describe("The fact or insight to remember"),
    }),
    execute: async ({ content }) => {
      const id = await config.store.save({ content }, config.scope);
      return `Saved to memory (id: ${id.slice(0, 8)}).`;
    },
  });
}

/**
 * Convenience bundle — returns both tools for spreading into an agent's tool map.
 *
 * Usage:
 *   const memory = new InMemoryMemoryStore();
 *   agentLoop({
 *     tools: { ...createMemoryTools({ store: memory }) },
 *     memory: { store: memory },
 *   });
 */
export function createMemoryTools(config: MemoryToolConfig) {
  return {
    searchMemory: createSearchMemoryTool(config),
    saveMemory: createSaveMemoryTool(config),
  };
}
