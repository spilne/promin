import { z } from "zod";
import { tool } from "../tool.ts";
import { multiTool, command } from "../multi-tool.ts";
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

function fmtAge(d: Date): string {
  const sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

/**
 * A single multiTool that bundles memory search, list, save, update, and delete
 * into one tool call so agents need only one tool slot for memory management.
 */
export function createMemoryTool(store: MemoryStore) {
  return multiTool({
    name: "memory",
    description: "Read and write long-term memory that persists across sessions.",
    commands: {
      search: command({
        description: "Search for relevant memories by natural-language query",
        parameters: z.object({
          query: z.string().describe("Natural-language search query"),
          limit: z.number().int().min(1).max(20).default(5).describe("Max entries to return"),
        }),
        execute: async ({ query, limit }) => {
          const [entries, all] = await Promise.all([
            store.search(query, limit),
            store.list(undefined),
          ]);
          if (entries.length === 0) return "No memories found matching that query.";
          const header =
            all.length > entries.length
              ? `Showing ${entries.length} of ${all.length} entries:\n`
              : "";
          return (
            header +
            entries
              .map(
                (e, i) => `${i + 1}. [${e.id.slice(0, 8)}] (${fmtAge(e.createdAt)}) ${e.content}`,
              )
              .join("\n")
          );
        },
      }),
      list: command({
        description: "Browse recent memories without a search query",
        parameters: z.object({
          limit: z.number().int().min(1).max(50).default(10).describe("Max entries to return"),
        }),
        execute: async ({ limit }) => {
          const [entries, all] = await Promise.all([store.list(limit), store.list(undefined)]);
          if (entries.length === 0) return "No memories stored yet.";
          const header =
            all.length > entries.length
              ? `Showing ${entries.length} of ${all.length} entries:\n`
              : "";
          return (
            header +
            entries
              .map(
                (e, i) => `${i + 1}. [${e.id.slice(0, 8)}] (${fmtAge(e.createdAt)}) ${e.content}`,
              )
              .join("\n")
          );
        },
      }),
      save: command({
        description: "Persist a fact or insight so it can be recalled in future sessions",
        parameters: z.object({
          content: z.string().min(1).describe("The fact or insight to remember"),
        }),
        execute: async ({ content }) => {
          const id = await store.save({ content });
          return `Saved to memory (id: ${id.slice(0, 8)}).`;
        },
      }),
      update: command({
        description: "Correct or replace the content of an existing memory entry",
        parameters: z.object({
          id: z.string().describe("First 8+ characters of the memory id"),
          content: z.string().min(1).describe("Replacement content"),
        }),
        execute: async ({ id, content }) => {
          const all = await store.list();
          const entry = all.find((e) => e.id.startsWith(id));
          if (!entry) return `No memory found with id starting "${id}".`;
          await store.update(entry.id, { content });
          return `Updated memory ${entry.id.slice(0, 8)}.`;
        },
      }),
      delete: command({
        description: "Remove a memory entry permanently",
        parameters: z.object({
          id: z.string().describe("First 8+ characters of the memory id"),
        }),
        execute: async ({ id }) => {
          const all = await store.list();
          const entry = all.find((e) => e.id.startsWith(id));
          if (!entry) return `No memory found with id starting "${id}".`;
          await store.delete(entry.id);
          return `Deleted memory ${entry.id.slice(0, 8)}.`;
        },
      }),
    },
  });
}
