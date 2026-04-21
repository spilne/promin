import { watch } from "node:fs";
import { readdir, access } from "node:fs/promises";
import { join, extname } from "node:path";
import { pathToFileURL } from "node:url";
import type { AgentTool } from "./tool.ts";
import type { LLMToolDefinition } from "./llm-provider.ts";
import { zodToJsonSchema } from "./zod-to-json-schema.ts";

export interface ToolRegistry {
  /** Returns a snapshot of the currently loaded tools, keyed by tool name. */
  getTools(): Record<string, AgentTool<any, any>>;
  close(): void;
}

export interface FileToolRegistryConfig {
  /** Directory to scan and watch for tool files. */
  dir: string;
  /** Set to false to disable file watching after initial load. Default: true. */
  watch?: boolean;
  onLoad?: (name: string) => void;
  onUnload?: (name: string) => void;
  onError?: (file: string, error: unknown) => void;
}

const TOOL_EXTENSIONS = new Set([".ts", ".js"]);

/**
 * Builds LLMToolDefinition[] from a tool map, enriching the description
 * with usage guidance and examples when present.
 */
// biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
export function buildToolDefs(tools: Record<string, AgentTool<any, any>>): LLMToolDefinition[] {
  return Object.entries(tools).map(([key, t]) => ({
    name: key,
    description: enrichDescription(t),
    parameters: zodToJsonSchema(t.parameters),
  }));
}

// biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
function enrichDescription(t: AgentTool<any, any>): string {
  let desc = t.description;
  if (t.usage) desc += `\n\nUsage: ${t.usage}`;
  if (t.examples?.length) {
    desc += "\n\nExamples:";
    for (const ex of t.examples) {
      desc += `\n- Input: ${JSON.stringify(ex.input)} → ${ex.output}`;
    }
  }
  return desc;
}

/**
 * Creates a ToolRegistry backed by a directory of tool files.
 *
 * Each file must export a default AgentTool. The tool's `name` field becomes
 * the registry key. On file changes the registry updates automatically and the
 * new tools are available on the next LLM call (next think step / next turn).
 *
 * Example tool file (tools/calculator.ts):
 *   import { tool } from "@promin/agent";
 *   import { z } from "zod";
 *   export default tool({
 *     name: "calculator",
 *     description: "...",
 *     parameters: z.object({ expression: z.string() }),
 *     execute: async ({ expression }) => String(eval(expression)),
 *   });
 */
export async function createFileToolRegistry(
  config: FileToolRegistryConfig,
): Promise<ToolRegistry> {
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  const tools = new Map<string, AgentTool<any, any>>();
  const fileToName = new Map<string, string>();
  let watcher: ReturnType<typeof watch> | null = null;

  async function loadFile(filePath: string): Promise<void> {
    try {
      const url = `${pathToFileURL(filePath).href}?t=${Date.now()}`;
      const mod = await import(url);
      // biome-ignore lint/suspicious/noExplicitAny: validated below
      const t = mod.default as AgentTool<any, any>;
      if (!t?.name || typeof t.execute !== "function") {
        config.onError?.(
          filePath,
          new Error("Tool file must export a default AgentTool with name and execute"),
        );
        return;
      }
      const prev = fileToName.get(filePath);
      if (prev && prev !== t.name) tools.delete(prev);
      tools.set(t.name, t);
      fileToName.set(filePath, t.name);
      config.onLoad?.(t.name);
    } catch (err) {
      config.onError?.(filePath, err);
    }
  }

  function unloadFile(filePath: string): void {
    const name = fileToName.get(filePath);
    if (!name) return;
    tools.delete(name);
    fileToName.delete(filePath);
    config.onUnload?.(name);
  }

  // Initial load
  const entries = await readdir(config.dir);
  await Promise.all(
    entries
      .filter((f) => TOOL_EXTENSIONS.has(extname(f)))
      .map((f) => loadFile(join(config.dir, f))),
  );

  // File watch
  if (config.watch !== false) {
    watcher = watch(config.dir, (_, filename) => {
      if (!filename || !TOOL_EXTENSIONS.has(extname(filename))) return;
      const filePath = join(config.dir, filename);
      access(filePath)
        .then(() => loadFile(filePath))
        .catch(() => unloadFile(filePath));
    });
  }

  return {
    getTools(): Record<string, AgentTool<any, any>> {
      return Object.fromEntries(tools);
    },
    close() {
      watcher?.close();
      tools.clear();
      fileToName.clear();
    },
  };
}
