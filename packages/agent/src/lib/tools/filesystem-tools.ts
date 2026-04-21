import { readFile, writeFile, readdir, mkdir, stat } from "node:fs/promises";
import { join, resolve, relative } from "node:path";
import { z } from "zod";
import { tool } from "../tool.ts";

export interface FilesystemToolsConfig {
  /**
   * Root directory the agent is allowed to read and write.
   * All paths are resolved relative to this root and confined within it.
   */
  rootDir: string;
  /** Allow shell execution via createShellTool. Default: false. */
  allowShell?: boolean;
  /** Max bytes returned by readFile before truncation. Default: 100 000. */
  maxReadBytes?: number;
}

// ---- shared path guard ----

function safePath(rootDir: string, userPath: string): string {
  const abs = resolve(rootDir, userPath);
  const rel = relative(rootDir, abs);
  if (rel.startsWith("..")) {
    throw new Error(`Path "${userPath}" escapes the allowed root directory.`);
  }
  return abs;
}

// ---- individual tool factories ----

export function createReadFileTool(config: FilesystemToolsConfig) {
  const maxBytes = config.maxReadBytes ?? 100_000;

  return tool({
    name: "readFile",
    description: `Read the contents of a file. Paths are relative to ${config.rootDir}.`,
    parameters: z.object({
      path: z.string().describe("File path relative to the root directory"),
    }),
    execute: async ({ path }) => {
      const abs = safePath(config.rootDir, path);
      const buf = await readFile(abs);
      if (buf.length > maxBytes) {
        return `[truncated — file is ${buf.length} bytes, showing first ${maxBytes}]\n${buf.slice(0, maxBytes).toString("utf8")}`;
      }
      return buf.toString("utf8");
    },
  });
}

export function createWriteFileTool(config: FilesystemToolsConfig) {
  return tool({
    name: "writeFile",
    description: `Write or overwrite a file. Creates parent directories if needed. Paths are relative to ${config.rootDir}.`,
    parameters: z.object({
      path: z.string().describe("File path relative to the root directory"),
      content: z.string().describe("Full file content to write"),
    }),
    requireApproval: true,
    execute: async ({ path, content }) => {
      const abs = safePath(config.rootDir, path);
      await mkdir(abs.slice(0, abs.lastIndexOf("/")), { recursive: true });
      await writeFile(abs, content, "utf8");
      return `Wrote ${content.length} chars to ${path}`;
    },
  });
}

export function createListDirTool(config: FilesystemToolsConfig) {
  return tool({
    name: "listDir",
    description: `List files in a directory. Paths are relative to ${config.rootDir}.`,
    parameters: z.object({
      path: z.string().optional().describe("Directory path relative to the root (default: '.')"),
      recursive: z.boolean().default(false).describe("Descend into subdirectories"),
    }),
    execute: async ({ path, recursive }) => {
      const abs = safePath(config.rootDir, path ?? ".");
      const names = await readdir(abs, { recursive }) as string[];
      if (names.length === 0) return "(empty)";
      // Mark directories with a trailing slash by stat-ing each entry
      const lines = await Promise.all(
        names.map(async (name) => {
          try {
            const s = await stat(join(abs, name));
            return s.isDirectory() ? `${name}/` : name;
          } catch {
            return name;
          }
        }),
      );
      return lines.join("\n");
    },
  });
}

export function createStatTool(config: FilesystemToolsConfig) {
  return tool({
    name: "statFile",
    description: `Get metadata for a file or directory (size, type, modified time).`,
    parameters: z.object({
      path: z.string().describe("Path relative to the root directory"),
    }),
    execute: async ({ path }) => {
      const abs = safePath(config.rootDir, path);
      const s = await stat(abs);
      return JSON.stringify({
        type: s.isDirectory() ? "directory" : s.isFile() ? "file" : "other",
        size: s.size,
        modified: s.mtime.toISOString(),
      });
    },
  });
}

// ---- bundle ----

export interface FilesystemTools {
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs validated at runtime
  readFile: ReturnType<typeof createReadFileTool>;
  writeFile: ReturnType<typeof createWriteFileTool>;
  listDir: ReturnType<typeof createListDirTool>;
  statFile: ReturnType<typeof createStatTool>;
}

/**
 * Creates a set of file system tools confined to rootDir.
 *
 * All paths are validated to prevent directory traversal outside rootDir.
 * writeFile requires user approval by default.
 *
 * Usage:
 *   const fs = createFilesystemTools({ rootDir: "/workspace/my-project" });
 *   agentLoop({ tools: { ...fs }, ... });
 */
export function createFilesystemTools(config: FilesystemToolsConfig): FilesystemTools {
  return {
    readFile: createReadFileTool(config),
    writeFile: createWriteFileTool(config),
    listDir: createListDirTool(config),
    statFile: createStatTool(config),
  };
}
