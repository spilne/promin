import { constants } from "node:fs";
import { lstat, mkdir, open, readdir, realpath, stat } from "node:fs/promises";
import { basename, dirname, join, resolve, sep } from "node:path";
import { z } from "zod";
import { tool } from "../tool.ts";

export interface FilesystemToolsConfig {
  /**
   * Root directory the agent is allowed to read and write.
   * All paths are resolved relative to this root and confined within it.
   */
  rootDir: string;
  /** Max bytes returned by readFile before truncation. Default: 100 000. */
  maxReadBytes?: number;
}

// ---- shared path guard ----

/**
 * Resolve `userPath` against `rootDir` and return its real location, or
 * throw when that location is outside the root.
 *
 * Symlinks are resolved before the check, so a link inside the root that
 * points outside it is refused. For a path that doesn't exist yet the
 * nearest existing ancestor is resolved and the missing tail appended, so a
 * write through a symlinked directory is caught too. A dangling symlink on
 * the way is refused outright: writing through it would create its
 * unchecked target.
 */
export async function safePath(params: { rootDir: string; userPath: string }): Promise<string> {
  const root = await realpath(params.rootDir);
  const missing: string[] = [];
  let existing = resolve(root, params.userPath);
  let real: string;
  for (;;) {
    try {
      real = await realpath(existing);
      break;
    } catch (err) {
      if (!isMissing(err)) throw err;
      const present = await lstat(existing).then(
        () => true,
        () => false,
      );
      if (present) throw new Error(`Path "${params.userPath}" goes through a dangling symlink.`);
      const parent = dirname(existing);
      if (parent === existing) throw err;
      missing.unshift(basename(existing));
      existing = parent;
    }
  }
  const abs = missing.length > 0 ? join(real, ...missing) : real;
  const prefix = root.endsWith(sep) ? root : root + sep;
  if (abs !== root && !abs.startsWith(prefix)) {
    throw new Error(`Path "${params.userPath}" escapes the allowed root directory.`);
  }
  return abs;
}

function isMissing(err: unknown): boolean {
  const code = (err as NodeJS.ErrnoException | undefined)?.code;
  return code === "ENOENT" || code === "ENOTDIR";
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
      const abs = await safePath({ rootDir: config.rootDir, userPath: path });
      // O_NOFOLLOW: a symlink swapped in after the check is not followed.
      const handle = await open(abs, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const { size } = await handle.stat();
        const buf = Buffer.alloc(Math.min(size, maxBytes));
        const { bytesRead } = await handle.read({ buffer: buf, position: 0 });
        const text = buf.subarray(0, bytesRead).toString("utf8");
        if (size > maxBytes) {
          return `[truncated — file is ${size} bytes, showing first ${maxBytes}]\n${text}`;
        }
        return text;
      } finally {
        await handle.close();
      }
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
      const abs = await safePath({ rootDir: config.rootDir, userPath: path });
      await mkdir(dirname(abs), { recursive: true });
      // O_NOFOLLOW: a symlink swapped in after the check is not followed.
      const handle = await open(
        abs,
        constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC | constants.O_NOFOLLOW,
        0o644,
      );
      try {
        await handle.writeFile(content, "utf8");
      } finally {
        await handle.close();
      }
      return `Wrote ${content.length} chars to ${path}`;
    },
  });
}

// Directories that are nearly always enormous and never useful to list.
const IGNORED_DIR_SEGMENTS = new Set([
  "node_modules",
  ".git",
  "dist",
  "build",
  ".turbo",
  ".nx",
  "coverage",
  ".cache",
  ".vite",
  "__pycache__",
  ".pytest_cache",
  "vendor",
  "target",
  ".gradle",
]);

const MAX_LIST_LINES = 2_000;

export function createListDirTool(config: FilesystemToolsConfig) {
  return tool({
    name: "listDir",
    description: [
      `List files in a directory. Paths are relative to ${config.rootDir}.`,
      `Ignores: ${[...IGNORED_DIR_SEGMENTS].join(", ")}.`,
      "Non-recursive by default — prefer that unless you need a deep tree.",
      `Results are capped at ${MAX_LIST_LINES} entries.`,
    ].join(" "),
    parameters: z.object({
      path: z.string().optional().describe("Directory path relative to the root (default: '.')"),
      recursive: z.boolean().default(false).describe("Descend into subdirectories"),
    }),
    execute: async ({ path, recursive }) => {
      const abs = await safePath({ rootDir: config.rootDir, userPath: path ?? "." });
      const lines: string[] = [];
      let omitted = 0;
      // Walk by hand rather than `readdir({ recursive })`, which follows
      // symlinked directories out of the root. A symlink is listed as
      // itself and never descended into.
      const walk = async (dir: string, prefix: string): Promise<void> => {
        const entries = await readdir(dir, { withFileTypes: true });
        entries.sort((a, b) => a.name.localeCompare(b.name));
        for (const entry of entries) {
          if (IGNORED_DIR_SEGMENTS.has(entry.name)) continue;
          const name = prefix + entry.name;
          if (lines.length < MAX_LIST_LINES) lines.push(entry.isDirectory() ? `${name}/` : name);
          else omitted += 1;
          if (recursive && entry.isDirectory()) await walk(join(dir, entry.name), `${name}/`);
        }
      };
      await walk(abs, "");
      if (lines.length === 0) return "(empty)";

      const suffix =
        omitted > 0
          ? `\n\n[…${omitted} more entries omitted — narrow your path or use a more specific query]`
          : "";
      return lines.join("\n") + suffix;
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
      const abs = await safePath({ rootDir: config.rootDir, userPath: path });
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
  readFile: ReturnType<typeof createReadFileTool>;
  writeFile: ReturnType<typeof createWriteFileTool>;
  listDir: ReturnType<typeof createListDirTool>;
  statFile: ReturnType<typeof createStatTool>;
}

/**
 * Creates a set of file system tools confined to rootDir.
 *
 * Every path is resolved through symlinks and checked against the real
 * rootDir (see `safePath`). writeFile requires user approval.
 *
 * Usage:
 *   const fs = createFilesystemTools({ rootDir: "/workspace/my-project" });
 *   agentAction({ tools: { ...fs }, ... });
 */
export function createFilesystemTools(config: FilesystemToolsConfig): FilesystemTools {
  return {
    readFile: createReadFileTool(config),
    writeFile: createWriteFileTool(config),
    listDir: createListDirTool(config),
    statFile: createStatTool(config),
  };
}
