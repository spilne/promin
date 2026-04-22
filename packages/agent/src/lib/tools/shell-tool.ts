import { z } from "zod";
import { tool } from "../tool.ts";

export interface ShellToolConfig {
  /**
   * Working directory for all commands. Strongly recommended to set this
   * to the project root to prevent accidental system-wide side effects.
   */
  cwd?: string;
  /**
   * Allowed command prefixes (e.g. ["bun", "git", "npm"]). When set,
   * commands not starting with one of these prefixes are rejected.
   * Omit to allow any command (use with caution).
   */
  allowedCommands?: string[];
  /** Timeout in milliseconds. Default: 30 000. */
  timeoutMs?: number;
  /** Max characters of output returned. Default: 20 000. */
  maxOutputChars?: number;
}

/**
 * Creates a shell execution tool that runs arbitrary commands in a subprocess.
 *
 * Always requires user approval. Optionally restrict to an allowlist of
 * command prefixes to reduce the blast radius.
 *
 * Usage:
 *   agentLoop({
 *     tools: { shell: createShellTool({ cwd: "/workspace", allowedCommands: ["bun", "git"] }) },
 *     autoApprove: "none",
 *   });
 */
export function createShellTool(config: ShellToolConfig = {}) {
  const maxOutput = config.maxOutputChars ?? 20_000;
  const timeoutMs = config.timeoutMs ?? 30_000;

  return tool({
    name: "shell",
    description:
      "Run a shell command and return stdout + stderr. " +
      (config.allowedCommands
        ? `Allowed commands: ${config.allowedCommands.join(", ")}.`
        : "Any command is allowed — use with care.") +
      (config.cwd ? ` Working directory: ${config.cwd}.` : ""),
    parameters: z.object({
      command: z.string().describe("The shell command to execute"),
    }),
    requireApproval: true,
    execute: async ({ command }) => {
      if (config.allowedCommands) {
        const allowed = config.allowedCommands.some(
          (prefix) => command === prefix || command.startsWith(`${prefix} `),
        );
        if (!allowed) {
          return `Rejected: command must start with one of: ${config.allowedCommands.join(", ")}`;
        }
      }

      const proc = Bun.spawn(["sh", "-c", command], {
        cwd: config.cwd,
        stdout: "pipe",
        stderr: "pipe",
      });

      const timer = setTimeout(() => proc.kill(), timeoutMs);
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      clearTimeout(timer);

      const combined = [stdout, stderr].filter(Boolean).join("\n");
      const output =
        combined.length > maxOutput
          ? `${combined.slice(0, maxOutput)}\n[…output truncated at ${maxOutput} chars]`
          : combined;

      return `exit ${exitCode}\n${output || "(no output)"}`;
    },
  });
}
