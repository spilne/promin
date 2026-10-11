import { isAbsolute } from "node:path";
import { z } from "zod";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { tool } from "../tool.ts";

export interface ShellToolConfig {
  /**
   * Executables the model may run, by bare name looked up on the child's
   * `PATH` (`"git"`, `"bun"`) or by absolute path. Required and matched
   * exactly against `command` — there is no "run anything" mode.
   *
   * The allowlist bounds which program runs, not what it does: `find -exec`,
   * `git -c core.pager=…` or `npm run` can still start other programs, so
   * keep approval on and list only what the agent needs.
   */
  allowedCommands: readonly string[];
  /** Working directory for every command. Default: `process.cwd()`. */
  cwd?: string;
  /**
   * Names of variables copied from the host environment into the child.
   * Default: `DEFAULT_INHERITED_ENV` (PATH, HOME, locale, temp dir).
   * Names that look like credentials (`*KEY*`, `*TOKEN*`, `*SECRET*`,
   * `*PASSWORD*`, `*CREDENTIAL*`, `*AUTH*`) are refused at construction:
   * pass a value the child really needs through `env` instead.
   */
  inheritEnv?: readonly string[];
  /** Extra variables set in the child, after `inheritEnv`. */
  env?: Readonly<Record<string, string>>;
  /** Kill the whole process group after this many ms. Default: 30 000. */
  timeoutMs?: number;
  /** Bytes of stdout + stderr kept; past it the process group is killed. Default: 20 000. */
  maxOutputBytes?: number;
  /** Ask a human before each command. Default: `true`. */
  requireApproval?: boolean;
  /** Time source for the timeout. Default: `SystemWallClock`. */
  clock?: WallClock;
}

/** Host variables a command inherits unless `inheritEnv` says otherwise. */
export const DEFAULT_INHERITED_ENV: readonly string[] = [
  "PATH",
  "HOME",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "TERM",
  "TMPDIR",
  "TZ",
];

const CREDENTIAL_NAME = /KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH|COOKIE|SESSION/i;

/**
 * Creates a tool that runs one allowlisted executable with an argv array —
 * no shell, so `;`, `|`, `$(…)` and globs in arguments are plain text.
 *
 * The child gets a scrubbed environment (only `inheritEnv` + `env`), runs
 * in its own process group, and the whole group is killed on timeout or
 * when output passes `maxOutputBytes`. Approval is required by default.
 *
 * Usage:
 *   agentLoop({
 *     tools: { shell: createShellTool({ cwd: "/workspace", allowedCommands: ["bun", "git"] }) },
 *   });
 */
export function createShellTool(config: ShellToolConfig) {
  if (config.allowedCommands.length === 0) {
    throw new Error("createShellTool: allowedCommands must list at least one executable.");
  }
  for (const name of config.allowedCommands) {
    if (name.includes("/") && !isAbsolute(name)) {
      throw new Error(
        `createShellTool: allowed command "${name}" must be a bare name or an absolute path.`,
      );
    }
  }
  const inherit = config.inheritEnv ?? DEFAULT_INHERITED_ENV;
  const leaked = inherit.filter((name) => CREDENTIAL_NAME.test(name));
  if (leaked.length > 0) {
    throw new Error(
      `createShellTool: inheritEnv must not pass credentials to the child (${leaked.join(", ")}).`,
    );
  }
  const maxOutput = config.maxOutputBytes ?? 20_000;
  const timeoutMs = config.timeoutMs ?? 30_000;
  const clock = config.clock ?? SystemWallClock;
  const allowed = new Set(config.allowedCommands);

  return tool({
    name: "shell",
    description:
      "Run one program with arguments (no shell: pipes, redirects, `;` and `$()` are not interpreted) " +
      `and return its exit code, stdout and stderr. Allowed programs: ${config.allowedCommands.join(", ")}.` +
      (config.cwd ? ` Working directory: ${config.cwd}.` : ""),
    parameters: z.object({
      command: z.string().min(1).describe("The program to run, e.g. 'git'"),
      args: z
        .array(z.string())
        .default([])
        .describe("Arguments, one per element, e.g. ['log', '--oneline', '-5']"),
    }),
    requireApproval: config.requireApproval ?? true,
    execute: async ({ command, args }) => {
      if (!allowed.has(command)) {
        return `Rejected: "${command}" is not an allowed program. Allowed: ${config.allowedCommands.join(", ")}.`;
      }
      const env = childEnv({ inherit, extra: config.env });
      const executable = isAbsolute(command)
        ? command
        : Bun.which(command, { PATH: env.PATH ?? "" });
      if (!executable) return `Rejected: "${command}" was not found on PATH.`;
      return runCommand({
        argv: [executable, ...args],
        cwd: config.cwd,
        env,
        timeoutMs,
        maxOutput,
        clock,
      });
    },
  });
}

function childEnv(params: {
  inherit: readonly string[];
  extra: Readonly<Record<string, string>> | undefined;
}): Record<string, string> {
  const env: Record<string, string> = {};
  for (const name of params.inherit) {
    const value = process.env[name];
    if (value !== undefined) env[name] = value;
  }
  return { ...env, ...params.extra };
}

async function runCommand(params: {
  argv: string[];
  cwd: string | undefined;
  env: Record<string, string>;
  timeoutMs: number;
  maxOutput: number;
  clock: WallClock;
}): Promise<string> {
  const { timeoutMs, maxOutput, clock } = params;
  const proc = Bun.spawn(params.argv, {
    ...(params.cwd !== undefined && { cwd: params.cwd }),
    env: params.env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    // Own session + process group, so a kill reaches everything it started.
    detached: true,
  });

  let stopReason: "timeout" | "output" | undefined;
  const killGroup = (reason: "timeout" | "output") => {
    if (stopReason) return;
    stopReason = reason;
    try {
      process.kill(-proc.pid, "SIGKILL");
    } catch {
      // Group already gone; fall back to the leader alone.
      proc.kill("SIGKILL");
    }
  };
  const timer = clock.setTimeout(() => killGroup("timeout"), timeoutMs);
  // Background children the program left behind would hold the pipes open;
  // once the program itself exits, the rest of its group goes too.
  void proc.exited.then(() => {
    try {
      process.kill(-proc.pid, "SIGKILL");
    } catch {
      // No group members left.
    }
  });

  let kept = 0;
  const collect = async (stream: ReadableStream<Uint8Array>): Promise<string> => {
    const decoder = new TextDecoder();
    let text = "";
    for await (const chunk of stream) {
      const room = maxOutput - kept;
      if (room <= 0) continue;
      const part = chunk.byteLength > room ? chunk.subarray(0, room) : chunk;
      kept += part.byteLength;
      text += decoder.decode(part, { stream: true });
      if (chunk.byteLength > room) killGroup("output");
    }
    return text + decoder.decode();
  };

  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      collect(proc.stdout),
      collect(proc.stderr),
      proc.exited,
    ]);
    const combined = [stdout, stderr].filter(Boolean).join("\n");
    if (stopReason === "timeout") {
      return `timed out after ${timeoutMs} ms; process group killed\n${combined || "(no output)"}`;
    }
    if (stopReason === "output") {
      return `output exceeded ${maxOutput} bytes; process group killed\n${combined}\n[…output truncated]`;
    }
    return `exit ${exitCode}\n${combined || "(no output)"}`;
  } finally {
    timer.clear();
  }
}
