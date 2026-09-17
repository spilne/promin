// ---------------------------------------------------------------------------
// `createCursorCodingTool` — agent-callable tool that delegates a coding
// task to Cursor's CLI agent (https://cursor.com/docs/cli).
//
// Use case: an agent loop wants to hand off "implement this module" /
// "refactor this directory" to Cursor while keeping the surrounding
// tool-driven flow. The tool spawns one Cursor session per call,
// streams nothing back to the model (the `execute` return value is the
// final result), and surfaces token / tool-call info structurally.
//
// vs `CursorAgent`
// ----------------
// The agent surface (`CursorAgent` in `../cursor/cursor-agent.ts`) routes
// an entire conversation to Cursor — the agent loop IS Cursor. This tool
// surface is for delegation from a host agent that's still running its
// own loop. Both share `runCursorSession` so a future change (new flag,
// new frame type) lands in one place.
// ---------------------------------------------------------------------------

import { z } from "zod";
import { tool } from "../tool.ts";
import type { AgentTool } from "../tool.ts";
import { runCursorSession } from "../cursor/session.ts";
import type { CursorTransport } from "../cursor/session.ts";

export interface CursorCodingToolDeps {
  /** Override the binary name. Default: `"agent"`. */
  readonly command?: string;
  /** Default model when the model arg isn't supplied per-call. */
  readonly defaultModel?: string;
  /** Default workspace when the call doesn't pass one. */
  readonly defaultWorkspace?: string;
  /** Pass `--trust`. Default: true (Cursor's first-run prompt is interactive otherwise). */
  readonly trust?: boolean;
  /** `--sandbox enabled|disabled`. Default: omitted. */
  readonly sandbox?: "enabled" | "disabled";
  /** Extra raw args appended to every spawn. */
  readonly extraArgs?: ReadonlyArray<string>;
  /** Env vars merged over `process.env`. `CURSOR_API_KEY` lives here. */
  readonly env?: Readonly<Record<string, string | undefined>>;
  /** Test seam — let tests inject a fake transport. */
  readonly transport?: CursorTransport;
}

const SCHEMA = z.object({
  prompt: z
    .string()
    .min(1)
    .describe(
      "Coding task to delegate to Cursor. Phrase as you would a fresh user request — Cursor opens a new session per call. Example: 'Implement a JSON-RPC server in src/rpc/server.ts that handles `ping` and `echo` methods.'",
    ),
  model: z
    .string()
    .optional()
    .describe(
      "Model identifier passed to Cursor as `--model`. Falls back to the tool's defaultModel. Cursor's accepted set is dynamic; query `--list-models` for the live list.",
    ),
  workspace: z
    .string()
    .optional()
    .describe(
      "Override the workspace directory Cursor operates in (`--workspace`). Defaults to the tool's defaultWorkspace, then the parent process cwd.",
    ),
  worktree: z
    .boolean()
    .optional()
    .describe("When true, spawn Cursor in a fresh git worktree (`--worktree`)."),
});

export interface CursorCodingToolResultOk {
  readonly ok: true;
  readonly text: string;
  readonly sessionId: string | null;
  readonly toolCalls: ReadonlyArray<{
    readonly id: string;
    readonly name: string;
    readonly input: unknown;
    readonly output?: unknown;
    readonly failed: boolean;
  }>;
}

export interface CursorCodingToolResultError {
  readonly ok: false;
  readonly error: string;
  readonly stderr: string;
  readonly exitCode: number;
}

export type CursorCodingToolResult = CursorCodingToolResultOk | CursorCodingToolResultError;

export function createCursorCodingTool(
  deps: CursorCodingToolDeps = {},
): AgentTool<z.infer<typeof SCHEMA>, CursorCodingToolResult> {
  return tool({
    name: "cursorCoding",
    description:
      "Delegate a coding task to Cursor's CLI agent. Returns the final answer text plus any tool calls Cursor made (file reads / writes / shell). Use for medium-to-large coding work where you want Cursor's editor-aware planning; keep small surgical edits inline. Streaming is fire-and-forget — you only see the final result.",
    parameters: SCHEMA,
    execute: async (input): Promise<CursorCodingToolResult> => {
      const req = {
        prompt: input.prompt,
        ...(deps.command !== undefined && { command: deps.command }),
        ...(input.model !== undefined && { model: input.model }),
        ...(input.model === undefined &&
          deps.defaultModel !== undefined && { model: deps.defaultModel }),
        ...(input.workspace !== undefined && { workspace: input.workspace }),
        ...(input.workspace === undefined &&
          deps.defaultWorkspace !== undefined && { workspace: deps.defaultWorkspace }),
        ...(input.worktree !== undefined && { worktree: input.worktree }),
        ...(deps.trust !== undefined && { trust: deps.trust }),
        ...(deps.sandbox !== undefined && { sandbox: deps.sandbox }),
        ...(deps.extraArgs !== undefined && { extraArgs: deps.extraArgs }),
        ...(deps.env !== undefined && { env: deps.env }),
      };

      const { events, result } = runCursorSession(req, deps.transport);
      // Drain the event stream so the underlying child makes progress.
      // The tool surface doesn't expose a stream to the model; it returns
      // the final result, so we just ignore each event.
      for await (const _ of events) {
        // Intentional drain.
      }
      const r = await result;
      if (r.isError) {
        return {
          ok: false,
          error: r.text || r.stderr || `Cursor exited with code ${r.exitCode}`,
          stderr: r.stderr,
          exitCode: r.exitCode,
        };
      }
      return {
        ok: true,
        text: r.text,
        sessionId: r.sessionId,
        toolCalls: r.toolCalls.map((tc) => {
          const inner = innerToolPayload(tc.started.tool_call);
          const completedInner = tc.completed
            ? innerToolPayload(tc.completed.tool_call)
            : undefined;
          const result = completedInner
            ? (completedInner as Record<string, unknown>).result
            : undefined;
          const failed = !!(
            result &&
            typeof result === "object" &&
            (result as Record<string, unknown>).error !== undefined
          );
          const out: {
            id: string;
            name: string;
            input: unknown;
            output?: unknown;
            failed: boolean;
          } = {
            id: tc.callId,
            name: extractToolName(tc.started.tool_call),
            input: inner ? ((inner as Record<string, unknown>).args ?? inner) : {},
            failed,
          };
          if (result !== undefined) out.output = result;
          return out;
        }),
      };
    },
  });
}

function extractToolName(toolCall: Readonly<Record<string, unknown>>): string {
  for (const key of Object.keys(toolCall)) {
    if (key.endsWith("ToolCall") || key.endsWith("Call")) {
      return key.replace(/ToolCall$|Call$/, "") || key;
    }
  }
  return Object.keys(toolCall)[0] ?? "unknown";
}

function innerToolPayload(
  toolCall: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> | null {
  for (const key of Object.keys(toolCall)) {
    const v = toolCall[key];
    if (v && typeof v === "object") return v as Record<string, unknown>;
  }
  return null;
}
