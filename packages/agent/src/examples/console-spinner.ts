import type { Terminal } from "./common/terminal.ts";
import type { ToolRegistry } from "../lib/tool-registry.ts";
import type { AgentTool } from "../lib/tool.ts";

const ABBREV_PRIORITY = [
  "command",
  "cmd",
  "path",
  "file",
  "url",
  "query",
  "expression",
  "text",
  "message",
  "content",
  "name",
  "prompt",
  "input",
];

export function abbrevInput(input: Record<string, unknown>): string {
  for (const key of ABBREV_PRIORITY) {
    if (key in input && typeof input[key] === "string") {
      const v = input[key] as string;
      return v.length > 42 ? `${v.slice(0, 39)}…` : v;
    }
  }
  for (const v of Object.values(input)) {
    if (typeof v === "string") return v.length > 42 ? `${v.slice(0, 39)}…` : v;
  }
  return "";
}

const HANG_WARN_MS = 30_000;

export interface SpinnerTracker {
  /** Current completed-tool-call count within this turn — used by approval hook for step label. */
  readonly step: number;
  /** Wrap a registry so every tool call appears in the spinner with live elapsed time. */
  withStatusTracking(registry: ToolRegistry): ToolRegistry;
  /** Reset between turns. */
  resetTurn(): void;
}

export function createSpinnerTracker(term: Terminal): SpinnerTracker {
  const activeCalls = new Map<string, { name: string; abbrevDim: string; startMs: number }>();
  let callSeq = 0;
  let liveRefresh: ReturnType<typeof setInterval> | null = null;
  let turnStep = 0;

  function refreshSpinner(): void {
    if (activeCalls.size === 0) {
      term.startSpinner(`thinking...  \x1b[2mstep ${turnStep + 1}\x1b[0m`);
      return;
    }
    const now = Date.now();
    const labels = [...activeCalls.values()].map((e) => {
      const ms = now - e.startMs;
      const elapsed = ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : "";
      const warn = ms >= HANG_WARN_MS ? " \x1b[33m⚠ hanging?\x1b[0m" : "";
      const suffix = elapsed ? `  \x1b[2m${elapsed}${warn}\x1b[0m` : warn;
      return `→ ${e.name}${e.abbrevDim}${suffix}`;
    });
    term.startSpinner(labels.length === 1 ? labels[0] : labels.join(`  \x1b[2m║\x1b[0m  `));
  }

  return {
    get step() {
      return turnStep;
    },

    resetTurn() {
      turnStep = 0;
      activeCalls.clear();
    },

    withStatusTracking(registry: ToolRegistry): ToolRegistry {
      return {
        getTools() {
          const tools = registry.getTools();
          return Object.fromEntries(
            Object.entries(tools).map(([name, t]) => [
              name,
              {
                ...t,
                // biome-ignore lint/suspicious/noExplicitAny: runtime-validated by Zod in agentAction
                execute: async (input: any) => {
                  const abbrev = abbrevInput(input ?? {});
                  const abbrevDim = abbrev ? `  \x1b[2m${abbrev}\x1b[0m` : "";
                  const callId = String(++callSeq);
                  const startMs = Date.now();
                  activeCalls.set(callId, { name, abbrevDim, startMs });
                  refreshSpinner();
                  if (!liveRefresh) liveRefresh = setInterval(refreshSpinner, 1_000);
                  let failed = false;
                  try {
                    return await t.execute(input);
                  } catch (err) {
                    failed = true;
                    throw err;
                  } finally {
                    activeCalls.delete(callId);
                    if (activeCalls.size === 0 && liveRefresh) {
                      clearInterval(liveRefresh);
                      liveRefresh = null;
                    }
                    const ms = Date.now() - startMs;
                    const elapsedStr = ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`;
                    if (failed) {
                      term.printAbove(
                        `\x1b[31m✗ ${name}${abbrevDim}  failed  (${elapsedStr})\x1b[0m`,
                      );
                    } else {
                      term.printAbove(`\x1b[2m✓ ${name}${abbrevDim}  (${elapsedStr})\x1b[0m`);
                    }
                    turnStep++;
                    refreshSpinner();
                  }
                },
              } as AgentTool<any, any>,
            ]),
          ) as Record<string, AgentTool<any, any>>;
        },
        close: () => registry.close(),
      };
    },
  };
}
