// ---------------------------------------------------------------------------
// Stall detector — notices an agent turn that stopped making progress.
//
// Two signals:
//   - repeated calls: a round whose tool calls were all made before in this
//     turn (same tool, same input) and whose text is empty or repeated adds
//     nothing; `maxRepeatRounds` such rounds in a row is a stall.
//   - tool budget: one tool called more than `maxCallsPerTool` times in a
//     turn is a stall, whatever the inputs.
//
// The engine rebuilds the detector from the journaled LLM responses on
// every replay, so its verdicts are deterministic. It holds no clock: a
// round is the unit of time.
// ---------------------------------------------------------------------------

import type { ToolCall } from "./message.ts";

export interface StallDetectorOptions {
  /** No-progress rounds in a row before a final answer is forced. Default 3. */
  readonly maxRepeatRounds?: number;
  /** Calls of one tool within a turn before a final answer is forced. Default 15. */
  readonly maxCallsPerTool?: number;
}

export type StallVerdict =
  | { readonly stalled: false }
  | {
      readonly stalled: true;
      readonly reason: "repeated_calls" | "tool_budget";
      /** The tool that tripped the detector (the round's first call for repeats). */
      readonly toolName: string;
    };

const DEFAULT_MAX_REPEAT_ROUNDS = 3;
const DEFAULT_MAX_CALLS_PER_TOOL = 15;

export class StallDetector {
  private readonly maxRepeatRounds: number;
  private readonly maxCallsPerTool: number;
  private readonly seenCalls = new Set<string>();
  private readonly seenTexts = new Set<string>();
  private readonly callsPerTool = new Map<string, number>();
  private stuckRounds = 0;

  constructor(options: StallDetectorOptions = {}) {
    this.maxRepeatRounds = options.maxRepeatRounds ?? DEFAULT_MAX_REPEAT_ROUNDS;
    this.maxCallsPerTool = options.maxCallsPerTool ?? DEFAULT_MAX_CALLS_PER_TOOL;
  }

  /** Record one round — the assistant text and the tool calls it asked for. */
  observe(round: { content: string | null; calls: ReadonlyArray<ToolCall> }): StallVerdict {
    const text = round.content?.trim() ?? "";
    const newText = text !== "" && !this.seenTexts.has(text);
    if (text !== "") this.seenTexts.add(text);

    let newCall = false;
    let overBudget: string | undefined;
    for (const call of round.calls) {
      const signature = `${call.name}\u0000${stableStringify(call.input)}`;
      if (!this.seenCalls.has(signature)) {
        newCall = true;
        this.seenCalls.add(signature);
      }
      const count = (this.callsPerTool.get(call.name) ?? 0) + 1;
      this.callsPerTool.set(call.name, count);
      if (count > this.maxCallsPerTool && overBudget === undefined) overBudget = call.name;
    }

    if (overBudget !== undefined) {
      return { stalled: true, reason: "tool_budget", toolName: overBudget };
    }
    this.stuckRounds = newCall || newText ? 0 : this.stuckRounds + 1;
    if (this.stuckRounds >= this.maxRepeatRounds && round.calls.length > 0) {
      return { stalled: true, reason: "repeated_calls", toolName: round.calls[0]!.name };
    }
    return { stalled: false };
  }
}

/** JSON with object keys sorted, so `{a,b}` and `{b,a}` sign the same. */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "undefined";
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
}
