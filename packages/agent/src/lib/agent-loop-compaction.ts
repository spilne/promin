import type { LLMProvider } from "./llm-provider.ts";
import type { Message } from "./message.ts";
import { nonSystemMsgs } from "./agent-shared.ts";
import { fenceReferenceData, referenceDataMessage } from "./reference-data.ts";

export interface CompactionResult {
  messages: Message[];
  summary: string | null;
}

export interface CompactionConfig {
  keepMessages: number;
  summarize: boolean;
}

export const DEFAULT_SUMMARY_PROMPT =
  "Summarize the following conversation segment concisely. " +
  "Preserve key facts, decisions, user preferences, and any context needed for future turns.";

export const RECAP_SUMMARY_PROMPT =
  "Produce a ≤150-word summary of the conversation below. " +
  "Preserve: key decisions made, facts established, open tasks, and current task state. " +
  "Write in past tense. Output only the summary, no preamble.";

/**
 * Compact a message list by dropping the oldest non-system messages.
 *
 * Keeps the most recent `config.keepMessages` turns, slicing at a clean user-turn
 * boundary so the resulting history is never malformed (no orphaned tool results).
 * When `config.summarize` is true, the dropped segment is summarized by `llm` and
 * injected as a fenced reference-data message (user role) so the agent retains
 * high-level context. The summary is distilled from user, tool and model text,
 * so it is data, never a `system` message.
 */
export async function compact(
  messages: Message[],
  config: CompactionConfig,
  llm: LLMProvider,
  summaryPrompt = DEFAULT_SUMMARY_PROMPT,
): Promise<CompactionResult> {
  const system = messages.filter((m) => m.role === "system");
  const nonSystem = nonSystemMsgs(messages);

  // Find a clean slice boundary: the first user-turn at or after the keep window.
  // Slicing mid-sequence (e.g. keeping a tool_result without its tool_use) produces
  // invalid Anthropic API input — messages[0] would contain a tool_result block with
  // no matching tool_use in the previous message.
  let keepStart = Math.max(0, nonSystem.length - config.keepMessages);
  while (keepStart < nonSystem.length && nonSystem[keepStart]!.role !== "user") {
    keepStart++;
  }

  const keep = nonSystem.slice(keepStart);
  const dropped = nonSystem.slice(0, keepStart);

  if (!config.summarize || dropped.length === 0) {
    return { messages: [...system, ...keep], summary: null };
  }

  let summary: string | null = null;
  try {
    const summaryResp = await llm.chat({
      messages: [
        { role: "system", content: summaryPrompt },
        ...dropped,
        { role: "user", content: "Summarize the above conversation." },
      ],
    });
    summary = summaryResp.content ?? "";
  } catch (err) {
    console.error("[agentLoop] compaction summarization failed, dropping without summary:", err);
  }

  const fenced = summary
    ? fenceReferenceData({
        source: "conversation-summary",
        sections: [{ title: "Earlier conversation summary", body: summary }],
      })
    : undefined;
  return {
    messages: [...system, ...(fenced ? [referenceDataMessage(fenced)] : []), ...keep],
    summary,
  };
}
