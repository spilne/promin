import type { LLMProvider } from "./llm-provider.ts";
import type { Message } from "./message.ts";
import { nonSystemMsgs } from "./agent-shared.ts";

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

  return {
    messages: [
      ...system,
      ...(summary
        ? [{ role: "system" as const, content: `Earlier conversation summary:\n${summary}` }]
        : []),
      ...keep,
    ],
    summary,
  };
}
