// Deterministic stand-ins for the demo agents' LLMs. The demo serves
// weather-bot with one; the eval suites in ../evals run every demo agent
// with them, so CI scores the agents' real tool, retrieval and memory
// paths without a network.

import type { LLMChatParams, LLMProvider, LLMResponse, Message } from "@promin/agent";

/** The text of the most recent user message. */
export function lastUserText(params: { readonly messages: ReadonlyArray<Message> }): string {
  for (let i = params.messages.length - 1; i >= 0; i--) {
    const m = params.messages[i]!;
    if (m.role === "user" && typeof m.content === "string") return m.content;
  }
  return "";
}

/**
 * Two-phase mock: a user turn emits one tool call, the tool result that
 * follows gets a final answer. "Did the previous message come from a
 * tool?" decides, so one instance handles any number of turns.
 */
export function toolCallingMockLLM(opts: {
  readonly toolName: string;
  readonly pickInput: (lastUser: string) => Record<string, unknown>;
  readonly finalAnswer: (toolResult: string) => string;
}): LLMProvider {
  let callCounter = 0;
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => {
      const last = params.messages.at(-1);
      if (last?.role === "tool") {
        return {
          content: opts.finalAnswer(last.content),
          finishReason: "stop",
          usage: { inputTokens: 32, outputTokens: 24 },
        };
      }
      callCounter += 1;
      return {
        content: null,
        toolCalls: [
          {
            id: `tc-${callCounter}`,
            name: opts.toolName,
            input: opts.pickInput(lastUserText(params)),
          },
        ],
        finishReason: "tool_calls",
        usage: { inputTokens: 28, outputTokens: 12 },
      };
    },
  };
}

const KNOWN_CITIES = [
  "berlin",
  "paris",
  "london",
  "new york",
  "tokyo",
  "san francisco",
  "kyiv",
  "lisbon",
];

/** The city a question names (title-cased), else Berlin. */
export function pickCityFromText(text: string): string {
  const lower = text.toLowerCase();
  for (const c of KNOWN_CITIES) {
    if (lower.includes(c)) return c.replace(/\b\w/g, (ch) => ch.toUpperCase());
  }
  return "Berlin";
}

/** weather-bot: calls `weather` with the city asked about, then reports it. */
export function weatherBotLLM(): LLMProvider {
  return toolCallingMockLLM({
    toolName: "weather",
    pickInput: (text) => ({ city: pickCityFromText(text) }),
    finalAnswer: (toolResult) => {
      try {
        const parsed = JSON.parse(toolResult) as { tempF?: number; conditions?: string };
        if (typeof parsed.tempF === "number" && parsed.conditions) {
          return `It's ${parsed.conditions} and ${parsed.tempF}°F right now.`;
        }
      } catch {
        // tool output not JSON — fall through
      }
      return `Got it: ${toolResult}`;
    },
  });
}

const STOPWORDS = new Set(
  (
    "a an and are as at be by can do does for from how i in is it me my of on or our " +
    "should the this to we what when where which who why will with you your must did"
  ).split(" "),
);

/** The content words of a question: lower-cased, stopwords and short words dropped. */
export function keywordsOf(text: string): string {
  return text
    .toLowerCase()
    .split(/[^a-z0-9-]+/)
    .filter((word) => word.length > 2 && !STOPWORDS.has(word))
    .join(" ");
}

/**
 * knowledge-bot: searches the org handbook (`searchOrgKnowledge`) with the
 * question's keywords, then answers citing the sources it got back.
 */
export function knowledgeBotLLM(): LLMProvider {
  return toolCallingMockLLM({
    toolName: "searchOrgKnowledge",
    pickInput: (text) => ({ query: keywordsOf(text) }),
    finalAnswer: (toolResult) => {
      const sources = [...toolResult.matchAll(/source=([^\n(]+)/g)].map((m) => m[1]!.trim());
      return sources.length === 0
        ? "The knowledge base has nothing on that; check with the doc owner."
        : `From the handbook (sources: ${[...new Set(sources)].join(", ")}).`;
    },
  });
}

/** First-person statements the fact extractor turns into facts about the user. */
const FACT_PATTERNS: ReadonlyArray<{ readonly pattern: RegExp; readonly fact: string }> = [
  { pattern: /^my name is (.+)$/i, fact: "User's name is $1" },
  { pattern: /^i(?:'m| am) on the (.+) team$/i, fact: "User is on the $1 team" },
  { pattern: /^i live in (.+)$/i, fact: "User lives in $1" },
  { pattern: /^i prefer (.+)$/i, fact: "User prefers $1" },
  { pattern: /^i(?:'m| am) allergic to (.+)$/i, fact: "User is allergic to $1" },
];

/** Numbered lines under `## <title>` in a reconcile prompt, in order. */
function numberedSection(params: { readonly text: string; readonly title: string }): string[] {
  const start = params.text.indexOf(`## ${params.title}`);
  if (start < 0) return [];
  const body = params.text.slice(start + params.title.length + 3).split(/\n## /)[0]!;
  return body
    .split("\n")
    .map((line) => /^\d+\. (.*)$/.exec(line.trim())?.[1])
    .filter((line): line is string => line !== undefined);
}

/** "User lives in" for "User lives in Lisbon": the part of a fact that names what it is about. */
function factAttribute(fact: string): string {
  return fact.split(" ").slice(0, 3).join(" ").toLowerCase();
}

/**
 * The consolidator's reconcile call, answered deterministically: a new fact
 * replaces the saved fact about the same attribute (same first three words)
 * when the two differ.
 */
function reconcileReply(params: { readonly messages: ReadonlyArray<Message> }): string {
  const text = lastUserText(params);
  const saved = numberedSection({ text, title: "Saved facts" });
  const fresh = numberedSection({ text, title: "New facts" });
  const replaces: Array<{ new: number; old: number }> = [];
  fresh.forEach((next, n) => {
    const old = saved.findIndex(
      (fact) => factAttribute(fact) === factAttribute(next) && fact !== next,
    );
    if (old >= 0) replaces.push({ new: n + 1, old: old + 1 });
  });
  return JSON.stringify({ replaces });
}

/**
 * A distillation LLM that extracts facts the way the distill prompt asks:
 * only from `[user]` transcript lines, one fact per first-person statement
 * it recognises. Answers the consolidator's JSON envelope, and its
 * reconcile call (see `reconcileReply`).
 */
export function factExtractorLLM(): LLMProvider {
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => {
      if (
        params.messages[0]?.content?.toString().includes("keep a user's saved facts consistent")
      ) {
        return { content: reconcileReply(params), finishReason: "stop" };
      }
      const transcript = lastUserText(params);
      const facts: string[] = [];
      for (const line of transcript.split("\n")) {
        const match = /^\[user\] (.*)$/.exec(line.trim());
        if (!match) continue;
        for (const sentence of match[1]!.split(/(?<=[.!?])\s+/)) {
          const statement = sentence.trim().replace(/[.!]+$/, "");
          if (statement.endsWith("?")) continue;
          for (const { pattern, fact } of FACT_PATTERNS) {
            if (pattern.test(statement)) {
              facts.push(statement.replace(pattern, fact));
              break;
            }
          }
        }
      }
      return {
        content: JSON.stringify({
          summary: "The user shared some details about themselves.",
          outcome: null,
          salience: 0.7,
          facts,
        }),
        finishReason: "stop",
        usage: { inputTokens: 40, outputTokens: 20 },
      };
    },
  };
}
