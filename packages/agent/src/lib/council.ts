import { z } from "zod";
import { tool } from "./tool.ts";
import type { LLMProvider } from "./llm-provider.ts";

// ---- types ----

export interface Councilor {
  /** Unique name shown in the deliberation transcript */
  name: string;
  /** LLM this councilor uses — lets you mix Claude, GPT-4, Gemini, etc. */
  llm: LLMProvider;
  /**
   * Role description injected as the councilor's system prompt.
   * Examples: "skeptic — find risks and failure modes",
   *           "advocate — argue for the proposal",
   *           "pragmatist — focus on implementation cost"
   */
  role: string;
}

export interface CouncilConfig {
  councilors: Councilor[];
  /**
   * LLM that reads the full deliberation and produces the final verdict.
   * Can be the same as a councilor's LLM or a separate, more capable model.
   */
  synthesizer: LLMProvider;
  /**
   * Number of deliberation rounds.
   * - Round 1: each councilor analyzes the question independently.
   * - Round 2+: each councilor reads all previous round outputs and critiques/refines.
   * Default: 1. Two rounds is usually sufficient; more rounds have diminishing returns.
   */
  rounds?: number;
  /** Max tokens per councilor per turn. Default: 1024. */
  maxTokens?: number;
  /** Max tokens for the synthesizer. Default: 2048. */
  synthMaxTokens?: number;
  signal?: AbortSignal;
}

export interface CouncilContribution {
  councilor: string;
  role: string;
  text: string;
}

export interface CouncilRound {
  round: number;
  contributions: CouncilContribution[];
}

export interface CouncilResult {
  verdict: string;
  rounds: CouncilRound[];
}

// ---- core ----

/**
 * Convene a council of agents to deliberate on a question.
 *
 * Each councilor analyzes from their own perspective in round 1.
 * In subsequent rounds they read and critique each other's output.
 * A synthesizer agent produces the final verdict.
 *
 * All councilor calls in each round run in parallel.
 *
 * Usage:
 *   const result = await runCouncil("Should we use microservices?", {
 *     councilors: [
 *       { name: "Alice", llm: claude, role: "advocate — argue for the proposal" },
 *       { name: "Bob",   llm: gpt4,   role: "skeptic — find risks and failure modes" },
 *       { name: "Carol", llm: claude, role: "pragmatist — weigh implementation cost" },
 *     ],
 *     synthesizer: claude,
 *     rounds: 2,
 *   });
 *   console.log(result.verdict);
 */
export async function runCouncil(question: string, config: CouncilConfig): Promise<CouncilResult> {
  const numRounds = config.rounds ?? 1;
  const rounds: CouncilRound[] = [];

  for (let r = 0; r < numRounds; r++) {
    const previousContributions = rounds[r - 1]?.contributions ?? [];

    const settled = await Promise.allSettled(
      config.councilors.map(async (c): Promise<CouncilContribution> => {
        const isFirstRound = r === 0;

        const systemPrompt = isFirstRound
          ? `You are ${c.name}, a council member.\nYour role: ${c.role}.\nAnalyze the question from your perspective. Be direct, specific, and honest about uncertainty.`
          : `You are ${c.name}, a council member.\nYour role: ${c.role}.\nYou have seen the previous round's analyses. Critique, challenge, or refine them from your perspective. Be direct about disagreements.`;

        let userContent: string;
        if (isFirstRound) {
          userContent = question;
        } else {
          const own = previousContributions.find((p) => p.councilor === c.name);
          const others = previousContributions.filter((p) => p.councilor !== c.name);
          const sections: string[] = [];
          if (own) sections.push(`Your previous analysis:\n${own.text}`);
          if (others.length)
            sections.push(
              `Other council members' analyses:\n\n${others.map((p) => `[${p.councilor} — ${p.role}]:\n${p.text}`).join("\n\n")}`,
            );
          userContent = `${question}\n\n${sections.join("\n\n")}`;
        }

        const resp = await c.llm.chat({
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userContent },
          ],
          maxTokens: config.maxTokens ?? 1024,
          signal: config.signal,
        });

        return { councilor: c.name, role: c.role, text: resp.content ?? "" };
      }),
    );

    const contributions: CouncilContribution[] = settled.map((result, i) => {
      if (result.status === "fulfilled") return result.value;
      const c = config.councilors[i]!;
      console.error(`[council] councilor "${c.name}" failed:`, result.reason);
      return { councilor: c.name, role: c.role, text: "[unavailable]" };
    });

    rounds.push({ round: r + 1, contributions });
  }

  // Build deliberation transcript for the synthesizer
  const transcript = rounds
    .map(
      (round) =>
        `--- Round ${round.round} ---\n` +
        round.contributions.map((c) => `[${c.councilor} — ${c.role}]:\n${c.text}`).join("\n\n"),
    )
    .join("\n\n");

  const synthResp = await config.synthesizer.chat({
    messages: [
      {
        role: "system",
        content:
          "You are a neutral synthesizer. Your job is to read a council deliberation and produce a clear, reasoned final verdict. " +
          "Acknowledge the strongest points from each member. Resolve conflicts explicitly. Be decisive — give a concrete recommendation.",
      },
      {
        role: "user",
        content: `Question: ${question}\n\nCouncil deliberation:\n\n${transcript}\n\nYour verdict:`,
      },
    ],
    maxTokens: config.synthMaxTokens ?? 2048,
    signal: config.signal,
  });

  return { verdict: synthResp.content ?? "", rounds };
}

// ---- tool integration ----

export interface CouncilToolConfig extends CouncilConfig {
  /** Tool name exposed to the agent. Default: "council" */
  name?: string;
  /** Tool description exposed to the agent. */
  description?: string;
}

/**
 * Wraps runCouncil as an agent tool so any agentLoop can convene a council on demand.
 *
 * Usage:
 *   agentLoop({
 *     tools: {
 *       council: createCouncilTool({
 *         councilors: [...],
 *         synthesizer: claude,
 *         rounds: 1,
 *       }),
 *     },
 *   });
 *
 * The agent calls: council({ question: "Should we use X or Y?" })
 */
export function createCouncilTool(config: CouncilToolConfig) {
  return tool({
    name: config.name ?? "council",
    description:
      config.description ??
      "Convene a council of expert agents to deliberate on a question and produce a synthesized verdict. " +
        `Council members: ${config.councilors.map((c) => `${c.name} (${c.role})`).join(", ")}. ` +
        "Use for complex decisions, design trade-offs, or whenever a second (and third) opinion matters.",
    parameters: z.object({
      question: z.string().describe("The question or proposal for the council to deliberate on"),
    }),
    execute: async ({ question }) => {
      const result = await runCouncil(question, config);
      return formatCouncilResult(result);
    },
  });
}

// ---- formatting ----

/** Format a CouncilResult as a readable string for the agent or for display. */
export function formatCouncilResult(result: CouncilResult): string {
  const deliberation = result.rounds
    .map(
      (r) =>
        `Round ${r.round}:\n` +
        r.contributions.map((c) => `  [${c.councilor}]: ${c.text}`).join("\n\n"),
    )
    .join("\n\n---\n\n");

  return `Verdict:\n${result.verdict}\n\nDeliberation:\n${deliberation}`;
}
