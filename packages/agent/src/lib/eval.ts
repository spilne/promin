import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { Workflow } from "@promin/workflow";
import type { WorkflowRunner } from "@promin/workflow";
import type { AgentInput, AgentResult } from "./agent-action.ts";
import type { LLMProvider } from "./llm-provider.ts";

export interface EvalCase {
  input: string;
  expected?: string;
  metadata?: Record<string, unknown>;
}

export interface EvalScore {
  scorer: string;
  score: number;
}

export interface EvalResult {
  case: EvalCase;
  output: string;
  scores: EvalScore[];
  durationMs: number;
  error?: string;
}

export interface EvalScorer {
  name: string;
  score(params: { output: string; expected: string | undefined; input: string }): Promise<number>;
}

/**
 * Run an offline evaluation suite against an `agentAction` workflow.
 *
 * Each case is run in its own isolated `InMemoryWorkflowStorage`, so cases cannot
 * share state. Results include the agent's output, per-scorer scores, and wall-clock
 * duration. Errors during a case are caught and recorded — the suite never throws.
 *
 * `concurrency` controls how many cases run in parallel (default: 1 = sequential).
 *
 * @example
 * ```ts
 * const results = await runEval({
 *   agent: myAgent,
 *   runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
 *   cases: [{ input: "What is 2+2?", expected: "4" }],
 *   scorers: [exactMatch, llmJudge({ llm: claude, rubric: "..." })],
 *   concurrency: 5,
 * });
 * ```
 */
export async function runEval(params: {
  agent: Workflow<AgentInput, AgentResult>;
  runner: WorkflowRunner;
  cases: EvalCase[];
  scorers: EvalScorer[];
  concurrency?: number;
  workflowIdPrefix?: string;
}): Promise<EvalResult[]> {
  const { agent, cases, scorers, concurrency = 1, workflowIdPrefix = "eval" } = params;

  const results: EvalResult[] = Array.from<EvalResult>({ length: cases.length });

  const runCase = async (evalCase: EvalCase, index: number): Promise<void> => {
    const workflowId = `${workflowIdPrefix}-${index}`;
    const storage = new InMemoryWorkflowStorage();
    const caseRunner = createWorkflowRunner({ storage });

    const start = Date.now();
    let output = "";
    let errorMsg: string | undefined;

    try {
      const result = (await caseRunner.run({
        workflow: agent,
        workflowId,
        input: { task: evalCase.input },
      })) as AgentResult;
      output = result.answer;
    } catch (err) {
      errorMsg = err instanceof Error ? err.message : String(err);
    }

    const durationMs = Date.now() - start;

    let scores: EvalScore[];
    if (errorMsg !== undefined) {
      scores = scorers.map((s) => ({ scorer: s.name, score: 0 }));
    } else {
      scores = await Promise.all(
        scorers.map(async (s) => ({
          scorer: s.name,
          score: await s.score({ output, expected: evalCase.expected, input: evalCase.input }),
        })),
      );
    }

    const evalResult: EvalResult = { case: evalCase, output, scores, durationMs };
    if (errorMsg !== undefined) {
      evalResult.error = errorMsg;
    }
    results[index] = evalResult;
  };

  const queue = cases.map((c, i) => ({ c, i }));
  const slots = Math.min(concurrency, cases.length);

  const runSlot = async (): Promise<void> => {
    while (queue.length > 0) {
      const item = queue.shift();
      if (!item) break;
      await runCase(item.c, item.i);
    }
  };

  await Promise.all(Array.from({ length: slots }, () => runSlot()));

  return results;
}

/** Scores 1 when output and expected match (case-insensitive trim), 0 otherwise. */
export const exactMatch: EvalScorer = {
  name: "exactMatch",
  score: async ({ output, expected }) => {
    if (expected === undefined) return 0;
    return output.trim().toLowerCase() === expected.trim().toLowerCase() ? 1 : 0;
  },
};

/** Scores 1 when the output contains every string in `substrings` (case-insensitive), 0 otherwise. */
export function containsAll(substrings: string[]): EvalScorer {
  return {
    name: "containsAll",
    score: async ({ output }) => {
      const lower = output.toLowerCase();
      return substrings.every((s) => lower.includes(s.toLowerCase())) ? 1 : 0;
    },
  };
}

/**
 * LLM-as-judge scorer. Asks `llm` to score the output 0–10 using `rubric` as the
 * system prompt. Returns a normalized 0–1 score.
 */
export function llmJudge(params: { llm: LLMProvider; rubric: string }): EvalScorer {
  return {
    name: "llmJudge",
    score: async ({ output, expected, input }) => {
      const response = await params.llm.chat({
        messages: [
          { role: "system", content: params.rubric },
          {
            role: "user",
            content: `Input: ${input}\nOutput: ${output}\nExpected: ${expected ?? ""}\nScore 0-10:`,
          },
        ],
      });
      const text = response.content ?? "";
      const match = text.match(/\d+/);
      if (!match) return 0;
      const raw = parseInt(match[0], 10);
      return Math.min(raw, 10) / 10;
    },
  };
}
