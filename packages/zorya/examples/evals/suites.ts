// Eval suites for the demo agents. Plain JSON (`EvalSuiteSeed`): demo.ts
// registers the agent suites with the Evals page, and CI
// (`bun run evals:ci`, see run-ci.ts) runs all of them with scripted LLMs
// against the thresholds in thresholds.json.

import type { EvalSuiteSeed } from "../../src/index.ts";

/**
 * knowledge-bot retrieval: does a question pull the handbook document that
 * answers it? Scored on the sources its `searchOrgKnowledge` calls
 * returned (seeded org-handbook, ./kb).
 */
export const KNOWLEDGE_BOT_RETRIEVAL: EvalSuiteSeed = {
  id: "knowledge-bot-retrieval",
  description: "Retrieval recall of knowledge-bot on the seeded org-handbook.",
  dataset: {
    kind: "inline",
    id: "org-handbook-questions",
    cases: [
      {
        id: "sev1-escalation",
        input: "Who do I escalate to during a SEV-1 outage?",
        expected: { sources: ["oncall-runbook"] },
      },
      {
        id: "latency-slo",
        input: "What is the API p95 latency SLO?",
        expected: { sources: ["oncall-runbook"] },
      },
      {
        id: "secret-rotation",
        input: "How often are production secrets rotated?",
        expected: { sources: ["security-policy"] },
      },
      {
        id: "critical-cves",
        input: "How fast must critical CVEs be patched?",
        expected: { sources: ["security-policy"] },
      },
      {
        id: "rollback",
        input: "How do I rollback a bad deploy?",
        expected: { sources: ["deploy-pipeline"] },
      },
      {
        id: "streaming-transport",
        input: "Why did we choose SSE over WebSockets for agent streaming?",
        expected: { sources: ["rfc-001-streaming"] },
      },
      {
        id: "review-turnaround",
        input: "How quickly must a reviewer respond to a code review?",
        expected: { sources: ["code-review-guide"] },
      },
      {
        id: "core-hours",
        input: "What are the core overlap working hours?",
        expected: { sources: ["engineering-handbook"] },
      },
    ],
  },
  scorers: [
    { kind: "retrievalRecall", toolNames: ["searchOrgKnowledge"] },
    { kind: "toolTrajectory", expected: ["searchOrgKnowledge"], mode: "ordered", required: false },
  ],
  concurrency: 4,
  caseTimeoutMs: 60_000,
  metadata: { tags: ["demo", "rag"], agentIds: ["knowledge-bot"] },
};

/** weather-bot: calls the weather tool exactly once and answers from it. */
export const WEATHER_BOT_TRAJECTORY: EvalSuiteSeed = {
  id: "weather-bot-trajectory",
  description: "weather-bot calls `weather` once and reports the reading.",
  dataset: {
    kind: "inline",
    id: "weather-questions",
    cases: ["Paris", "Tokyo", "Kyiv", "San Francisco"].map((city) => ({
      id: city.toLowerCase().replace(/\s+/g, "-"),
      input: `What's the weather like in ${city} today?`,
    })),
  },
  scorers: [
    { kind: "toolTrajectory", expected: ["weather"], mode: "exact" },
    { kind: "regexMatch", id: "reports-reading", pattern: "\\d+°F" },
    { kind: "budget", maxTotalTokens: 200, required: false },
  ],
  concurrency: 4,
  caseTimeoutMs: 30_000,
  metadata: { tags: ["demo", "tools"], agentIds: ["weather-bot"] },
};

/**
 * Memory fact precision: a scripted conversation with knowledge-bot is
 * distilled, and the facts stored for the user are compared with the ones
 * the user actually stated. Its target is the conversation + distill
 * pipeline (run-ci.ts), not a single agent turn, so it is CI-only.
 */
export const MEMORY_FACT_PRECISION: EvalSuiteSeed = {
  id: "memory-fact-precision",
  description: "Facts distilled from scripted conversations, against what the user stated.",
  dataset: {
    kind: "inline",
    id: "scripted-conversations",
    cases: [
      {
        id: "introductions",
        input: {
          turns: [
            "Hi! My name is Ana. I'm on the SRE team.",
            "What does the on-call runbook say about paging?",
            "Thanks. I live in Lisbon. I am usually online in the morning.",
          ],
        },
        expected: ["User's name is Ana", "User is on the SRE team", "User lives in Lisbon"],
      },
      {
        id: "repeated-facts",
        input: {
          turns: [
            "I prefer short answers.",
            "How do I rollback a deploy?",
            "As I said, I prefer short answers. I'm allergic to long meetings.",
          ],
        },
        expected: ["User prefers short answers", "User is allergic to long meetings"],
      },
      {
        id: "questions-only",
        input: {
          turns: ["How often are secrets rotated?", "Who approves prod deploys?"],
        },
        expected: [],
      },
    ],
  },
  scorers: [
    { kind: "setMatch", id: "factPrecision", metric: "precision" },
    { kind: "setMatch", id: "factRecall", metric: "recall", required: false },
  ],
  metadata: { tags: ["demo", "memory"], agentIds: [] },
};

/** Suites demo.ts registers with the Evals page (they run against agent versions). */
export const DEMO_EVAL_SUITES: ReadonlyArray<EvalSuiteSeed> = [
  KNOWLEDGE_BOT_RETRIEVAL,
  WEATHER_BOT_TRAJECTORY,
];
