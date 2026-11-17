// ---------------------------------------------------------------------------
// research-synthesis DAG recipe — typed planning, fan-out and synthesis.
//
// One planner returns a typed question list. `forEach` runs the researcher
// once per question, then a transform compacts successful findings before
// the synthesizer writes the terminal report.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const researchSynthesisRecipe: RegisterDagInput = {
  id: "research-synthesis",
  version: "v3",
  metadata: {
    description: "Typed research plan → fan-out research → compact findings → synthesis.",
    tags: ["demo", "research", "fan-out"],
  },
  nodes: [
    {
      id: "planner",
      kind: "agent",
      agentId: "dag-planner",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      output: {
        schema: {
          type: "object",
          properties: {
            questions: { type: "array", items: { type: "string" } },
          },
          required: ["questions"],
        },
      },
    },
    {
      id: "research",
      kind: "agent",
      agentId: "dag-researcher",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      forEach: {
        items: { kind: "node", nodeId: "planner", path: "questions" },
        as: "question",
        maxItems: 8,
      },
    },
    {
      id: "findings",
      kind: "transform",
      inputs: {
        research: { kind: "node", nodeId: "research", path: "" },
      },
      from: "research",
      ops: [{ op: "compact" }],
    },
    {
      id: "synthesizer",
      kind: "agent",
      agentId: "dag-synthesizer",
      inputs: {
        topic: { kind: "initial", path: "topic" },
        findings: { kind: "node", nodeId: "findings", path: "" },
      },
    },
  ],
  edges: [
    { from: "planner", to: "research" },
    { from: "research", to: "findings" },
    { from: "findings", to: "synthesizer" },
  ],
  entry: ["planner"],
  terminals: ["synthesizer"],
};
