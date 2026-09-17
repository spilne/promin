// ---------------------------------------------------------------------------
// research-synthesis DAG recipe — diamond-shape multi-specialist pipeline.
//
// 1 planner → 3 parallel researchers → 1 synthesizer
//
// References agents by id:
//   - "dag-planner"      — splits topic into 3 sub-questions
//   - "dag-researcher"   — produces a finding for each sub-question
//   - "dag-synthesizer"  — joins findings into a single report
//
// Caller is expected to register these agent recipes (with their own
// system prompts + LLM bindings) before running the DAG. demo.ts wires
// them up against the host's existing model catalog.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const researchSynthesisRecipe: RegisterDagInput = {
  id: "research-synthesis",
  version: "v1",
  metadata: {
    description: "Diamond-shape research synthesis: split topic → parallel research → join.",
    tags: ["demo", "research"],
  },
  nodes: [
    {
      id: "planner",
      agentId: "dag-planner",
      inputs: {
        task: { kind: "initial", path: "topic" },
      },
    },
    {
      id: "research-1",
      agentId: "dag-researcher",
      inputs: {
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "1st sub-question" },
      },
    },
    {
      id: "research-2",
      agentId: "dag-researcher",
      inputs: {
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "2nd sub-question" },
      },
    },
    {
      id: "research-3",
      agentId: "dag-researcher",
      inputs: {
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "3rd sub-question" },
      },
    },
    {
      id: "synthesizer",
      agentId: "dag-synthesizer",
      inputs: {
        topic: { kind: "initial", path: "topic" },
        finding1: { kind: "node", nodeId: "research-1", path: "" },
        finding2: { kind: "node", nodeId: "research-2", path: "" },
        finding3: { kind: "node", nodeId: "research-3", path: "" },
      },
    },
  ],
  edges: [
    { from: "planner", to: "research-1" },
    { from: "planner", to: "research-2" },
    { from: "planner", to: "research-3" },
    { from: "research-1", to: "synthesizer" },
    { from: "research-2", to: "synthesizer" },
    { from: "research-3", to: "synthesizer" },
  ],
  entry: ["planner"],
  terminals: ["synthesizer"],
};
