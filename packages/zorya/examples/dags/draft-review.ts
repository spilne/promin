// ---------------------------------------------------------------------------
// draft-review DAG recipes — a sub-graph inside an evaluator/optimizer loop.
//
// One round drafts and reviews a topic. The parent repeats that round until
// the reviewer passes it, carrying the prior round's outputs into the writer.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const draftReviewRoundRecipe: RegisterDagInput = {
  id: "draft-review-round",
  version: "v1",
  metadata: {
    description: "Write a draft, then return a typed editorial verdict.",
    tags: ["demo", "evaluation", "sub-dag"],
  },
  nodes: [
    {
      id: "writer",
      kind: "agent",
      agentId: "dag-writer",
      inputs: {
        topic: { kind: "initial", path: "topic" },
        previous: { kind: "initial", path: "previous" },
      },
    },
    {
      id: "review",
      kind: "agent",
      agentId: "dag-reviewer",
      inputs: {
        draft: { kind: "node", nodeId: "writer", path: "" },
        previous: { kind: "initial", path: "previous" },
      },
      output: {
        schema: {
          type: "object",
          properties: {
            verdict: { type: "string", enum: ["pass", "revise"] },
            notes: { type: "string" },
          },
          required: ["verdict", "notes"],
        },
      },
    },
  ],
  edges: [{ from: "writer", to: "review" }],
  entry: ["writer"],
  terminals: ["writer", "review"],
};

export const draftReviewRecipe: RegisterDagInput = {
  id: "draft-review",
  version: "v1",
  metadata: {
    description: "Repeat the draft-review sub-graph until its reviewer passes.",
    tags: ["demo", "evaluation", "repeat", "sub-dag"],
  },
  nodes: [
    {
      id: "round",
      kind: "dag",
      dagId: "draft-review-round",
      version: "v1",
      inputs: {
        topic: { kind: "initial", path: "topic" },
      },
      repeat: {
        until: { kind: "equals", path: "review.verdict", value: "pass" },
        maxIterations: 3,
        onExhausted: "continue",
      },
    },
  ],
  edges: [],
  entry: ["round"],
  terminals: ["round"],
};
