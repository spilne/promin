// ---------------------------------------------------------------------------
// draft-review DAG recipes — a sub-graph inside an evaluator/optimizer loop.
//
// One round drafts and reviews a topic. The parent repeats that round until
// the reviewer passes it, then waits for a person to approve publication.
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
  version: "v2",
  metadata: {
    description: "Repeat draft review until it passes, then request publication approval.",
    tags: ["demo", "evaluation", "repeat", "sub-dag", "approval"],
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
    {
      id: "sign-off",
      kind: "approval",
      prompt: "Publish this draft?",
      timeoutMs: 86_400_000,
      inputs: {
        draft: { kind: "node", nodeId: "round", path: "writer" },
        review: { kind: "node", nodeId: "round", path: "review" },
      },
    },
    {
      id: "published",
      kind: "transform",
      inputs: {
        draft: { kind: "node", nodeId: "round", path: "writer" },
      },
      from: "draft",
      ops: [{ op: "pick", path: "" }],
    },
  ],
  edges: [
    { from: "round", to: "sign-off" },
    {
      from: "sign-off",
      to: "published",
      condition: { kind: "equals", path: "approved", value: true },
    },
  ],
  entry: ["round"],
  terminals: ["published"],
};
