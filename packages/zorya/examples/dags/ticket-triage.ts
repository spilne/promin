// ---------------------------------------------------------------------------
// ticket-triage DAG recipe — deterministic decision routing with one merge.
//
// A typed decision chooses the support team and refund intent. Exactly one
// specialist reply runs, then an any-join compacts the routed result.
// ---------------------------------------------------------------------------

import type { RegisterDagInput } from "@promin/agent";

export const ticketTriageRecipe: RegisterDagInput = {
  id: "ticket-triage",
  version: "v1",
  metadata: {
    description: "Classify a support ticket and route it to one specialist reply.",
    tags: ["demo", "decision", "router"],
  },
  nodes: [
    {
      id: "triage",
      kind: "decide",
      model: { provider: "llm", id: "demo" },
      questions: {
        team: {
          type: "choice",
          instructions: "Which team should handle this support ticket?",
          options: [
            { name: "billing", description: "Charges, invoices, payments, or refunds" },
            { name: "technical", description: "Errors, outages, or product behavior" },
            { name: "other", description: "Anything that belongs to neither team" },
          ],
        },
        refund: {
          type: "yesno",
          instructions: "Is the customer asking about a refund or duplicate charge?",
        },
      },
      inputs: { message: { kind: "initial", path: "message" } },
    },
    {
      id: "billing-reply",
      kind: "agent",
      agentId: "dag-billing-reply",
      inputs: {
        message: { kind: "initial", path: "message" },
        triage: { kind: "node", nodeId: "triage", path: "" },
      },
    },
    {
      id: "tech-reply",
      kind: "agent",
      agentId: "dag-tech-reply",
      inputs: {
        message: { kind: "initial", path: "message" },
        triage: { kind: "node", nodeId: "triage", path: "" },
      },
    },
    {
      id: "general-reply",
      kind: "agent",
      agentId: "dag-general-reply",
      inputs: {
        message: { kind: "initial", path: "message" },
        triage: { kind: "node", nodeId: "triage", path: "" },
      },
    },
    {
      id: "reply",
      kind: "transform",
      join: "any",
      inputs: {
        billing: { kind: "node", nodeId: "billing-reply", path: "" },
        technical: { kind: "node", nodeId: "tech-reply", path: "" },
        other: { kind: "node", nodeId: "general-reply", path: "" },
      },
      ops: [
        { op: "collect", fields: ["billing", "technical", "other"] },
        { op: "compact" },
        { op: "take", count: 1 },
      ],
    },
  ],
  edges: [
    {
      from: "triage",
      to: "billing-reply",
      condition: { kind: "equals", path: "team.choice", value: "billing" },
    },
    {
      from: "triage",
      to: "tech-reply",
      condition: { kind: "equals", path: "team.choice", value: "technical" },
    },
    {
      from: "triage",
      to: "general-reply",
      condition: { kind: "equals", path: "team.choice", value: "other" },
    },
    { from: "billing-reply", to: "reply" },
    { from: "tech-reply", to: "reply" },
    { from: "general-reply", to: "reply" },
  ],
  entry: ["triage"],
  terminals: ["reply"],
};
