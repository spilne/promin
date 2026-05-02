/**
 * Example — agent with a knowledge graph tool
 *
 * Pre-populates an in-memory knowledge graph with people and their
 * relationships, then runs a few one-shot turns asking the agent
 * questions about them. The agent uses the `knowledgeGraph` tool to
 * query / add / search.
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... bun packages/agent/src/examples/knowledge-graph-example.ts
 */

import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { anthropic } from "../lib/adapters/anthropic.ts";
import { LocalAgent } from "../lib/agent/local-agent.ts";
import { InMemoryKnowledgeGraph } from "../lib/knowledge-graph.ts";
import { createKnowledgeGraphTool } from "../lib/tools/knowledge-graph-tools.ts";

const apiKey = process.env["ANTHROPIC_API_KEY"];
if (!apiKey) {
  console.error("Set ANTHROPIC_API_KEY to run.");
  process.exit(1);
}

const kg = new InMemoryKnowledgeGraph();

await kg.addNode({ id: "alice", type: "person", metadata: { role: "engineer" } });
await kg.addNode({ id: "bob", type: "person", metadata: { role: "designer" } });
await kg.addNode({ id: "charlie", type: "person", metadata: { role: "manager" } });

await kg.addRelationship("alice", "worked_with", "bob", { project: "Website Redesign" });
await kg.addRelationship("bob", "reports_to", "charlie");
await kg.addRelationship("alice", "reports_to", "charlie");
await kg.addRelationship("charlie", "manages", "alice");
await kg.addRelationship("charlie", "manages", "bob");

console.log("Knowledge graph seeded with alice, bob, charlie + relationships.\n");

const agent = new LocalAgent({
  namespaceId: "demo",
  agentId: "kg-bot",
  runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
  agent: {
    name: "kg-bot",
    llm: anthropic("claude-sonnet-4-6", { apiKey }),
    tools: { knowledgeGraph: createKnowledgeGraphTool({ graph: kg }) },
    systemPrompt: `You are a helpful assistant with a knowledge graph of people and their relationships.

Use the \`knowledgeGraph\` tool to:
- query relationships ("worked_with", "reports_to", "manages")
- search for people by name or metadata
- add new people / relationships when learned
- show what's connected to a node

Reply in one short sentence.`,
  },
});

const prompts = [
  "Who does Alice work with?",
  "Who reports to Charlie?",
  "Add a new person named Dave who is a product manager and reports to Charlie.",
  "Now list everyone Charlie manages.",
];

for (const prompt of prompts) {
  console.log(`> ${prompt}`);
  const out = await agent.invoke({ task: prompt });
  console.log(`  ${await out.text}\n`);
}
