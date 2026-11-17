// ---------------------------------------------------------------------------
// dag-research-synthesis — runnable demo of operator-authored agentic DAG.
//
//   planner (typed questions)
//      ↓
//   research × forEach(question)
//      ↓
//   findings (compact transform)
//      ↓
//   synthesizer
//
// Why this is a good fit for the operator-authored DAG primitive:
//   - Different agents at different cost tiers (cheap planner + cheap
//     researchers + expensive synthesizer). One generic agent can't make
//     the cost / quality tradeoffs operators want at each station.
//   - Genuine parallelism (`forEach` researcher attempts run independently;
//     a worker crash replays finished item children instead of re-running them).
//   - Replay-safety actually matters: by the time you reach the
//     synthesizer node, you've already paid for 4 LLM calls. A worker
//     restart there shouldn't re-do them.
//
// Run:
//   bun --conditions=@promin/source run packages/zorya/examples/dag-research-synthesis.ts "your topic here"
//
// The demo auto-detects whether you have ANTHROPIC_API_KEY set:
//   - Without key: every node uses a deterministic scripted LLM, so it always
//     runs and prints the DAG-level structure without making real calls.
//   - With key: planner + researchers use Haiku (fast/cheap), synthesizer
//     uses Sonnet (quality), so you can see real model-tier mixing.
// ---------------------------------------------------------------------------

import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import {
  anthropic,
  InMemoryDagRegistry,
  LocalAgent,
  createDagWorkflow,
  type Agent,
  type AgentResolver,
  type LLMProvider,
} from "@promin/agent";
import { researchSynthesisRecipe } from "./dags/research-synthesis.ts";
import { scriptedDagLlms } from "./dags/scripted-dag-llms.ts";

const haveAnthropic = !!process.env["ANTHROPIC_API_KEY"];
const topic = process.argv[2] ?? "what makes a good engineering culture";

// ---------------------------------------------------------------------------
// 1. The DAG recipe — operator-authored, not LLM-generated.
// ---------------------------------------------------------------------------

const researchSynthesisDag = researchSynthesisRecipe;
const scripted = scriptedDagLlms();

// ---------------------------------------------------------------------------
// 2. Wire the agents the DAG references.
//    Each `agentId` in the DAG resolves to one of these.
// ---------------------------------------------------------------------------

function plannerLlm(): LLMProvider {
  // Cheap fast model — planner job is structured + short
  if (haveAnthropic) return anthropic("claude-haiku-4-5-20251001");
  return scripted["dag-planner"];
}

function researcherLlm(): LLMProvider {
  if (haveAnthropic) return anthropic("claude-haiku-4-5-20251001");
  return scripted["dag-researcher"];
}

function synthesizerLlm(): LLMProvider {
  // Quality model — synthesizer reads 3 inputs, decides what's important,
  // drops contradictions, writes the final piece.
  if (haveAnthropic) return anthropic("claude-sonnet-4-6");
  return scripted["dag-synthesizer"];
}

// ---------------------------------------------------------------------------
// 3. Build the resolver that maps agentId → Agent.
// ---------------------------------------------------------------------------

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

function makeAgent(name: string, llm: LLMProvider, systemPrompt: string): Agent {
  return new LocalAgent({
    agent: { name, llm, systemPrompt },
    runner,
    namespaceId: "demo",
  }) as unknown as Agent;
}

const agents: Record<string, Agent> = {
  "dag-planner": makeAgent(
    "planner",
    plannerLlm(),
    "You are a research planner. Given a topic, emit exactly 3 sub-questions, one per line, " +
      "covering different angles (historical, current, contrarian).",
  ),
  "dag-researcher": makeAgent(
    "researcher",
    researcherLlm(),
    "You are a research specialist. Given a topic and one question, produce a 4-6 sentence " +
      "research finding. Be factual and concrete.",
  ),
  "dag-synthesizer": makeAgent(
    "synthesizer",
    synthesizerLlm(),
    "You are a senior research editor. Given a topic and 3 research findings, produce a " +
      "single synthesized 8-12 sentence report. Resolve contradictions explicitly. Avoid filler.",
  ),
};

const resolver: AgentResolver = async ({ agentId }) => {
  const a = agents[agentId];
  if (!a) throw new Error(`unknown agent in DAG: ${agentId}`);
  return a;
};

// ---------------------------------------------------------------------------
// 4. Register the DAG, build the durable workflow, run it.
// ---------------------------------------------------------------------------

async function main() {
  console.log(`\n=== Agentic DAG demo: research-synthesis ===`);
  console.log(`Topic: ${topic}`);
  console.log(
    `Live LLMs: ${haveAnthropic ? "yes (Haiku + Sonnet)" : "no — using scripted models"}`,
  );

  const dagRegistry = new InMemoryDagRegistry();
  const dag = await dagRegistry.register(researchSynthesisDag);
  console.log(`\nRegistered DAG: ${dag.id}@${dag.version}`);
  console.log(`  ${dag.nodes.length} nodes, ${dag.edges.length} edges`);
  console.log(`  entry: ${dag.entry.join(",")}  terminals: ${dag.terminals.join(",")}\n`);

  const wf = createDagWorkflow({ registry: dagRegistry, resolver });

  console.log("Running…\n");
  const start = Date.now();
  const result = await runner.run({
    workflow: wf,
    workflowId: `research-${Date.now()}`,
    input: {
      dagId: dag.id,
      version: dag.version,
      initialInput: { topic },
      scope: { namespaceId: "demo", resourceId: "cli" },
    },
  });
  const elapsed = Date.now() - start;

  console.log(`\n=== Result (${elapsed}ms) ===`);
  console.log(`ok: ${result.ok}`);
  console.log(`completed nodes: ${Object.keys(result.nodeOutputs).length}`);
  if (result.skipped.length > 0) console.log(`skipped: ${result.skipped.join(", ")}`);
  if (Object.keys(result.errors).length > 0)
    console.log(`errors: ${JSON.stringify(result.errors, null, 2)}`);

  console.log(`\n--- planner output ---`);
  console.log(result.nodeOutputs.planner);
  console.log(`\n--- research fan-out ---`);
  console.log(result.nodeOutputs.research);
  console.log(`\n--- compact findings ---`);
  console.log(result.nodeOutputs.findings);
  console.log(`\n--- synthesizer (final) ---`);
  console.log(result.outputs.synthesizer);

  console.log(
    `\n💡 Replay test: run again with the same workflowId — every node will short-circuit.`,
  );
  console.log(`   workflowId used: research-${Date.now()}`);
}

main().catch((err) => {
  console.error("Demo failed:", err);
  process.exit(1);
});
