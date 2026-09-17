// ---------------------------------------------------------------------------
// dag-research-synthesis — runnable demo of operator-authored agentic DAG.
//
//   ┌──────────┐
//   │ planner  │   cheap fast model — splits topic into 3 sub-questions
//   └──┬───────┘
//      ├──────────────────────┬──────────────────────┐
//      ▼                      ▼                      ▼
//   ┌──────────┐         ┌──────────┐           ┌──────────┐
//   │researcher│         │researcher│           │researcher│   parallel — each
//   │   #1     │         │   #2     │           │   #3     │   gets one sub-question
//   └────┬─────┘         └────┬─────┘           └────┬─────┘
//        └─────────────────┬──┴───────────────────┘
//                          ▼
//                    ┌──────────────┐
//                    │ synthesizer  │   higher-quality model — joins findings
//                    └──────────────┘   into a single report
//
// Why this is a good fit for the operator-authored DAG primitive:
//   - Different agents at different cost tiers (cheap planner + cheap
//     researchers + expensive synthesizer). One generic agent can't make
//     the cost / quality tradeoffs operators want at each station.
//   - Genuine parallelism (3 researchers run independently — without the
//     DAG's per-node journaling, a worker crash mid-run loses all the
//     completed researcher output and re-bills you for re-running them).
//   - Replay-safety actually matters: by the time you reach the
//     synthesizer node, you've already paid for 4 LLM calls. A worker
//     restart there shouldn't re-do them.
//
// Run:
//   bun --conditions=@promin/source run packages/zorya/examples/dag-research-synthesis.ts "your topic here"
//
// The demo auto-detects whether you have ANTHROPIC_API_KEY set:
//   - Without key: every node uses a mock echo LLM, so it always runs +
//     prints the DAG-level structure without making real calls.
//   - With key: planner + researchers use Haiku (fast/cheap), synthesizer
//     uses Sonnet (quality), so you can see real model-tier mixing.
// ---------------------------------------------------------------------------

import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import {
  anthropic,
  InMemoryDagRegistry,
  LocalAgent,
  createDagWorkflow,
  type AgenticDagRecipe,
  type Agent,
  type AgentResolver,
  type LLMProvider,
} from "@promin/agent";
import { echoLLM } from "@promin/agent/testing";

const haveAnthropic = !!process.env["ANTHROPIC_API_KEY"];
const topic = process.argv[2] ?? "what makes a good engineering culture";

// ---------------------------------------------------------------------------
// 1. The DAG recipe — operator-authored, not LLM-generated.
// ---------------------------------------------------------------------------

const researchSynthesisDag: Omit<AgenticDagRecipe, "id" | "version"> = {
  nodes: [
    {
      id: "planner",
      agentId: "planner",
      // The planner gets the raw topic and is asked to emit 3 sub-questions.
      inputs: {
        task: { kind: "initial", path: "topic" },
      },
    },
    {
      id: "research-1",
      agentId: "researcher",
      inputs: {
        // For demo simplicity we route the whole planner output to each
        // researcher; in production a `pickPath` against a structured
        // planner output would route specific sub-questions per branch.
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "1st sub-question" },
      },
    },
    {
      id: "research-2",
      agentId: "researcher",
      inputs: {
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "2nd sub-question" },
      },
    },
    {
      id: "research-3",
      agentId: "researcher",
      inputs: {
        task: { kind: "node", nodeId: "planner", path: "" },
        focus: { kind: "literal", value: "3rd sub-question" },
      },
    },
    {
      id: "synthesizer",
      agentId: "synthesizer",
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
  metadata: {
    description: "Diamond-shape research synthesis: split → parallel research → join",
    tags: ["research", "demo"],
  },
};

// ---------------------------------------------------------------------------
// 2. Wire the agents the DAG references.
//    Each `agentId` in the DAG resolves to one of these.
// ---------------------------------------------------------------------------

function plannerLlm(): LLMProvider {
  // Cheap fast model — planner job is structured + short
  if (haveAnthropic) return anthropic("claude-haiku-4-5-20251001");
  return echoLLM({
    template:
      "Sub-questions:\n1. What's the historical context of '{task}'?\n2. Who are the leading voices on '{task}'?\n3. What's the contrarian view on '{task}'?",
  });
}

function researcherLlm(): LLMProvider {
  if (haveAnthropic) return anthropic("claude-haiku-4-5-20251001");
  return echoLLM({
    template: "Research finding (mock): brief takeaway on '{task}'.",
  });
}

function synthesizerLlm(): LLMProvider {
  // Quality model — synthesizer reads 3 inputs, decides what's important,
  // drops contradictions, writes the final piece.
  if (haveAnthropic) return anthropic("claude-sonnet-4-6");
  return echoLLM({
    template: "Final synthesis (mock): rolled up the 3 findings into one summary.",
  });
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
  planner: makeAgent(
    "planner",
    plannerLlm(),
    "You are a research planner. Given a topic, emit exactly 3 sub-questions, one per line, " +
      "covering different angles (historical, current, contrarian). Be concise — sub-questions only.",
  ),
  researcher: makeAgent(
    "researcher",
    researcherLlm(),
    "You are a research specialist. Given a topic + a focus directive, produce a 4-6 sentence " +
      "research finding. Be factual and concrete.",
  ),
  synthesizer: makeAgent(
    "synthesizer",
    synthesizerLlm(),
    "You are a senior research editor. Given a topic and 3 research findings, produce a " +
      "single synthesized 8-12 sentence report. Resolve contradictions explicitly. Avoid filler.",
  ),
};

const resolver: AgentResolver = async (agentId) => {
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
  console.log(`Live LLMs: ${haveAnthropic ? "yes (Haiku + Sonnet)" : "no — using echo mocks"}`);

  const dagRegistry = new InMemoryDagRegistry();
  const dag = await dagRegistry.register({
    id: "research-synthesis",
    version: "v1",
    ...researchSynthesisDag,
  });
  console.log(`\nRegistered DAG: ${dag.id}@${dag.version}`);
  console.log(`  ${dag.nodes.length} nodes, ${dag.edges.length} edges`);
  console.log(`  entry: ${dag.entry.join(",")}  terminals: ${dag.terminals.join(",")}\n`);

  const wf = createDagWorkflow({ resolver, name: "research-synthesis-dag" });

  console.log("Running…\n");
  const start = Date.now();
  const result = await runner.run({
    workflow: wf,
    workflowId: `research-${Date.now()}`,
    input: { dag, initialInput: { topic } },
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
  console.log(`\n--- research-1 ---`);
  console.log(result.nodeOutputs["research-1"]);
  console.log(`\n--- research-2 ---`);
  console.log(result.nodeOutputs["research-2"]);
  console.log(`\n--- research-3 ---`);
  console.log(result.nodeOutputs["research-3"]);
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
