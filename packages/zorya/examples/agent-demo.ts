// ---------------------------------------------------------------------------
// Agent demo server — boots ZoryaServer with the agent gateway wired up
// against an InMemoryAgentRegistry pre-populated with three sample agents
// and a mock LLM. No API keys required.
//
// Run:
//   bun --conditions=@promin/source run packages/zorya/examples/agent-demo.ts
//
// Then try:
//   curl -X POST http://localhost:4101/api/agents \
//     -H 'content-type: application/json'   # list registered recipes (GET)
//
//   # One-shot
//   curl -X POST http://localhost:4101/api/agents/echo-bot/invoke \
//     -H 'content-type: application/json' \
//     -d '{"task":"hello there", "namespaceId":"acme", "resourceId":"alice"}'
//
//   # Streaming (SSE)
//   curl -N -X POST http://localhost:4101/api/agents/echo-bot/stream \
//     -H 'content-type: application/json' \
//     -d '{"task":"stream me", "namespaceId":"acme"}'
//
//   # Conversational thread
//   curl -X POST http://localhost:4101/api/agents/echo-bot/threads/alice-default \
//     -H 'content-type: application/json' \
//     -d '{"task":"first turn", "namespaceId":"acme", "resourceId":"alice"}'
//   curl http://localhost:4101/api/agents/echo-bot/threads/alice-default/messages?namespaceId=acme
// ---------------------------------------------------------------------------

import {
  InMemoryAgentRegistry,
  InMemoryMemoryStore,
  resolveLocalAgent,
  type RegisterAgentInput,
} from "@promin/agent";
import { echoLLM, mockLLM } from "@promin/agent/testing";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import type { LLMProvider } from "@promin/agent";
import { existsSync } from "node:fs";
import path from "node:path";
import { ZoryaServer } from "../src/index.ts";

// ---------------------------------------------------------------------------
// Sample agent recipes — three personalities, all backed by the mock LLM.
// ---------------------------------------------------------------------------

const ECHO_BOT: RegisterAgentInput = {
  id: "echo-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "echo-v1" },
    systemPrompt:
      "You are a friendly echo bot for Acme support. Repeat what the user said with a short acknowledgement.",
    tools: [],
  },
  metadata: {
    description: "Echoes user messages with a friendly tone.",
    capabilities: ["chat"],
    tags: ["demo", "stable"],
  },
};

const SUPPORT_BOT: RegisterAgentInput = {
  id: "support-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "support-v1" },
    systemPrompt:
      "You are Acme's customer support assistant. Triage issues, gather details, and resolve common problems.",
    tools: [],
  },
  metadata: {
    description: "Customer support triage agent.",
    capabilities: ["chat", "triage"],
    tags: ["demo", "beta"],
  },
};

const RESEARCH_BOT: RegisterAgentInput = {
  id: "research-bot",
  backend: {
    type: "local",
    model: { provider: "mock", id: "research-v1" },
    systemPrompt:
      "You are a research assistant. Synthesize information from past conversations and surface relevant context.",
    tools: [],
  },
  metadata: {
    description: "Cross-thread research agent with semantic recall.",
    capabilities: ["chat", "research"],
    tags: ["demo", "experimental"],
  },
};

// ---------------------------------------------------------------------------
// Per-agent LLM map — built ONCE at boot. Each LLMProvider is stateful
// (echoLLM tracks call count, mockLLM cycles through its array), so the
// gateway resolver below picks from this map rather than rebuilding the
// provider per request.
//
// `support-bot` uses a round-robin variant so multiple requests across a
// thread cycle through canned replies instead of always returning the
// first scripted response.
// ---------------------------------------------------------------------------

function roundRobinLLM(replies: ReadonlyArray<string>): LLMProvider {
  let i = 0;
  return {
    chat: async () => {
      const content = replies[i % replies.length]!;
      i += 1;
      return {
        content,
        finishReason: "stop" as const,
        usage: { inputTokens: 24, outputTokens: 18 },
      };
    },
  };
}

function buildLlmMap(): Record<string, LLMProvider> {
  return {
    "echo-bot": echoLLM({
      template: "echo-bot says: I heard '{task}' (turn #{n})",
    }),
    "support-bot": roundRobinLLM([
      "Thanks for reaching out. Could you tell me what error message you're seeing?",
      "Got it. Can you confirm whether this happens on every request or just some?",
      "Looks like a known caching issue. Try clearing your cookies for our domain — that resolves it for ~80% of cases.",
    ]),
    "research-bot": echoLLM({
      template:
        "Researching '{task}'... summary (turn #{n}): I found 3 relevant past discussions on this topic.",
    }),
  };
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

async function main() {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const memory = new InMemoryMemoryStore();
  const registry = new InMemoryAgentRegistry();

  await registry.register(ECHO_BOT);
  await registry.register(SUPPORT_BOT);
  await registry.register(RESEARCH_BOT);

  const llms = buildLlmMap();

  // Serve the bundled dashboard when it has been built. Falls back to "no
  // UI" so the gateway curl examples still work without a build step.
  const uiDir = path.resolve(import.meta.dir, "../dist/public");
  const haveUi = existsSync(path.join(uiDir, "index.html"));

  const server = new ZoryaServer({
    storage,
    uiDir: haveUi ? uiDir : undefined,
    agents: {
      registry,
      resolve: (recipe) =>
        resolveLocalAgent(recipe, {
          runner,
          memory,
          // Stable per-agent provider — built once, shared across requests
          // so cycle-state (round-robin, echo turn counters) survives.
          llm: () => llms[recipe.id] ?? echoLLM(),
          tools: {},
        }),
    },
  });

  const port = Number(process.env.PORT ?? 4101);
  const { port: actualPort, hostname } = server.listen({ port });
  const host = hostname === "0.0.0.0" ? "localhost" : hostname;

  const banner = `
╭─────────────────────────────────────────────────────────────────╮
│   Zorya Agent Gateway — demo                                    │
╰─────────────────────────────────────────────────────────────────╯

Running on http://${host}:${actualPort}

${
  haveUi
    ? `Open the dashboard:
  http://${host}:${actualPort}/#/agents
  → Agents tab lists the three recipes; click into one to chat.`
    : `Dashboard NOT served (build first):
  bun nx run @promin/zorya:build-ui
  Then re-run this demo to get the UI at http://${host}:${actualPort}/#/agents.`
}

Registered agents:
  - echo-bot      friendly echo, no tools
  - support-bot   3-turn canned support flow
  - research-bot  echo with research-style framing

Or try the HTTP gateway directly:

  # List agents
  curl http://${host}:${actualPort}/api/agents | jq

  # One-shot invoke
  curl -X POST http://${host}:${actualPort}/api/agents/echo-bot/invoke \\
    -H 'content-type: application/json' \\
    -d '{"task":"hello there","namespaceId":"acme","resourceId":"alice"}' | jq

  # Streaming (SSE)
  curl -N -X POST http://${host}:${actualPort}/api/agents/echo-bot/stream \\
    -H 'content-type: application/json' \\
    -d '{"task":"stream me","namespaceId":"acme"}'

  # Conversational thread
  curl -X POST http://${host}:${actualPort}/api/agents/support-bot/threads/alice-1 \\
    -H 'content-type: application/json' \\
    -d '{"task":"my login keeps failing","namespaceId":"acme","resourceId":"alice"}' | jq

  # Read thread history
  curl '${`http://${host}:${actualPort}/api/agents/support-bot/threads/alice-1/messages?namespaceId=acme&resourceId=alice`}' | jq

Press Ctrl+C to stop.
`;
  console.log(banner);

  // Background traffic so the dashboard's run list looks alive. Optional —
  // disable with KEEP_QUIET=1.
  if (process.env.KEEP_QUIET !== "1") {
    startBackgroundTraffic({ host, port: actualPort });
  }
}

function startBackgroundTraffic(opts: { host: string; port: number }) {
  const baseUrl = `http://${opts.host}:${opts.port}`;
  const agents = ["echo-bot", "support-bot", "research-bot"];
  const tasks = [
    "what's the status of my order?",
    "tell me a joke",
    "I forgot my password",
    "how do I cancel my subscription?",
    "is the API down?",
    "what time is it?",
  ];
  const users = ["alice", "bob", "carol"];

  setInterval(() => {
    const agent = agents[Math.floor(Math.random() * agents.length)]!;
    const task = tasks[Math.floor(Math.random() * tasks.length)]!;
    const user = users[Math.floor(Math.random() * users.length)]!;
    fetch(`${baseUrl}/api/agents/${agent}/invoke`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ task, namespaceId: "acme", resourceId: user }),
    }).catch(() => {});
  }, 5_000);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
