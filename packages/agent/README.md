# @promin/agent

Agent loop, multi-agent orchestration, LLM adapters, tools, memory, and terminal UI for building interactive AI applications.

---

## Table of contents

- [Core concepts](#core-concepts)
- [agentLoop — persistent sessions](#agentloop--persistent-sessions)
- [agentAction — single-shot workflows](#agentaction--single-shot-workflows)
- [Tools](#tools)
  - [tool()](#tool)
  - [multiTool()](#multitool)
  - [Built-in tools](#built-in-tools)
- [Memory](#memory)
- [Context compaction](#context-compaction)
- [Multi-agent patterns](#multi-agent-patterns)
  - [agentNetwork — hub-and-spoke](#agentnetwork--hub-and-spoke)
  - [createAgentTown — message-passing mesh](#createagenttown--message-passing-mesh)
  - [runCouncil — multi-model deliberation](#runcouncil--multi-model-deliberation)
- [LLM adapters](#llm-adapters)
  - [anthropic](#anthropic)
  - [openai / gemini / ollama / llamacpp](#openai--gemini--ollama--llamacpp)
  - [routerLLM](#routerllm)
  - [fallbackLLM](#fallbackllm)
  - [twoSpeedLLM](#twospeedllm)
- [Evals](#evals)
- [Terminal UI](#terminal-ui)
- [Session logging](#session-logging)

---

## Core concepts

| Concept          | What it is                                                                        |
| ---------------- | --------------------------------------------------------------------------------- |
| `AgentLoop`      | Factory for long-lived interactive sessions. One loop, many sessions.             |
| `AgentSession`   | A single conversation: accumulates message history, can stream, can be compacted. |
| `agentAction`    | One-shot agent workflow (journaled, no session state between calls).              |
| `AgentTool`      | A Zod-validated tool the LLM can call.                                            |
| `LLMProvider`    | Uniform interface across Anthropic, OpenAI, Gemini, Ollama, etc.                  |
| `MemoryStore`    | Cross-session long-term memory.                                                   |
| `WorkflowRunner` | Durable execution engine from `@promin/workflow` — makes agents crash-safe.       |

---

## agentLoop — persistent sessions

Use `agentLoop` for interactive chat where the user sends many messages in sequence.

```ts
import { agentLoop } from "@promin/agent";
import { anthropic } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

const storage = new InMemoryWorkflowStorage();
const runner = createWorkflowRunner({ storage });

const loop = agentLoop({
  name: "my-agent",
  llm: anthropic("claude-sonnet-4-6", { apiKey }),
  tools: { search: webSearchTool },
  systemPrompt: "You are a helpful assistant.",
  memory: { store: memoryStore },
});

const session = await loop.session({ runner, sessionId: "s1" });

// Streaming
for await (const chunk of session.stream("What's the weather in Tokyo?")) {
  process.stdout.write(chunk);
}

// Non-streaming
const answer = await session.send("Summarise that.");

await session.close();
```

### AgentSession API

| Method                        | Description                                           |
| ----------------------------- | ----------------------------------------------------- |
| `send(task)`                  | Run a turn and return the full answer string.         |
| `stream(task, opts?)`         | Run a turn and yield token deltas.                    |
| `approve(toolCallId)`         | Approve a tool call gated by `requireApproval: true`. |
| `reject(toolCallId, reason?)` | Reject a pending tool call.                           |
| `status()`                    | `"idle"` / `"thinking"` / `"waiting_approval"`.       |
| `messages()`                  | Full message history as of the last completed turn.   |
| `usage()`                     | Cumulative `{ inputTokens, outputTokens }`.           |
| `compact(opts?)`              | Manually compact history between turns.               |
| `close()`                     | Shut down and release resources.                      |

### Key config options

```ts
agentLoop({
  name: "my-agent",
  llm,
  tools, // plain tools map
  toolRegistry, // dynamic hot-loadable registry (alternative to tools)
  systemPrompt,
  maxTurns: 1000,
  maxStepsPerTurn: 20,
  context: {
    maxMessages: 80, // compact when history exceeds this
    keepMessages: 40, // keep this many after compaction
    summarize: true, // LLM summarizes dropped messages
    contextLimit, // token-based compaction threshold
    compressAt: 0.7, // compact at 70% of contextLimit
  },
  memory: {
    store: memoryStore,
    injectLimit: 5, // inject N relevant memories at session start
    saveOnCompact: true,
  },
  hooks: {
    beforeTurn, // inject context before the LLM loop
    afterTurn, // analytics, logging
    onIdle, // fires when the session is quiet for idleTimeoutMs
    onApprovalRequired, // inline approval for the same process (terminal REPLs)
    onClose,
  },
  compactionLlm, // cheaper model for summaries
  thinkingBudgetTokens, // extended thinking (Claude 3.7+)
  logger, // InMemorySessionLogger for /log pane
});
```

### Side effects in hooks

Most agent hooks (`beforeTurn`, `afterTurn`, `onLifecycle`, `onApprovalRequired`, `processors.beforeLLM` / `afterLLM`) run inside journaled activities. The journal short-circuits the callback on replay — the activity's recorded return value is read directly without re-invoking the hook. This is the right default for replay safety: a worker restart mid-turn won't re-prompt the user for approval, won't double-write to your audit log, won't double-bump a rate-limit counter.

`agentAction`'s `onStep` is the exception. It runs BETWEEN journaled activities, so the workflow body re-runs the callback on every recovery pass. To gate non-idempotent side effects, every hook context now carries a boolean `isReplay` flag plumbed from `ctx.isReplay` on the workflow body:

```ts
agentAction({
  // ...
  onStep: ({ step, isReplay }) => {
    if (isReplay) return; // skip metrics on replay
    metrics.record("agent.step", { step });
  },
});
```

The semantics: `isReplay` is `true` when the wrapping workflow body is executing on top of pre-existing journal entries (worker restart, signal-resume). For hooks that fire only when their wrapping `ctx.activity` is fresh (everything except `onStep`), the flag tells you whether the BODY containing this activity has been re-started — useful when you want side effects keyed to body restarts, but not required for replay safety. For `onStep`, gate non-idempotent side effects on `!isReplay`.

For durable audit trails of approval decisions, prefer the persistent approval-storage primitive over relying on `onApprovalRequired` side effects — the storage records decisions outside the activity boundary so they survive replay correctly.

---

## agentAction — single-shot workflows

Use `agentAction` when you need a one-shot agent run that is fully durable and replayable.

```ts
import { agentAction } from "@promin/agent";
import { z } from "zod";

const classifier = agentAction({
  name: "classifier",
  llm,
  systemPrompt: "Classify the sentiment of the input text.",
  outputSchema: z.object({
    sentiment: z.enum(["positive", "neutral", "negative"]),
    confidence: z.number(),
  }),
  maxSteps: 5,
});

const { output } = await runner.run({
  workflow: classifier,
  workflowId: "c1",
  input: { task: "I love this product!" },
});
// output: { sentiment: "positive", confidence: 0.97 }
```

`outputSchema` uses tool-calling under the hood — the agent calls `_respond({ ... })` when ready, which is then Zod-parsed.

---

## Tools

### tool()

Type-safe wrapper that infers input/output from the Zod schema:

```ts
import { tool } from "@promin/agent";
import { z } from "zod";

const calculator = tool({
  name: "calculate",
  description: "Evaluate a simple arithmetic expression.",
  parameters: z.object({ expression: z.string() }),
  execute: async ({ expression }) => eval(expression), // illustrative
});
```

### multiTool()

Group multiple operations under one LLM tool call to keep the tool list short:

```ts
import { multiTool, command } from "@promin/agent";

const memory = multiTool({
  name: "memory",
  description: "Read and write long-term memory.",
  commands: {
    search: command({
      description: "Find relevant memories",
      parameters: z.object({ query: z.string(), limit: z.number().optional() }),
      execute: async ({ query, limit }) => store.search(query, limit),
    }),
    save: command({
      description: "Persist a new memory",
      parameters: z.object({ content: z.string() }),
      execute: async ({ content }) => store.save({ content }),
    }),
  },
});
```

### Built-in tools

| Factory                                      | Description                                                     |
| -------------------------------------------- | --------------------------------------------------------------- |
| `createFilesystemTools()`                    | `readFile`, `writeFile`, `listDir`, `statFile`                  |
| `createShellTool()`                          | Execute shell commands in a workspace                           |
| `createMemoryTools()` / `createMemoryTool()` | `searchMemory`, `saveMemory` (or a combined `memory` multiTool) |
| `createLlmTool()`                            | One-shot LLM call from inside the agent                         |
| `createAgentTool()`                          | Delegate to a child agentLoop session                           |
| `createFetchUrlTool()`                       | Fetch a URL                                                     |
| `createWebSearchTool()`                      | Web search                                                      |
| `createSchedulerTools()`                     | `scheduleTask`, `listSchedules`, `cancelSchedule`               |
| `createWriteToolTool()`                      | Let the agent write and hot-load new tools at runtime           |
| `createRequireSecretTool()`                  | Prompt the user for an API key via a secure channel             |

---

## Memory

`MemoryStore` provides scoped long-term memory. The bundled `InMemoryMemoryStore` is suitable for development; wire in a vector-backed store (e.g. Postgres with `pgvector`) for production.

```ts
import { InMemoryMemoryStore } from "@promin/agent";

const store = new InMemoryMemoryStore({
  embeddings: myEmbeddingProvider, // optional: enables semantic search
});

const id = await store.save({ content: "User prefers dark mode." });
const results = await store.search("UI preferences", 5);
```

Inject into `agentLoop` via `memory.store` — the loop automatically retrieves relevant memories at session start and saves compaction summaries.

---

## Context compaction

When message history grows large, `agentLoop` automatically compacts it:

- **Message-count trigger**: fires when non-system messages exceed `context.maxMessages` (default 80). Keeps the most recent `keepMessages` (default 40) turns at a clean user-turn boundary.
- **Token-based trigger**: fires when input tokens for a think step exceed `context.contextLimit * compressAt`. Uses a RECAP prompt to produce a ≤150-word summary of the dropped segment.
- **Manual compaction**: call `session.compact()` between turns at any time (e.g. via a `/compact` REPL command).

When `summarize: true` (the default), dropped turns are summarized by the LLM and injected as a `system` message so the agent retains high-level context.

---

## Multi-agent patterns

### agentNetwork — hub-and-spoke

Each specialist is an `agentAction` workflow. The orchestrator calls them as tools.

```ts
import { agentNetwork, agentAction, agentLoop } from "@promin/agent";

const network = agentNetwork({
  runner,
  agents: {
    researcher: {
      workflow: agentAction({ llm, tools: { search }, systemPrompt: "Research topics." }),
      description: "Searches the web and returns a summary.",
    },
    coder: {
      workflow: agentAction({ llm, tools: { shell }, systemPrompt: "Write and run code." }),
      description: "Writes and executes code. Returns output or file path.",
    },
  },
});

const orchestrator = agentLoop({
  llm,
  tools: network.handoffTools(),
  systemPrompt: "Orchestrate tasks using the specialist agents.",
});
```

Each handoff starts a new durable workflow — fully journaled and observable.

### createAgentTown — message-passing mesh

Agents communicate asynchronously via inboxes. The Mayor is the human-facing entry point; all others are background daemons.

```ts
import { createAgentTown } from "@promin/agent";

const town = createAgentTown({
  runner,
  mayor: "coordinator",
  agents: {
    coordinator: { llm, prompt: "Delegate subtasks to peers." },
    researcher: { llm, tools: { search }, prompt: "Research topics on request." },
    coder: { llm, tools: { shell }, prompt: "Write code on request." },
  },
  sharedMemory: sharedStore,
});

for await (const chunk of town.stream("Research PRPH2 and write a summary.")) {
  process.stdout.write(chunk);
}
await town.close();
```

Every agent automatically gets `sendMessage` and (for the mayor) `readInbox` tools. Each agent's LLM turns are individually journaled.

| Dimension           | agentNetwork       | createAgentTown       |
| ------------------- | ------------------ | --------------------- |
| Specialist lifetime | Ephemeral per call | Persistent session    |
| Communication       | Sync tool call     | Async message-passing |
| Topology            | Hub-and-spoke      | Mesh (any-to-any)     |
| Best for            | Stateless subtasks | Long-running agents   |

### runCouncil — multi-model deliberation

```ts
import { runCouncil } from "@promin/agent";

const result = await runCouncil("Should we adopt microservices?", {
  councilors: [
    { name: "Alice", llm: claude, role: "advocate — argue for the proposal" },
    { name: "Bob", llm: gpt4, role: "skeptic — find risks and failure modes" },
  ],
  synthesizer: claude,
  rounds: 2,
});
console.log(result.verdict);
```

All councilor calls in each round run in parallel. Use `createCouncilTool()` to expose it as an `AgentTool` inside an agent.

---

## LLM adapters

All adapters implement `LLMProvider` and are interchangeable.

### anthropic

```ts
import { anthropic } from "@promin/agent";

const llm = anthropic("claude-sonnet-4-6", {
  apiKey,
  maxTokens: 4096,
});
```

Automatically enables prompt caching (system + tools prefix is cached). Extended thinking: pass `thinkingBudgetTokens` in `LLMChatParams`.

### openai / gemini / ollama / llamacpp

```ts
import { openai, gemini, ollama, llamacpp } from "@promin/agent";

const gpt4 = openai("gpt-4o", { apiKey: openaiKey });
const gemini = gemini("gemini-1.5-pro", { apiKey: geminiKey });
const local = ollama("llama3", { baseUrl: "http://localhost:11434" });
const cpp = llamacpp("my-model", { baseUrl: "http://localhost:8080" });
```

### routerLLM

Route each call to a different model based on a predicate:

```ts
import { routerLLM } from "@promin/agent";

const llm = routerLLM([
  { when: (p) => p.messages.length > 60, use: longContextModel },
  { when: () => true, use: defaultModel },
]);
```

### fallbackLLM

Try providers in order; move to the next on any error:

```ts
import { fallbackLLM } from "@promin/agent";

const llm = fallbackLLM([primaryProvider, backupProvider]);
```

**Note**: mid-stream fallback in `chatStream()` delivers partial interleaved output. Use `chat()` if that is unacceptable.

### twoSpeedLLM

Automatically routes to a cheap/fast model for synthesis steps and a capable model for reasoning:

```ts
import { twoSpeedLLM } from "@promin/agent";

const llm = twoSpeedLLM({
  capable: anthropic("claude-opus-4-7", { apiKey }),
  fast: anthropic("claude-haiku-4-5-20251001", { apiKey }),
  // Optional override:
  when: (p) => /* true → fast */ false,
});
```

Default heuristic: if the last non-system message is a tool result → `fast`; otherwise → `capable`.

---

## Evals

```ts
import { runEval, exactMatch, containsAll, llmJudge } from "@promin/agent";

const results = await runEval({
  agent: myAgentWorkflow,
  runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
  cases: [
    { input: "What is 2+2?", expected: "4" },
    { input: "Capital of France?", expected: "Paris" },
  ],
  scorers: [
    exactMatch,
    containsAll(["Paris"]),
    llmJudge({ llm: claude, rubric: "Score accuracy 0-10." }),
  ],
  concurrency: 5,
});

for (const r of results) {
  console.log(r.case.input, r.scores);
}
```

| Scorer                      | Description                                           |
| --------------------------- | ----------------------------------------------------- |
| `exactMatch`                | 1 if output matches expected (case-insensitive trim). |
| `containsAll(strs)`         | 1 if output contains every string in `strs`.          |
| `llmJudge({ llm, rubric })` | LLM scores 0–10; returned as 0–1.                     |

---

## Terminal UI

For building interactive console applications:

```ts
import { Terminal, ChatTerminal, ConsoleRunner, MarkdownRenderer } from "@promin/agent";
```

| Class              | Description                                                                                   |
| ------------------ | --------------------------------------------------------------------------------------------- |
| `Terminal`         | Spinner, prompt animation, synchronized output, panes, tree views.                            |
| `ChatTerminal`     | Readline REPL with tab-completion, multi-line input (`\` continuation), Shift+Enter newlines. |
| `ConsoleRunner`    | Wraps `runTurn` with abort signal, retry, and streaming; manages Ctrl+C behavior.             |
| `MarkdownRenderer` | Streaming syntax-highlighted Markdown renderer for terminals.                                 |

See `packages/agent/src/examples/console-chat.ts` for a complete REPL example.

---

## Session logging

```ts
import { InMemorySessionLogger } from "@promin/agent";

const logger = new InMemorySessionLogger(2000); // ring buffer, 2000 events max

const loop = agentLoop({ ..., logger });

// After some turns:
const events = logger.events();
// [{ type: "turn.start", turn: 0, task: "Hello", ts: 1714000000000 }, ...]
```

Event types: `turn.start`, `turn.end`, `turn.aborted`, `llm.call`, `tool.start`, `tool.end`, `tool.parse_error`, `compact`, `approval.requested`, `approval.decision`, `step_limit.hit`, `subagent.start`, `subagent.end`.
