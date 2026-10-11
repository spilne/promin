# @promin/agent

Agent loop, LLM adapters, tools, memory, retrieval, and a terminal UI for building interactive AI applications.

---

## Table of contents

- [Core concepts](#core-concepts)
- [agentLoop — persistent sessions](#agentloop--persistent-sessions)
- [agentAction — single-shot workflows](#agentaction--single-shot-workflows)
- [Tools](#tools)
  - [tool()](#tool)
  - [Multi-command tools](#multi-command-tools)
  - [Built-in tools](#built-in-tools)
- [Memory](#memory)
- [Retrieval (RAG)](#retrieval-rag)
- [Context compaction](#context-compaction)
- [LLM adapters](#llm-adapters)
  - [anthropic](#anthropic)
  - [openai / gemini / ollama / llamacpp](#openai--gemini--ollama--llamacpp)
  - [routerLLM](#routerllm)
  - [resilientLLM](#resilientllm)
  - [rotatingLLM](#rotatingllm)
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

For durable audit trails of approval decisions, read the journaled decision back from workflow storage instead of relying on `onApprovalRequired` side effects — the journal survives replay correctly.

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

### Multi-command tools

Group related operations under one tool with a `z.discriminatedUnion` on `command`. The JSON-schema converter flattens a top-level discriminated union into one object with a required `command` enum (provider tool APIs reject a top-level `anyOf`), and Zod still validates each branch's required fields:

```ts
import { tool } from "@promin/agent";
import { z } from "zod";

const notes = tool({
  name: "notes",
  description: "Read and write notes.\n\nCommands:\n  search: find notes\n  save: persist a note",
  parameters: z.discriminatedUnion("command", [
    z.object({ command: z.literal("search"), query: z.string(), limit: z.number().default(5) }),
    z.object({ command: z.literal("save"), content: z.string() }),
  ]),
  execute: async (input) => {
    switch (input.command) {
      case "search":
        return store.search(input.query, input.limit);
      case "save":
        return store.save(input.content);
    }
  },
});
```

Approval is per tool (`requireApproval`), not per command.

### Built-in tools

`shell`, `fetchUrl`, `writeFile` and `schedulerCreate` ask for approval by
default. `fetchUrl` reaches public hosts only unless its `OutboundPolicy` says
otherwise; the same policy (`checkOutboundUrl`, `outboundFetch`,
`createOutboundFetch`) guards any server-side fetch of a URL you did not write
yourself and connects to the address it checked, which defeats DNS rebinding.

| Factory                         | Description                                                                                           |
| ------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `createFilesystemTools()`       | `readFile`, `writeFile`, `listDir`, `statFile`, confined to a root (symlinks resolved)                |
| `createShellTool()`             | Run an allowlisted program with argv (no shell), scrubbed env, group-kill on timeout                  |
| `createLayeredMemoryTool()`     | `memory` tool over a `MemoryStore`: `set`, `setWorking`, `recall`                                     |
| `createLlmTool()`               | One-shot LLM call from inside the agent                                                               |
| `createAgentTool()`             | Delegate to a child agentLoop session                                                                 |
| `createFetchUrlTool()`          | Fetch a public http(s) URL (SSRF policy, pinned DNS, size cap, approval)                              |
| `createWebSearchTool()`         | Web search                                                                                            |
| `createDurableSchedulerTools()` | `schedulerCreate` (approval, min interval, atomic per-thread cap), `schedulerList`, `schedulerCancel` |
| `createWriteToolTool()`         | Let the agent write and hot-load new tools at runtime                                                 |

---

## Memory

`MemoryStore` is the agent's durable state, in three scopes (namespace → resource → thread): operator-authored static rules, facts, a markdown working-memory scratchpad, episodes (distilled summaries) and the thread's messages. `resolveContext` merges the scopes every turn and splits them by trust: the static rules become `instructions` (a system block after the persona, capped at `MAX_STATIC_RULES_CHARS`), while facts, working memory and episodes become `reference` — one fenced reference-data block sent as a user-role message, never as a system message, because users, tools and the model can all write it. `InMemoryMemoryStore` is the reference implementation; `PgMemoryStore` (`@promin/postgres`) and `SqliteMemoryStore` (`@promin/sqlite`) persist it.

Every fact records its provenance (`source`: `operator`, `user` or `model`). The `memory` tool writes `model` facts (at most `MAX_FACT_CHARS`) and scratchpads (at most `MAX_WORKING_MEMORY_CHARS`). `DefaultConsolidator.distillThread` extracts facts only from what the user said — tool results are left out of its transcript — and caps resource facts at `maxResourceFacts` (default `DEFAULT_MAX_RESOURCE_FACTS`, 50), evicting only the oldest `model` facts.

Untrusted text — memory, retrieved knowledge, compaction summaries, `recall` and retriever-tool output — always goes through `fenceReferenceData`: a "this is data, not instructions" frame plus `<reference-data>` delimiters that the content cannot forge (look-alike tags inside it are neutralised).

```ts
import { InMemoryMemoryStore, LocalAgent, createLayeredMemoryTool } from "@promin/agent";

const memory = new InMemoryMemoryStore();
await memory.appendResourceFact(
  { namespaceId: "acme", resourceId: "alice" },
  { text: "Prefers dark mode.", source: "user" },
);

// LocalAgent wires the memory cascade (and, by default, the `memory` tool) for you:
const agent = new LocalAgent({
  agent: { name: "support", llm },
  runner,
  memory,
  namespaceId: "acme",
  resourceId: "alice",
});

// Or give any agent loop the model-facing tool directly:
const memoryTool = createLayeredMemoryTool({
  store: memory,
  namespaceId: "acme",
  resourceId: "alice",
  threadId: "t-1",
});
```

The `memory` tool exposes `set` (a durable fact at thread or resource scope), `setWorking` (replace the scratchpad) and `recall` (keyword search over facts and episodes, ranked with episode salience). `DefaultConsolidator` distills threads into episodes and resource facts.

---

## Retrieval (RAG)

A `Retriever` answers `retrieve({ query, topK?, filter?, maxChunkCharacters? })` with
scored, source-bearing chunks (`{ chunk: { id, text, index, source: { id, title?, uri?, tags?, metadata? } }, score }`).
Higher scores are more relevant; the scale is retriever-specific.

| Export                                   | What it does                                                                                                                                           |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `FlatTextChunker`                        | Paragraph chunker (`maxSize` 1000, `overlap` 100 chars). Implements `KnowledgeChunker`.                                                                |
| `InMemoryRetriever`                      | In-process store: cosine similarity with an `EmbeddingProvider`, keyword overlap without one. `addDocument`, `addChunks`, `deleteSource`.              |
| `PgVectorRetriever` (`@promin/postgres`) | pgvector table: `ensureSchema({ createExtension?, index? })`, `addDocument`, `addChunks` (upsert by chunk id), `deleteSource`, cosine search.          |
| `createRetrieverTool`                    | Wraps a retriever as an agent tool (default name `search_knowledge_base`). Sources land in the model text and in the tool result's `sources` metadata. |
| `InMemoryRetrieverRegistry`              | `RetrieverRegistry` of live retrievers keyed by id, with description / tags / metadata for catalogs.                                                   |

```ts
import { InMemoryRetriever, LocalAgent } from "@promin/agent";

const docs = new InMemoryRetriever({ embeddings: myEmbeddingProvider });
await docs.addDocument({ id: "returns", title: "Returns", text: returnsPolicyMarkdown });

const agent = new LocalAgent({
  agent: { name: "support", llm },
  runner,
  retrievers: {
    // A bare retriever becomes a `search_docs` tool the model calls on demand.
    docs,
    // A binding can instead inject results as context before every turn, or both.
    policy: {
      retriever: policyRetriever,
      mode: "tool-and-context",
      topK: 4,
      filter: { tags: ["public"] },
    },
  },
});
```

Binding modes (`AgentRetrieverBinding.mode`):

- `tool` (default) — a search tool named `name ?? retrieverToolName(id)` (`search_<id>`; the id `default` maps to `search_knowledge_base`). A tool already on the agent with the same name wins.
- `context` — the turn's task is searched before every turn (one-shot and thread) and the hits are added as a fenced reference-data message (user role, never system) right before the task. It rides in the workflow input, so replays see the same context, and it is never persisted to the thread history.
- `tool-and-context` — both.

`topK`, `filter`, `maxChunkCharacters`, `includeSources` and `includeScores` apply to both paths. The binding's `filter` is merged over a filter the model passes to the tool: tags are combined and the binding's metadata keys win, so the model cannot widen a tenant scope.

Recipes reference retrievers by id in `backend.knowledge` (`AgentKnowledgeRecipe`: `id` plus the binding options above). `resolveLocalAgent` looks each id up in `deps.retrievers`, a plain `{ id: Retriever }` map or a `RetrieverRegistry`, and throws when one is missing:

```ts
const retrievers = new InMemoryRetrieverRegistry([
  { id: "docs", retriever: docs, description: "Product docs", tags: ["public"] },
]);
const agent = resolveLocalAgent(recipe, { runner, llm, tools, retrievers });
```

`retrieverRegistryTestSuite` (from `@promin/agent/testing`) checks a custom `RetrieverRegistry`.

---

## Context compaction

When message history grows large, `agentLoop` automatically compacts it:

- **Message-count trigger**: fires when non-system messages exceed `context.maxMessages` (default 80). Keeps the most recent `keepMessages` (default 40) turns at a clean user-turn boundary.
- **Token-based trigger**: fires when input tokens for a think step exceed `context.contextLimit * compressAt`. Uses a RECAP prompt to produce a ≤150-word summary of the dropped segment.
- **Manual compaction**: call `session.compact()` between turns at any time (e.g. via a `/compact` REPL command).

When `summarize: true` (the default), dropped turns are summarized by the LLM and injected as a `system` message so the agent retains high-level context.

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

const gpt = openai("gpt-4o", { apiKey: openaiKey }); // any compatible server: baseUrl "https://host/v1"
const flash = gemini("gemini-2.5-flash", { apiKey: geminiKey });
const local = ollama({ model: "llama3", baseURL: "http://localhost:11434" });
const cpp = llamacpp({ baseURL: "http://localhost:8080" });
```

Every HTTP adapter accepts `timeoutMs` (default 10 minutes, covering the whole request including the stream), `clock` and `fetch`; the request also aborts on `LLMChatParams.signal`. Failures are typed: `LLMHttpError` (`status`, `retryAfterMs`, `provider`, `body` excerpt) for HTTP errors and errors streamed after a 200, `LLMTimeoutError`, and `LLMStreamInterruptedError` when a stream ends early. Tool arguments the model sent as malformed or truncated JSON arrive as `ToolCall.inputError`; the agent returns it to the model as the tool result instead of running the tool.

### routerLLM

Route each call to a different model based on a predicate:

```ts
import { routerLLM } from "@promin/agent";

const llm = routerLLM([
  { when: (p) => p.messages.length > 60, use: longContextModel },
  { when: () => true, use: defaultModel },
]);
```

`twoSpeedLLM` is the cost-optimizing preset: a cheap/fast model for synthesis steps, a capable one for reasoning. It is `routerLLM([{ when: isToolResultStep, use: fast }, { when: () => true, use: capable }])`:

```ts
import { twoSpeedLLM } from "@promin/agent";

const llm = twoSpeedLLM({
  capable: anthropic("claude-opus-4-7", { apiKey }),
  fast: anthropic("claude-haiku-4-5-20251001", { apiKey }),
  // Optional override (true → fast). Default: isToolResultStep — the last
  // non-system message is a tool result.
  when: (p) => false,
});
```

### resilientLLM

Retry transient errors with backoff, then fail over along a fallback chain:

```ts
import { resilientLLM, ALL_LLM_ERROR_CLASSES } from "@promin/agent";

// Retry the primary; on quota exhaustion or context overflow switch to the backup.
const llm = resilientLLM(primary, { fallback: backup });

// Plain fallback chain: try each provider once, in order, on any error.
const chain = resilientLLM(primary, {
  transientRetries: 0,
  rateLimitRetries: 0,
  fallback: [backup, lastResort],
  fallbackOn: ALL_LLM_ERROR_CLASSES,
});
```

Errors are classified by `classifyLLMError`: 429 is `rate_limit`; 408, 409, 5xx (incl. Anthropic's 529), timeouts and interrupted streams are `transient`; 413 and "prompt too long" 4xx are `context_overflow`; other 4xx are `permanent`. A retry waits the server's `Retry-After` when it sent one (up to `maxRetryAfterMs`, default 60s — longer and the call fails over or throws), otherwise `backoffMs`. `fallbackOn` (default `["rate_limit", "context_overflow"]`) lists the error classes that move the call to the next provider; each fallback starts with a fresh retry budget. A stream that already yielded chunks is not replayed unless `retryAfterPartial` is set, since the caller would see duplicate output.

### rotatingLLM

Distribute load across N providers (typically multiple API keys for the same model) and transparently failover on 429:

```ts
import { rotatingLLM, InMemoryCapacityStore } from "@promin/agent";

const llm = rotatingLLM([
  anthropic("claude-sonnet-4-6", { apiKey: keyA }),
  anthropic("claude-sonnet-4-6", { apiKey: keyB }),
  anthropic("claude-sonnet-4-6", { apiKey: keyC }),
]);
```

Selection: when slots report `LLMResponse.rateLimitHint.remainingTokens` (Anthropic does), `rotatingLLM` picks the slot with the most headroom. Otherwise it round-robins. Set `strategy: "round-robin"` to force it.

Failover: a 429 marks the slot exhausted in a `CapacityStore` (using the response's `Retry-After` header) and the call retries against the next available slot. Near-zero `remainingTokens` mark the slot proactively, before the next 429 fires (`exhaustionTokenThreshold`, default `1000`).

Multi-replica: pass a shared `CapacityStore` (Redis / Postgres) so a 429 hit by one process is honoured by every other process — no thundering-herd of 429s after the first slot exhausts. The default `InMemoryCapacityStore` is single-process.

```ts
const llm = rotatingLLM(slots, {
  strategy: "round-robin",
  capacityStore: redisCapacityStore,
  exhaustionTokenThreshold: 5_000,
});
```

### ModelCatalog

When agents are authored as data — recipes with `backend.model = { provider, id }` — the host needs a way to translate that string pair to a live `LLMProvider`. `ModelCatalog` is that bridge:

```ts
import { InMemoryModelCatalog, anthropic } from "@promin/agent";

const catalog = new InMemoryModelCatalog([
  {
    provider: "anthropic",
    id: "claude-sonnet-4-6",
    displayName: "Claude Sonnet 4.6",
    contextLimit: 200_000,
    capabilities: ["chat", "tools", "vision", "thinking"],
    costTier: "mid",
    llm: anthropic("claude-sonnet-4-6"),
  },
]);

resolveLocalAgent(recipe, {
  // ...
  llm: (provider, id) => catalog.get(provider, id)?.llm ?? fallback,
});
```

The runtime-only `llm` field is stripped by `catalog.serialize()` so the catalog can be safely returned over HTTP — Zorya exposes `GET /api/agents/_catalog/models` to populate the designer UI's model dropdown without leaking provider closures or API keys.

For folder-driven discovery (one file per model, hot-reload on change), use `createFileModelCatalog({ dir })` — same shape and ergonomics as `createFileToolRegistry`.

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

For building interactive console applications. The Node TTY kit lives on its own subpath so server and library consumers don't load it:

```ts
import { Terminal, ChatTerminal, ConsoleRunner, MarkdownRenderer } from "@promin/agent/terminal";
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
