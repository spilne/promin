# @promin/agent

Durable agent turns, LLM adapters, tools, memory, retrieval, and a terminal UI for building interactive AI applications.

---

## Table of contents

- [Core concepts](#core-concepts)
- [LocalAgent — conversations](#localagent--conversations)
- [agentAction — the durable turn engine](#agentaction--the-durable-turn-engine)
  - [Engine contract](#engine-contract)
- [Tools](#tools)
  - [tool()](#tool)
  - [Multi-command tools](#multi-command-tools)
  - [Built-in tools](#built-in-tools)
  - [MCP servers](#mcp-servers)
  - [Sub-agents](#sub-agents)
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
- [Turn events](#turn-events)

---

## Core concepts

| Concept          | What it is                                                                            |
| ---------------- | ------------------------------------------------------------------------------------- |
| `LocalAgent`     | An agent: one-shot `invoke` / `stream`, and conversational threads.                   |
| `AgentThread`    | A conversation: each `send` / `stream` is one durable turn over the thread's history. |
| `agentAction`    | The durable turn engine: one journaled agent turn (LocalAgent runs every turn on it). |
| `AgentTool`      | A Zod-validated tool the LLM can call.                                                |
| `LLMProvider`    | Uniform interface across Anthropic, OpenAI, Gemini, Ollama, etc.                      |
| `MemoryStore`    | Cross-session long-term memory.                                                       |
| `WorkflowRunner` | Durable execution engine from `@promin/workflow` — makes agents crash-safe.           |

---

## LocalAgent — conversations

Use a `LocalAgent` thread for interactive chat where the user sends many messages in sequence. Every turn runs on `agentAction` (below); the thread's history lives in the `MemoryStore` (or in memory when none is configured).

```ts
import { LocalAgent, InMemoryMemoryStore, anthropic } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";

const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });

const agent = new LocalAgent({
  agent: {
    name: "my-agent",
    llm: anthropic("claude-sonnet-4-6", { apiKey }),
    tools: { search: webSearchTool },
    systemPrompt: "You are a helpful assistant.",
    compaction: { maxTokens: 120_000 }, // in-turn compaction, see below
  },
  runner,
  memory: new InMemoryMemoryStore(),
  namespaceId: "acme",
});

const thread = await agent.thread("t1", { resourceId: "user-1" });

// Streaming
const run = thread.stream({ task: "What's the weather in Tokyo?" });
for await (const chunk of run.textStream) process.stdout.write(chunk);

// Non-streaming
const answer = await (await thread.send({ task: "Summarise that." })).text;
```

### AgentThread API

| Method                                   | Description                                                              |
| ---------------------------------------- | ------------------------------------------------------------------------ |
| `send(input, opts?)`                     | Run a turn; every result promise is already resolved.                    |
| `stream(input, opts?)`                   | Run a turn; `textStream` / `fullStream` are live, `cancel()` stops it.   |
| `resume(callId, decision)`               | Approve or reject a tool call the turn suspended on (`requireApproval`). |
| `pendingApprovals()`                     | The thread's open approvals.                                             |
| `messages(range?)`                       | The persisted transcript.                                                |
| `workingMemory()` / `setWorkingMemory()` | The thread's working-memory note.                                        |
| `delete()`                               | Delete the thread, its messages and its approvals.                       |

`agent.compactThread(id)` / `agent.distillThread(id)` roll a thread into episodes (`autoCompact` / `autoDistill` run them after turns). See `packages/agent/src/examples/console-chat.ts` for a REPL built on a thread.

### Side effects in hooks

Every agent hook (`beforeTurn`, `afterTurn`, `onLifecycle`, `onApprovalRequired`, `onStep`, `processors.beforeLLM` / `afterLLM`) runs inside a journaled activity. The journal short-circuits the callback on replay — the activity's recorded return value is read directly without re-invoking the hook. A worker restart after a hook finished won't re-prompt the user for approval, won't double-write to your audit log, won't double-bump a rate-limit counter.

A crash _while_ a hook runs is the exception: the hook's activity is idempotent, so recovery runs it again rather than halting the turn. Hooks (and the events and lifecycle callbacks the engine emits) are therefore at-least-once across a crash — make them idempotent.

`isReplay` on a hook's context is `true` when the workflow body containing the activity has been re-started (worker restart, signal-resume) — useful when you want side effects keyed to body restarts, not needed for replay safety.

For durable audit trails of approval decisions, read the journaled decision back from workflow storage instead of relying on `onApprovalRequired` side effects — the journal survives replay correctly.

---

## agentAction — the durable turn engine

`agentAction` builds one agent turn as a durable workflow: think → (approve) → tools → think … until the model answers. `LocalAgent` runs every turn (`invoke`, `stream`, `thread.send` / `stream` / `resume`) through it, so this is the one engine and the contract below holds everywhere.

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

const { output, finishReason } = await runner.run({
  workflow: classifier,
  workflowId: "c1",
  input: { task: "I love this product!" },
});
// output: { sentiment: "positive", confidence: 0.97 }, finishReason: "stop"
```

### Engine contract

**Journal.** Each side effect is one activity: `turn-start`, `transcript-open` (with a `transcript`), `before-turn`, then per step `plan-<n>` (tool-set snapshot and compaction cut), `compact-<n>` (the step summary, when compacting), `think-<n>` (the LLM call), `step-<n>-hook` (`onStep`), `step-<n>-approvals` (the `autoApprove` verdicts), `step-<n>-approval-<id>-start` / `-end` around the decision, `tool-<name>-<n>-<id>` (each tool call, in parallel), `stall-<n>`; then `after-turn`, `transcript-commit` (with a `transcript`) and `turn-end` (or `turn-failed` when the turn throws). Control flow between activities reads only the input and journaled values, so a replay takes the recorded path — a tool or `autoApprove` policy edited while a turn is suspended doesn't change it. Time comes from the injected `clock` (a `WallClock`), inside activities only. Every activity except a non-idempotent tool call is idempotent: a crash inside one re-runs it on recovery.

**Events.** Every bus event (`turn.start`, `llm.call`, `llm.retry`, `token.delta`, `tool.start` / `tool.progress` / `tool.end`, `compact`, `compaction.ineffective`, `approval.requested` / `approval.decision`, `stall.detected`, `step_limit.hit`, `turn.end` / `turn.aborted` / `turn.failed`) is emitted by the activity that produced it, so a replay never re-emits an event for a recorded activity. An approval resume streams only what happens after the resume. `approval.requested` carries the tool name and input; tool events carry the `toolCallId`.

**LLM calls** are idempotent activities: a crash mid-call re-runs the call on recovery. Failures are classified with `classifyLLMError`: `transient` and `rate_limit` are retried per `llmRetry` (exponential backoff, or the server's `Retry-After` up to `maxRetryAfterMs`); `context_overflow` and `permanent` fail the turn. If the provider is already a `resilientLLM`, set `llmRetry: false`.

**Tool calls run once.** A crash between starting a tool and recording its result leaves the outcome unknown:

- a tool with `idempotent: true` runs again with the same `ctx.toolCallId` — use it as the idempotency key you pass downstream (payment `Idempotency-Key`, upsert key, message dedupe id);
- any other tool is not re-run: the model gets an error result saying the outcome is unknown and should be checked before retrying.

Every tool gets `ctx.toolCallId`, `ctx.signal` (aborted on cancel or timeout) and `ctx.timeoutMs` (from the tool's `timeoutMs`, else the agent's `toolTimeoutMs`). Tool failures come back as results with `isError: true` (invalid input, a throw, a timeout, a cancellation, a rejected approval, an unknown tool); Anthropic receives the flag as `is_error`.

**Tool-set snapshot.** Tools are re-read every step (a `toolRegistry` hot-reloads), and each step journals the names, `requireApproval` and `idempotent` it saw, so a tool edited while a turn is suspended doesn't change the path the replay takes.

**Approvals.** A `requireApproval` tool not covered by `autoApprove` asks `onApprovalRequired` (journaled) or suspends on the `approve:<toolCallId>` signal (`ctx.approval`). Before it suspends, the `-start` activity records the request in the run's `approvalIndex` (a `PendingApprovalStore` for thread turns) and carries the tool name and input, which `listPendingSignals` reads.

**Ending a turn.** `result.finishReason` says why the turn ended, and the transcript always ends with an assistant message and has a result for every tool call. `result.turnMessages` is what the turn adds to a conversation (the task, then the assistant and tool messages it produced — no seed, nothing a `beforeTurn` hook did, no engine guidance); `LocalAgent` persists exactly that, through the run's `transcript`: `transcript-open` records the user message when the turn starts and `transcript-commit` records the produced messages once it has its answer, each an idempotent append keyed by the workflow id. A turn that throws emits `turn.failed` and an `onLifecycle` `failed` transition.

| `finishReason` | When                                                        | `answer`                               |
| -------------- | ----------------------------------------------------------- | -------------------------------------- |
| `stop`         | the model answered (or `onStep` returned `continue: false`) | the model's text                       |
| `max_steps`    | `maxSteps` think steps ran without an answer                | `STEP_LIMIT_ANSWER`                    |
| `stalled`      | the stall detector forced a final answer                    | the forced answer, or `STALLED_ANSWER` |
| `cancelled`    | `signal` aborted                                            | streamed text, or `CANCELLED_ANSWER`   |

**Cancellation.** `signal` is threaded into every LLM call and tool (and ends an LLM retry wait). The engine records the cancellation at the next think step, so a replay ends the turn at the same point.

**Stall detection.** `stall` (default on) watches for rounds that repeat earlier tool calls with no new text (`maxRepeatRounds`, default 3) and for one tool called more than `maxCallsPerTool` (default 15) times; either forces one more round with `tool_choice: "none"` (or a forced `_respond` with `outputSchema`).

**Structured output.** With `outputSchema`, the engine offers a `_respond` tool and forces it through `tool_choice` once the model is ready to answer (from the first step when the agent has no other tools, after a prose answer otherwise — the forced round gets a user nudge, so a request never ends on the assistant's prose; extended thinking is off for a forced round, which Anthropic requires). Other tool calls in a `_respond` response are answered as not run. A payload that fails the schema goes back to the model with the validation errors, up to `structuredOutputRepairs` (default 2) times, then the turn fails with `StructuredOutputParseError`; the repair round keeps the agent's tools available, so the model can re-issue a skipped call.

**Settings.** `thinkingBudgetTokens`, `maxTokens`, `redactPriorThinking`, `compaction`, `metrics` (+ `costs`, `llmProvider`, `llmModel`, `metricLabels`: `llm.*`, `tool.*`, `approval.*`), `hooks.beforeTurn` / `hooks.afterTurn`, `onLifecycle`, `onStep` (with `feedback`), `onToolCall`, `onToolResult`, `onChunk`, `onThinking`, `processors`, `rateLimiter`, `scope`, `toolAuditLogger`. `LocalAgent`'s `agent` field is `AgentEngineConfig` — this config minus the per-run `bus` / `scope` / `signal` / `transcript` / `approvalIndex` it supplies — and every field reaches every turn.

### Thread turns

`LocalAgent` runs each `thread.send` / `stream` as one durable unit:

- **Deterministic turn id.** The workflow id is `opts.runId` when given, else `threadTurnWorkflowId({ namespaceId, threadId, turnKey })` with `opts.turnKey` (a client's idempotency key; random when omitted). A send retried with the same key reaches the same workflow: a finished turn answers from storage, an interrupted one continues from its journal — the model and tools don't run again. The key names one task: reusing it (or a `runId`) for a different task throws `TurnKeyConflictError`. A failed turn answers its failure; send again with a new key.
- **History in the journal.** The user message is appended by `transcript-open` and the turn's output by `transcript-commit`, both with `MemoryStore.appendMessages(…, { idempotencyKey })`, so a crash — after the turn's output was written, or on recovery by another worker — never loses or duplicates history. A failed or suspended turn has still recorded its user message.
- **Pending-approval index.** A turn that suspends on an approval records `{ namespaceId, threadId, toolCallId } → { workflowId, toolName, input, requestedAt, expiresAt }` in `LocalAgentConfig.approvals` (a `PendingApprovalStore`: `InMemoryPendingApprovalStore`, `SqlitePendingApprovalStore`, `PostgresPendingApprovalStore`, all passing `pendingApprovalStoreTestSuite`) just before it suspends. A row stays _open_ until its decision reached the turn (`deliveredAt`). `thread.pendingApprovals()` lists the thread's open rows; `thread.resume(callId, { approved, reason, by })` looks the turn up by exact key, records the decision with its approver (`by`) atomically — a second, different decision throws `ApprovalAlreadyDecidedError` — then delivers it, continues the turn and closes the row. A decision that arrives before the turn has suspended waits for the suspension (then `ApprovalNotReadyError`, retryable); a resume interrupted after recording its decision is finished by resuming again with the same decision. With `approvalTtlMs`, an expired approval cannot be approved (`ApprovalExpiredError`); rejecting it, or the next send on the thread, closes it as `expired` and finishes its turn with the rejection. The default store is in-memory and shared by agents over the same runner (with durable workflow storage it warns); a host with durable storage passes a durable one (`resolveLocalAgent({ approvals })`).
- **Turn trace.** Every `agentAction` turn returns `AgentResult.trace` (a `TurnTrace`): think, approval and tool spans with start times and durations on the engine clock, per-think token usage, the turn's usage, step count, duration and finish reason. Span times are journaled with the step they time, so a turn resumed after an approval or recovered after a crash reports the original timings (a sub-agent call's span is untimed: its child run has its own trace). The committed final assistant message carries it under `metadata[TURN_TRACE_META_KEY]`, and `buildAgentTrace` puts it on the turn as `timing`.
- **Ordering.** A new send on a thread with an open approval whose turn has not ended throws `ThreadAwaitingApprovalError` — including a turn that is still on its way to the suspension — so the turn's output lands right after its own user message. Decide the approval first. `thread.prepareTurn()` runs this check alone (a gateway calls it before it streams); approvals whose turn ended out of band are closed instead of blocking.

Run output: `steps`, `toolCalls` and `toolResults` (with tool names and `failed`) cover this turn only; `fullStream` and `textStream` are push-based and every consumer reads them from the start; `cancel()` aborts the turn and resolves once it has stopped — a tool that ignores `ctx.signal` keeps the turn running until it returns.

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

`execute(input, ctx)` gets `ctx.toolCallId`, `ctx.signal` and `ctx.timeoutMs` from the engine (see [Engine contract](#engine-contract)). Mark a tool `idempotent: true` when running it twice for the same `toolCallId` is safe, and give it a `timeoutMs` to bound one call.

Elevated tools (`createElevatedTool`) must `await ctx.audit({ action, target })` before acting: it writes an `intent` record to the `ToolAuditLogger` at once, and the factory writes a `completed` / `failed` record when the body settles.

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
| `subAgentTool()`                | Delegate to a sub-agent; each call runs as a child workflow (see [Sub-agents](#sub-agents))           |
| `createFetchUrlTool()`          | Fetch a public http(s) URL (SSRF policy, pinned DNS, size cap, approval)                              |
| `createWebSearchTool()`         | Web search                                                                                            |
| `createDurableSchedulerTools()` | `schedulerCreate` (approval, min interval, atomic per-thread cap), `schedulerList`, `schedulerCancel` |
| `createWriteToolTool()`         | Let the agent write and hot-load new tools at runtime                                                 |

### MCP servers

Tools from [Model Context Protocol](https://modelcontextprotocol.io) servers
become ordinary `AgentTool`s. The operator defines the servers
(`McpServerConfig`: a stdio `command` / `args` / `env` / `cwd`, or a Streamable
HTTP `url`); a recipe attaches them by name or names an http endpoint, never a
command:

```ts
import {
  McpClientPool,
  loadMcpToolsFromRecipe,
  resolveLocalAgent,
  type McpServerConfig,
} from "@promin/agent";

const servers: McpServerConfig[] = [
  {
    name: "github",
    transport: "stdio",
    command: "github-mcp-server",
    args: ["stdio"],
    credentialRef: "GITHUB_TOKEN", // SecretsStorage key, resolved per tenant
    credentialEnv: "GITHUB_PERSONAL_ACCESS_TOKEN",
    tools: ["search_issues", "get_issue"], // optional narrowing
  },
];
const pool = new McpClientPool(); // shared; closeAll() on shutdown

// recipe.backend.mcpServers = [{ server: "github" }]
const mcpTools = await loadMcpToolsFromRecipe({ recipe, servers, pool, secrets, scope });
const agent = await resolveLocalAgent(recipe, { ...deps, mcpTools });
```

- **Names**: `<server>__<tool>`, valid for every provider
  (`^[a-zA-Z0-9_-]{1,64}$`); a name with other characters or over 64 gets a
  hash suffix. `tool.mcp` keeps the server and original MCP tool name.
- **Schemas**: the server's `inputSchema` is advertised as-is
  (`AgentTool.jsonSchema`) and input is validated against it with Ajv (the MCP
  SDK's validator) before the call — with a private Ajv instance per server
  and tenant, on a copy that keeps only structural keywords (types, bounds,
  `required`, `enum`/`const`, subschemas). `pattern`, `patternProperties` and
  `format` compile to backtracking regexes a hostile server could use to
  block the event loop, and no linear-time regex engine is available without
  a native build, so the server validates them itself; `$id`, `default`,
  `examples`, `x-*` and unknown keywords are dropped too. A `$ref` must be a
  JSON pointer that walks only through subschema keywords; any other `$ref`
  (external, or into data such as `#/default`) turns validation off for that
  tool. Output schemas are never compiled (agents read content blocks, not
  `structuredContent`).
- **Trust**: MCP tools require approval and are not idempotent unless the
  operator sets `requireApproval: false` / `idempotent: true` on the server.
  Tool names over 128 characters or with control characters are skipped;
  names and descriptions shown to the model are flattened, capped (description
  1 024 characters) and labelled; results reach the model fenced as untrusted
  reference data. A server exposes at most `maxTools` tools (default 128) and
  256 KB of schema; `tools/list` stops after 16 pages / 1 024 tools and runs
  under one `timeoutMs` deadline.
- **Pool**: clients are keyed by a hash of the server config, the tenant
  namespace and the resolved credential, so two tenants (or two configs that
  share a name) never share a connection, a process, a token or a validator;
  a new credential for the same server, namespace and credential scope (the
  resource or namespace it was found at) retires the old client, and tools
  loaded with the old credential fail with `McpCredentialRotatedError`
  instead of reconnecting with it. A client whose transport closed (a stdio
  server exited) or whose request failed below the protocol is replaced on
  the next call; for http servers a timed-out or aborted request does the
  same, which is what closes its response stream. Each call honors
  `timeoutMs` (default 60 s) and the engine's abort signal (the server is told
  to cancel); at most `maxConcurrentCalls` (default 4) run per client. Idle
  clients over `maxClients` are closed; after `closeAll()` the pool refuses
  new work.
- **stdio**: the server gets the MCP SDK's safe environment (`PATH`, `HOME`,
  …) plus its `env`, never the host's own; it runs in its own process group,
  which is killed when the client closes; its stderr goes to `onServerLog`.
- **HTTP**: every request (the POSTs and the SSE stream) goes through
  `createOutboundFetch`: public addresses only, pinned DNS, no redirects,
  responses capped at `maxResponseBytes` (8 MB), unless the operator sets
  `allowPrivateNetwork` on that server or passes an `outboundPolicy`.
- **Recipe endpoints**: `{ name, transport: "http", url, tools? }` entries are
  refused unless the host passes `recipeHttpServers: true`; they always
  require approval, expose at most 32 tools and never get a credential (a
  recipe isn't owned by the tenants that invoke it, so its URL must not
  receive their secrets).
- `loadMcpToolsFromRecipe` needs the caller's `scope`. `resolveLocalAgent`
  throws when a recipe attaches MCP servers but `deps.mcpTools` was not
  passed. `DefaultAgentToolCatalog({ mcp })` lists the servers' tools and
  reports the ones it could not list in `errors` as a code (the detail goes to
  `onError`).

### Sub-agents

`subAgentTool()` is the one way an agent hands a task to another agent. Each
call runs the sub-agent's turn as a journaled child workflow of the calling
turn, with id `<caller workflow id>.<step>.<toolCallId>` and
`parentWorkflowId` set, so the runs list shows it under its caller (the engines
give tool calls without an id, or with a repeated one, a deterministic id). A
crash or replay of the caller re-attaches to the same child run instead of
starting a second one, and the child's own journal keeps finished steps from
running again. Cancelling the caller's turn or running past `timeoutMs` (counted
from the call's journaled start, suspended time included) stops the child; a
failed, refused or cancelled child comes back to the caller's model as a tool
error. Depth and the call chain travel in the child's workflow input (a
malformed frame fails the run): a call deeper than `maxDepth` (default 3) or
back into an agent already waiting on the chain is refused.

A sub-agent tool that needs approval is carried up to the root turn: in a
`LocalAgent` thread the approval is indexed under the thread with the id
`<child workflow id>:<toolCallId>`, shows in `thread.pendingApprovals()`, blocks
new turns, and its events reach the root turn's stream. `thread.resume(id)`
delivers the decision to the child and re-runs the root turn, which re-attaches
the child; the root commits its transcript once. Approvals are judged by the
root turn: a root that ended out of band closes them and cancels its orphaned
sub-agent runs.

A routed sub-agent's `load` throws `ChildWorkflowGoneError` when the callee is
gone for good (version removed, backend changed, disabled): the call fails and
an in-flight child run is cancelled. Any other `load` error is transient: the
caller parks and re-attaches later.

```ts
const researcher = subAgentTool({
  name: "researcher",
  description: "Research a question with web search and return a short report.",
  agent: { name: "researcher", llm, tools: { search }, systemPrompt: "You research." },
  timeoutMs: 120_000,
});
const lead = agentAction({ name: "lead", llm, tools: { researcher } });
```

`agent` is an `agentAction` config or an `Agent` (`LocalAgent` runs its turn as
the child; other agents are invoked inside it). A routed sub-agent picks its
callee per call with `route` / `load`: the agents network's `callAgent`
(recipes with `backend.network`) is one, resolving peers through the host's
async, scope-aware resolver. The tool runs only inside a durable turn
(`agentAction`, `LocalAgent`).

### Versioned registries

Agents, roles, skills, DAGs and prompt fragments all live in one kind of
store, `VersionedRegistry<TBody, TFilter>`: `InMemoryVersionedRegistry`
here, `SqliteVersionedRegistry` (`@promin/sqlite`) and
`PostgresVersionedRegistry` (`@promin/postgres`), each bound to a kind
(`AGENT_KIND`, `ROLE_KIND`, `SKILL_KIND`, `DAG_KIND`, `FRAGMENT_KIND`; the
`*AgentRegistry` / `*RoleRegistry` / … classes bind it for you). All pass
`versionedRegistryTestSuite` from `@promin/agent/testing`.

- **Immutable versions.** `register({ id, ...body })` stores a version;
  without `version` it reuses the version holding the same body or mints
  the next `v<n>`. Re-registering an existing version with another body is a
  `RegistryConflictError` (`version_conflict`). A label is never rebound:
  `unregister` leaves a tombstone, the sequence never restarts, and only the
  same body can bring a removed version back — so a pin like `tone@v1`
  never resolves to other text.
- **Active pointer.** The first version is active; later ones are drafts
  until `promote({ id, version })` (or `register({ promote: true })`).
  `get({ id })` answers the active version, `get({ id, version })` any live
  one, `versions({ id })` all live ones in registration order. Removing the
  active version activates `unregister({ promote })`, else the version that
  was active before it (never just the latest draft), else the latest one.
- **Compare-and-set.** Writes take `expectedActive` (the active version the
  caller read, `null` for "id must not exist"); a stale one is an
  `active_mismatch` conflict and writes nothing.
- **State.** `enabled` is the id's kill switch (`setEnabled`, or
  `register({ enabled })` in the same write as a version) — held for
  invocations, DAG nodes, the agents network, scheduled turns, queued
  webhook deliveries, role refs, skills and fragments. `trust` is a
  version's review state (`setTrust`) — `needs-review` skills and fragments
  never reach a model.
- **Listing.** `list({ filter, enabled, source, limit, cursor })` answers one
  active entry per id in id order, filtered in the store before the page is
  cut, with a `nextCursor`. `source: "file"` lists the file-managed ids.
- **Concurrency.** SQLite writes run in `BEGIN IMMEDIATE` transactions
  (several processes on one file take turns; set `busy_timeout`); Postgres
  writes take a per-id advisory lock.

```ts
const roles = new InMemoryRoleRegistry();
const v1 = await roles.register({
  id: "reviewer",
  definition: { systemPrompt: "Review.", tools: [] },
});
const v2 = await roles.register({
  id: "reviewer",
  definition: { systemPrompt: "Review carefully.", tools: [] },
  expectedActive: v1.version, // someone else's edit since → RegistryConflictError
});
await roles.promote({ id: "reviewer", version: v2.version });
```

A role layers fragments by id (`systemPrompt.layers: ["tone"]`, following the
fragment's active version) or pins one (`"tone@v2"`). Pinning a role version
(`role: { ref: { id, version } }`) pins its definition, not the fragments it
layers by bare id: a role version renders the same prompt forever only when
its layers are pinned too (the dashboard's layer picker pins with one
click). Skills pin the same way through `skills: [{ id, version }]`. The folder scanners
(`startAgentScanLoop`, `startSkillScanLoop`, `startFragmentScanLoop`, one
shared `startScanLoop`) register a changed file as a new version with
`source: "file"` and leave unchanged files alone. A changed file becomes
active when the active version is the file's own; when an operator made
another version active, the file's version waits as a draft (a scan never
undoes an operator's promote). With `sync` they remove only versions they
wrote — never an operator's — and nothing after a scan that could not read
every file (an empty, unparsable or misnamed file is a scan error).

Agentic DAGs (`createDagWorkflow`) run each node the same way, as a child run
`<dag run>.node-<id>`. A run names a registered DAG (`{ dagId, version,
initialInput, scope }`) and reads the recipe from the `DagRegistry` in its first
journaled step. A node starts as soon as its own upstream nodes have settled; a
node failing with `onError: "abort"` stops the running nodes and starts no new
ones. Node turns never suspend on approvals: an approval-gated tool call is
refused with "requires approval; not available in DAG runs". Register the DAG workflow once at boot under its fixed name
(`DAG_WORKFLOW_NAME`) so recovery resumes a run after a restart.

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
const agent = await resolveLocalAgent(recipe, { runner, llm, tools, retrievers });
```

`retrieverRegistryTestSuite` (from `@promin/agent/testing`) checks a custom `RetrieverRegistry`.

---

## Context compaction

**In a turn.** A long turn (many tool steps) grows the messages sent on every think step. With `compaction: { maxTokens }` on the engine config, when the next step is estimated over `maxTokens` — the larger of a chars/4 estimate (`estimateTokens` to replace it) and the provider's reported usage of the previous step — the turn's older steps are folded into one summary:

- The seed (system prompt, history, reference data, the user task) is never compacted, and the most recent `keepRecentSteps` (default 2) tool steps stay verbatim. A step is an assistant message with its tool calls plus their results, so no tool call ever loses its result.
- The summary is one fenced reference-data message (`source="turn-summary"`, user role, never `system`) right after the task. A later compaction folds the previous summary into the new one, so there is always at most one.
- A compaction folds the fewest old steps that bring the estimate (with the summary at its cap) down to `targetRatio * maxTokens` (default 0.6), so the next one is several steps away.
- The summary request is plain text — the steps fenced as reference data, no tools, no tool blocks — sent to `compaction.llm` (default: the agent's `llm`) with `compaction.prompt` (default `TURN_SUMMARY_PROMPT`), retried per `llmRetry`. With `summarize: false`, or when the call still fails, a deterministic digest of the calls and their results is used. Either is capped at `summaryMaxTokens * 4` characters (default 15% of `maxTokens`, 64–1024 tokens), keeping the most recent lines.
- The summary call counts like a think: in `result.usage`, as an `llm.call` event with `purpose: "compaction"`, in the `llm.*` metrics (label `purpose=compaction`) and as a `compaction` span of the turn's trace.
- Nothing fires while there is nothing to compact: until a step ages out of the kept window, an over-budget turn goes on uncompacted. When a compaction can't bring the step under `maxTokens` (the seed or the kept steps alone are too big), the turn emits `compaction.ineffective` once and compacts again only every few steps.
- `AgentResult.messages`, `onStep`, `afterTurn` and a LocalAgent run's `messages` see the compacted list; `turnMessages`, `toolCalls` / `toolResults` and the thread transcript see every step.
- The config is validated when the turn is built (finite positive numbers, `targetRatio` ≤ 1, `keepRecentSteps` an integer ≥ 1).
- The cut is journaled in `plan-<n>` and the summary in `compact-<n>`, so a replay rebuilds the same messages without calling the summary LLM again. The turn's transcript (`turnMessages`, what a thread persists) is never compacted.

**Across turns.** A thread turn is seeded from `MemoryStore.resolveContext`, which trims history to `contextBudget.maxMessageTokens`; `autoCompact` / `compactThread` roll older messages into thread episodes.

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

await resolveLocalAgent(recipe, {
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

## Turn events

Pass a `SessionEventBus` as `bus` to `agentAction` (`LocalAgent` wires one per run and turns its events into `fullStream`):

```ts
import { SessionEventBus, agentAction } from "@promin/agent";

const bus = new SessionEventBus();
bus.subscribe((event) => console.log(event.type, event));
const turn = agentAction({ name: "my-agent", llm, bus });
```

Event types: `turn.start`, `turn.end`, `turn.aborted`, `turn.failed`, `llm.call`, `llm.retry`, `token.delta`, `tool.start`, `tool.progress`, `tool.end`, `compact`, `approval.requested`, `approval.decision`, `stall.detected`, `step_limit.hit`.
