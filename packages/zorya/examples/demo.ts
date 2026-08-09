// ---------------------------------------------------------------------------
// Demo server — runs real workflows on an in-memory engine and serves the
// dashboard. Run with:
//   bun run packages/zorya/examples/demo.ts

// What's happening:
// - DefaultWorkflowRunner drives two real workflow definitions
//   (`order`, `payment`) with random-delay async steps and occasional
//   failures.
// - POST /api/runs/trigger/:name via ZoryaServer spawns a run.
// - A background loop triggers random runs every few seconds so the Runs
//   list and stats bar animate on their own.
// - A lightweight poll loop fires due schedules (every second) and calls
//   the same runner — so the Schedules page ticks increment live.
// ---------------------------------------------------------------------------

import { InMemoryWorkerRegistry, RecoveryStrategy, type Workflow } from "@promin/workflow";
import {
  createSqliteZoryaStack,
  LocalWorkflows,
  QueuedWorkflows,
  ZoryaScheduler,
  ZoryaAgents,
  ZoryaSkills,
  ZoryaFragments,
  ZoryaDags,
  createZoryaServerBuilder,
  RegistryBackedWorkersProvider,
  scanAgentsFolder,
  scanWorkflowsFolder,
} from "../src/index.ts";
import { researchSynthesisRecipe } from "./dags/research-synthesis.ts";
import {
  createPostgresDb,
  migrate as migratePostgres,
  PostgresAgentRegistry,
  PostgresSkillRegistry,
  PostgresFragmentStore,
  PostgresMemoryStore,
} from "@promin/postgres";
import {
  anthropic,
  ollama,
  applyDiscoveredAgents,
  createDurableSchedulerTools,
  createFileToolRegistry,
  inProcessSchedulerClient,
  InMemoryFragmentRegistry,
  InMemoryModelCatalog,
  resolveCursorAgent,
  DefaultAgentToolCatalog,
  type FragmentStore,
  resolveCredentialRef,
  resolveLocalAgent,
  resolveSkillCatalog,
  resolveRoleBinding,
  InMemoryRoleRegistry,
  resolveRemoteAgent,
  tool,
  type AgentTool,
  type LLMChatParams,
  type LLMProvider,
  type LLMResponse,
  type LLMStreamChunk,
  type Agent,
  type AgentRegistry,
  type MemoryStore,
  type ModelCatalogItem,
  type RegisteredAgent,
  type SkillRegistry,
} from "@promin/agent";
import { echoLLM } from "@promin/agent/testing";
import { z } from "zod";
import { startApprovalAutoSignaler } from "./demo/approval-auto-signaler.ts";
import { inputFor } from "./demo/input.ts";
import { createDemoLogger } from "./demo/logger.ts";
import { seedSchedules } from "./demo/schedules.ts";
// KB content lives at ./kb/org-knowledge-base.ts; the searchKnowledge +
// getDocument tools that wrap it are exposed under ./tools/ for the
// folder-scan registry.
import { ORG_KNOWLEDGE_BASE } from "./kb/org-knowledge-base.ts";
import path from "node:path";

const logger = createDemoLogger("zorya");

// ---------------------------------------------------------------------------
// Storage + runner
//
// Defaults to a persistent file under ./target so runs, schedules, and
// advertised workflows survive server restarts (and the dev hot-reload
// loop, which restarts the subprocess on .ts changes). Override with:
//   ZORYA_DB=:memory:        bun run zorya     # fresh on every boot
//   ZORYA_DB=./somewhere.db  bun run zorya     # custom path
//
// Set ZORYA_PG_URL to a Postgres connection string to run the agent
// registry + memory store on Postgres instead — boots the demo against
// a shared store so the multi-replica agent path can be exercised
// locally. Workflow storage / scheduler stay on the SQLite db above.
//   ZORYA_PG_URL=postgres://localhost/zorya  bun run zorya

const secretsPassphrase =
  process.env.PROMIN_SECRETS_PASSPHRASE ?? "demo-only-passphrase-change-in-prod";
const stack = createSqliteZoryaStack({
  path: process.env.ZORYA_DB,
  secretsPassphrase,
});
const {
  dbPath,
  storage,
  runner,
  namespaceRegistry,
  schedulerStorage,
  dagRegistry,
  instanceRegistry,
  secretsStorage,
  workflowStarts,
  advertisements,
} = stack;

// Agent registry + memory store — SQLite (shares the db above) by
// default, or Postgres when ZORYA_PG_URL is set. Postgres mode boots
// the demo against a shared store so the multi-replica agent path can
// be exercised locally; `migrate()` brings the PG schema up first.
const pgUrl = process.env.ZORYA_PG_URL;
let agentRegistry: AgentRegistry = stack.agentRegistry;
let skillRegistry: SkillRegistry = stack.skillRegistry;
let fragmentStore: FragmentStore = stack.fragmentStore;
let memoryStore: MemoryStore = stack.memoryStore;
if (pgUrl) {
  const pgDb = createPostgresDb(pgUrl);
  await migratePostgres(pgDb);
  agentRegistry = new PostgresAgentRegistry({ db: pgDb });
  skillRegistry = new PostgresSkillRegistry({ db: pgDb });
  fragmentStore = new PostgresFragmentStore({ db: pgDb });
  memoryStore = new PostgresMemoryStore({ db: pgDb });
  logger.log(
    "[zorya] agent + skill registries + fragment store + memory store → Postgres (ZORYA_PG_URL)",
  );
}
// Prompt-fragment registry — small curated markdown layers role recipes
// compose into their system prompt. The ZoryaFragments service below scans
// examples/fragments/*.md on boot + on a tick, and the manager UI mutates
// the same in-memory registry via /api/fragments CRUD.
const fragmentRegistry = new InMemoryFragmentRegistry();
// Role registry — the behavioral bundles agents bind by ref. Empty at boot;
// operators author roles via /api/roles, or "Save as role" lifts an agent's
// inline role into here. Refs resolve through this at agent-materialize time.
const roleRegistry = new InMemoryRoleRegistry();

// ---------------------------------------------------------------------------
// Agents — auto-discovered from ./agents (mirrors the workflow scanner).
// Each .ts module exporting a `RegisterAgentInput` object gets registered
// under its `id`. Add a new file and restart — no edits here required.
//
// LLM providers stay configured here because they hold runtime state
// (round-robin index, echo turn counter) that should survive across
// requests but not across processes — they're deliberately not part of
// the JSON-serializable recipe shape on disk.

const agentScanRoot = path.join(import.meta.dir, "agents");
const rawAgentScan = await scanAgentsFolder(agentScanRoot, {
  onAgent: (agent, src) =>
    logger.log(`[zorya] discovered agent ${agent.id} (${path.relative(agentScanRoot, src)})`),
});
for (const w of rawAgentScan.warnings) logger.warn(`[zorya] ${w}`);

// Recipes that need a live API key get filtered out when the key is
// missing, so the dashboard only surfaces agents that actually work.
const haveAnthropicKey = !!process.env["ANTHROPIC_API_KEY"];
const agentScan = rawAgentScan;

// `support-bot` rotates through canned replies so multiple turns in a
// thread don't all return the same line. Other bots use templated echo.
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

// Two-phase mock — turn 1 emits a tool call, turn 2 (after the tool result
// arrives in the message history) returns a final answer. Detection is
// "did the previous message come from a tool?" so a single instance handles
// any number of conversational turns.
function toolCallingMockLLM(opts: {
  toolName: string;
  pickInput: (lastUser: string) => unknown;
  finalAnswer: (toolResult: string) => string;
}): LLMProvider {
  let callCounter = 0;
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => {
      const last = params.messages.at(-1);
      if (last?.role === "tool") {
        return {
          content: opts.finalAnswer(last.content),
          finishReason: "stop",
          usage: { inputTokens: 32, outputTokens: 24 },
        };
      }
      callCounter += 1;
      const lastUser = lastUserText(params);
      return {
        content: null,
        toolCalls: [
          {
            id: `tc-${callCounter}`,
            name: opts.toolName,
            input: opts.pickInput(lastUser) as Record<string, unknown>,
          },
        ],
        finishReason: "tool_calls",
        usage: { inputTokens: 28, outputTokens: 12 },
      };
    },
  };
}

function lastUserText(params: LLMChatParams): string {
  for (let i = params.messages.length - 1; i >= 0; i--) {
    const m = params.messages[i]!;
    if (m.role === "user" && typeof m.content === "string") return m.content;
  }
  return "";
}

// Wrap any provider with realistic latency: a "thinking" pause before the
// reply starts, then chunked deltas during the stream so the UI sees a
// natural typing cadence instead of a blob landing in one frame. Falls
// back to chat()-only consumers cleanly — the wrapper still applies the
// pre-delay there so non-streaming routes feel paced too.
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const randInRange = ([lo, hi]: readonly [number, number]) =>
  Math.floor(lo + Math.random() * (hi - lo));

interface NaturalLLMOptions {
  /** Pre-response thinking delay range, in ms. Default `[300, 800]`. */
  preDelayMs?: readonly [number, number];
  /** Per-chunk delay range during streaming, in ms. Default `[20, 60]`. */
  chunkDelayMs?: readonly [number, number];
  /** Approximate characters per streamed chunk. Default `5`. */
  chunkSize?: number;
}

function naturalLLM(provider: LLMProvider, opts: NaturalLLMOptions = {}): LLMProvider {
  const preDelay = opts.preDelayMs ?? ([300, 800] as const);
  const chunkDelay = opts.chunkDelayMs ?? ([20, 60] as const);
  const chunkSize = opts.chunkSize ?? 5;
  return {
    chat: async (params: LLMChatParams): Promise<LLMResponse> => {
      await sleep(randInRange(preDelay));
      return provider.chat(params);
    },
    chatStream: async function* (params: LLMChatParams): AsyncIterable<LLMStreamChunk> {
      await sleep(randInRange(preDelay));
      // Re-use chat() so the underlying provider's per-call state (round-
      // robin index, tool-call counter) advances exactly once per turn,
      // regardless of which path the gateway takes.
      const r = await provider.chat(params);
      const text = r.content ?? "";
      for (let i = 0; i < text.length; i += chunkSize) {
        await sleep(randInRange(chunkDelay));
        yield { delta: text.slice(i, i + chunkSize) };
      }
      yield {
        delta: "",
        finishReason: r.finishReason,
        toolCalls: r.toolCalls,
        usage: r.usage,
        thinkingBlocks: r.thinkingBlocks,
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Tools — auto-discovered from ./tools/ via createFileToolRegistry. Each
// `.ts` file in that folder exports a default tool({...}); the registry
// scans + watches them and the resolver's pickTools narrows per recipe
// (recipe.backend.tools list → which subset this agent gets).
//
// Tools that close over demo state (e.g. listWorkflows reading from
// `storage`) stay inline — the file-scan registry can't inject demo-
// specific deps, so host-supplied closures get merged on top of the
// scanned set below.

// `listWorkflows` — pokes into the demo's actual workflow storage so a
// live agent can answer "what's running on this server right now?". The
// tool closes over the outer `storage` and `workflowsByName` so it sees
// the same view as the dashboard.
const listWorkflowsTool: AgentTool<
  { limit?: number },
  {
    workflows: Array<{ name: string; recentRuns: number; lastStatus: string | null }>;
  }
> = tool({
  name: "listWorkflows",
  description:
    "List the workflow definitions registered on this Zorya server, with a count of recent runs and the most recent status per workflow.",
  parameters: z.object({
    limit: z
      .number()
      .int()
      .positive()
      .max(100)
      .optional()
      .describe("Max workflows to return. Default 25."),
  }),
  execute: async ({ limit = 25 }) => {
    const allRuns = await storage.listWorkflows({ limit: 500 });
    const byName = new Map<string, { count: number; lastStatus: string | null; lastAt: number }>();
    for (const run of allRuns) {
      const entry = byName.get(run.workflowName) ?? { count: 0, lastStatus: null, lastAt: 0 };
      entry.count += 1;
      const startedAt =
        typeof run.startedAt === "number"
          ? run.startedAt
          : run.startedAt
            ? new Date(run.startedAt).getTime()
            : 0;
      if (startedAt > entry.lastAt) {
        entry.lastAt = startedAt;
        entry.lastStatus = run.status;
      }
      byName.set(run.workflowName, entry);
    }
    const workflows = Object.keys(workflowsByName)
      .slice(0, limit)
      .map((name) => {
        const stats = byName.get(name);
        return {
          name,
          recentRuns: stats?.count ?? 0,
          lastStatus: stats?.lastStatus ?? null,
        };
      });
    return { workflows };
  },
});

// Folder-scan registry: each `.ts` file under ./tools/ default-exports a
// tool, the registry hot-reloads them on change, and the resolver's
// pickTools narrows per recipe. host-supplied tools (closures over demo
// state, like listWorkflows) get merged on top in resolveAgent below.
const toolRegistry = await createFileToolRegistry({
  dir: path.join(import.meta.dir, "tools"),
  onLoad: (name) => logger.log(`[zorya] loaded tool ${name}`),
});

// Durable scheduler tool — agents call `scheduler.create(...)` to register
// a cron / interval job that re-fires the agent with `source: scheduled`.
// `getClient` is called per-execute with the live caller scope (read off
// `ctx.scope` populated by the agent runtime), so each schedule row gets
// stamped with the right (namespace, resource, thread, agentId) — no
// LocalAgent surface needed. Pairs with `dispatchAgentSchedule` which
// ZoryaServer auto-installs as the loop's fire override (see server.ts).
const schedulerTools = createDurableSchedulerTools({
  getClient: (scope) => inProcessSchedulerClient({ storage: schedulerStorage, scope }),
});

// Ollama config. Shared by `ollama-bot` (chat) and the consolidator
// (compaction / distillation across every agent).
//
// `OLLAMA_MODEL`: chat default. qwen2.5:3b has the best tool-calling
// discipline at this size — it doesn't fire tools on chitchat ("hello",
// "are you alive?") and doesn't leak JSON tool-call shapes into the
// content field on ambiguous prompts. llama3.2:3b is similar size but
// less disciplined; smaller variants (0.5b / 1.5b) either invent schema
// fields or skip tool calls entirely.
//
// `OLLAMA_CONSOLIDATOR_MODEL`: summarisation has no tool-call requirement
// so a tiny model trades quality for speed. Defaults to the chat model
// so a single `ollama pull` is enough to boot.
const OLLAMA_URL = process.env["OLLAMA_URL"] ?? "http://localhost:11434";
const OLLAMA_MODEL = process.env["OLLAMA_MODEL"] ?? "qwen2.5:3b";
const OLLAMA_CONSOLIDATOR_MODEL = process.env["OLLAMA_CONSOLIDATOR_MODEL"] ?? OLLAMA_MODEL;

// Per-agent LLM map — keyed by recipe id. Built once at boot. A discovered
// agent without an entry here falls through to a default `echoLLM` in the
// resolver, so adding a new agent file under `./agents` works without a
// code change here unless you need a custom provider. Every entry is
// wrapped with `naturalLLM` so the dashboard sees realistic latency +
// streaming cadence instead of a blob arriving in one frame.
const agentLlms: Record<string, LLMProvider> = {
  "echo-bot": naturalLLM(echoLLM({ template: "echo-bot says: I heard '{task}' (turn #{n})" })),
  "support-bot": naturalLLM(
    roundRobinLLM([
      "Thanks for reaching out. Could you tell me what error message you're seeing?",
      "Got it. Can you confirm whether this happens on every request or just some?",
      "Looks like a known caching issue. Try clearing your cookies for our domain — that resolves it for ~80% of cases.",
    ]),
  ),
  "research-bot": naturalLLM(
    echoLLM({
      template:
        "Researching '{task}'... summary (turn #{n}): I found 3 relevant past discussions on this topic.",
    }),
  ),
  "weather-bot": naturalLLM(
    toolCallingMockLLM({
      toolName: "weather",
      // Naive city extraction from the user's question. Falls back to a
      // default so the demo never produces a malformed tool input.
      pickInput: (text) => ({ city: pickCityFromText(text) }),
      finalAnswer: (toolResult) => {
        try {
          const parsed = JSON.parse(toolResult) as { tempF?: number; conditions?: string };
          if (typeof parsed.tempF === "number" && parsed.conditions) {
            return `It's ${parsed.conditions} and ${parsed.tempF}°F right now.`;
          }
        } catch {
          // tool output not JSON — fall through
        }
        return `Got it: ${toolResult}`;
      },
    }),
  ),
  // Live LLMs — bound only when the key is present. Anthropic's adapter
  // streams natively with real network latency, so we DON'T wrap it in
  // naturalLLM (that would pile fake delays on top of real ones).
  ...(haveAnthropicKey
    ? {
        "claude-bot": anthropic("claude-sonnet-4-6"),
        "knowledge-bot": anthropic("claude-sonnet-4-6"),
      }
    : {}),
  // Local LLM via Ollama — assumes `ollama serve` is running and the
  // chosen model has been pulled. Fast tiny model; good enough for the
  // demo and free.
  "ollama-bot": ollama({ model: OLLAMA_MODEL, baseURL: OLLAMA_URL }),
};

// Model catalog — bridge between recipes that pin `backend.model = { provider, id }`
// and a runtime LLMProvider. Populated only with provider/model pairs we
// can actually serve in this boot (live keys present, ollama running).
// The designer UI's model dropdown reads this via `GET /api/agents/_catalog/models`.
const catalogItems: ModelCatalogItem[] = [
  {
    provider: "ollama",
    id: OLLAMA_MODEL,
    displayName: `Ollama · ${OLLAMA_MODEL}`,
    capabilities: ["chat"],
    costTier: "low",
    llm: ollama({ model: OLLAMA_MODEL, baseURL: OLLAMA_URL }),
  },
];
if (haveAnthropicKey) {
  catalogItems.push(
    {
      provider: "anthropic",
      id: "claude-sonnet-4-6",
      displayName: "Claude Sonnet 4.6",
      contextLimit: 200_000,
      capabilities: ["chat", "tools", "vision", "thinking", "stream"],
      costTier: "mid",
      llm: anthropic("claude-sonnet-4-6"),
    },
    {
      provider: "anthropic",
      id: "claude-haiku-4-5-20251001",
      displayName: "Claude Haiku 4.5",
      contextLimit: 200_000,
      capabilities: ["chat", "tools", "stream"],
      costTier: "low",
      llm: anthropic("claude-haiku-4-5-20251001"),
    },
  );
}
const modelCatalog = new InMemoryModelCatalog(catalogItems);

const KNOWN_CITIES = [
  "berlin",
  "paris",
  "london",
  "new york",
  "tokyo",
  "san francisco",
  "kyiv",
  "lisbon",
];
function pickCityFromText(text: string): string {
  const lower = text.toLowerCase();
  for (const c of KNOWN_CITIES) {
    if (lower.includes(c)) return c.replace(/\b\w/g, (ch) => ch.toUpperCase());
  }
  return "Berlin";
}

async function seedAgents() {
  if (agentScan.agents.length === 0) {
    logger.log(`[zorya] no agents discovered under ${agentScanRoot}`);
    return;
  }
  // Upsert by default — keeps operator-edited rows from boot N when
  // recipe code changes for boot N+1. Pass `sync: true` if you want the
  // filesystem to be authoritative (deletes registry rows not in scan).
  const result = await applyDiscoveredAgents(agentRegistry, agentScan.agents);
  if (result.added.length > 0) {
    logger.log(`[zorya] registered new agents: ${result.added.join(", ")}`);
  }
  logger.log(`[zorya] upserted ${result.upserted.length} agent recipe(s)`);
}

// Materialize a recipe into a live Agent. Dispatches on backend type so
// remote recipes are proxied over HTTP without touching the local LLM/memory
// stack. Defined as a named function so the network deps can pass it back in
// for recursive `callAgent` lookups.
//
// The optional `scope` carries the per-request (namespaceId, resourceId)
// for BYOK credential resolution. When the recipe declares
// `model.credentialRef`, the secret is fetched here and forwarded to
// the LLM factory's third arg. Existing recipes without credentialRef
// take the host-default path (pooled API keys via env vars).
async function resolveAgent(
  recipe: RegisteredAgent,
  scope?: { readonly namespaceId?: string; readonly resourceId?: string },
): Promise<Agent> {
  if (recipe.backend.type === "remote") return resolveRemoteAgent(recipe);
  if (recipe.backend.type === "cursor") return resolveCursorAgent(recipe);
  // BYOK: resolve credentialRef from secrets vault before constructing
  // the LLM. Throws clearly when a ref is set but no value exists at
  // any scope; falls through to undefined (host default) when unset.
  const apiKey = await resolveCredentialRef({
    recipe,
    secrets: secretsStorage,
    ...(scope !== undefined && { scope }),
  });
  // Skills: resolve the recipe's catalog (description + whenToUse, pinned
  // versions) from the skill registry. `skip` so a recipe that pins a
  // not-yet-registered skill still resolves — same forgiving stance as
  // onUnknownTool. resolveLocalAgent injects the catalog block into the
  // system prompt and auto-attaches `loadSkill` when both are present.
  // Role binding: resolve it to the behavioral bundle (system prompt,
  // tools, skills, capabilities). `resolveRoleBinding` handles both inline
  // (no I/O) and ref (reads the role registry) bindings. resolveSkillCatalog
  // no longer reads `recipe.backend.skills` — it needs the role's skills
  // passed explicitly, so resolve the role first and hand them over.
  const roleDef = await resolveRoleBinding(recipe.backend.role, { roles: roleRegistry });
  const skillCatalog = await resolveSkillCatalog({
    recipe,
    registry: skillRegistry,
    skills: roleDef.skills ?? [],
    ...(roleDef.capabilities !== undefined && { capabilities: roleDef.capabilities }),
    onMissing: "skip",
  });
  return resolveLocalAgent(recipe, {
    runner,
    memory: memoryStore,
    skills: skillRegistry,
    skillCatalog,
    role: roleDef,
    fragments: fragmentRegistry,
    ...(apiKey !== undefined && { apiKey }),
    // Resolution order:
    //   1. Demo-specific id-keyed mocks (echo / round-robin / tool-calling
    //      placeholders) — kept so the hand-tuned demo agents still drive
    //      deterministic UI snapshots.
    //   2. ModelCatalog by `recipe.backend.model = { provider, id }` —
    //      the path a recipe authored in the designer UI takes.
    //   3. Echo fallback so a recipe pointing at an unknown model still
    //      boots (with an obvious "echo" output) instead of throwing.
    //
    // BYOK note: the third `byokKey` argument arrives when the recipe
    // declares `model.credentialRef` and the secret was resolved from
    // the vault. The demo's catalog doesn't currently rebuild providers
    // with per-tenant keys (single shared anthropic / ollama instance);
    // wiring that into InMemoryModelCatalog is a follow-up. For now
    // BYOK works end-to-end via the `apiKey` plumbing in resolveLocalAgent
    // — the value is captured but not yet swapped into the LLM provider.
    // The agent_secret table + credentialRef flow are still verifiable
    // in tests; this is a demo-side hook for production hosts to wire in.
    llm: (provider: string, id: string, _byokKey?: string) =>
      agentLlms[recipe.id] ?? modelCatalog.get(provider, id)?.llm ?? naturalLLM(echoLLM()),
    // Full registry of tools available; resolver's pickTools narrows by
    // recipe.backend.tools. listWorkflows is added inline because it
    // closes over `storage` + `workflowsByName`.
    tools: {
      ...toolRegistry.getTools(),
      listWorkflows: listWorkflowsTool,
      ...schedulerTools,
    },
    // Compaction + distillation is summarisation work — route it to
    // local Ollama by default (free, private, no API roundtrip). Falls
    // back to Anthropic Haiku if `ZORYA_CONSOLIDATOR=anthropic` is set
    // and the API key is present, or to undefined if neither is
    // available (the chat LLM gets reused as a last resort).
    consolidatorLlm:
      process.env["ZORYA_CONSOLIDATOR"] === "anthropic" && haveAnthropicKey
        ? anthropic("claude-haiku-4-5-20251001")
        : ollama({ model: OLLAMA_CONSOLIDATOR_MODEL, baseURL: OLLAMA_URL }),
    // Auto-fire compactThread after each thread turn once the
    // uncompacted backlog crosses either gate. Demo numbers — low
    // enough that you'll see a rollup episode appear in the
    // inspector after a handful of chat turns. `background` mode
    // keeps the user-visible turn snappy.
    autoCompact: {
      messageThreshold: 12,
      tokenThreshold: 4_000,
      keepRecent: 6,
      mode: "background",
    },
    // Auto-fire distillThread once a thread reaches a sensible
    // "this conversation has substance" length. Blocking mode
    // here so concurrent turns can't both fire while the first
    // distill's LLM call is still in flight.
    autoDistill: {
      messageThreshold: 6,
      mode: "blocking",
    },
    contextBudget: {
      maxMessageTokens: 32_000,
      maxEpisodeTokens: 4_000,
    },
    // Agents-network wiring: gives recipes that declared backend.network
    // access to findAgent + callAgent. Recursive — callees use the same
    // resolver so the chain stays in lockstep with the host's config.
    network: {
      registry: agentRegistry,
      resolve: resolveAgent,
      instanceRegistry,
    },
  });
}

// Demo tenants. Mirrors the UI default in agent-detail.tsx.
const DEMO_NAMESPACE = "acme";
const SEED_FACT_PREFIX = "Org doc available:";

// Seed namespace memory with KB metadata so any agent in the "acme"
// tenant has cross-cutting awareness of the org docs without having to
// call searchKnowledge first. Idempotent: re-running the demo skips
// when seed facts already exist (we look for the SEED_FACT_PREFIX).
async function seedNamespaceMemory() {
  const existing = await memoryStore.listNamespaceFacts(DEMO_NAMESPACE);
  if (existing.some((f) => f.text.startsWith(SEED_FACT_PREFIX))) {
    return;
  }
  await memoryStore.upsertNamespace(DEMO_NAMESPACE, {
    staticRules:
      "This is the Acme org tenant. Agents here can rely on facts under namespace scope as " +
      "company-wide policy, including the catalogue of internal documents listed below.",
    workingMemory:
      "Demo seed. Edit me from the memory inspector — every agent in this namespace will see " +
      "your changes on the next turn.",
  });
  for (const [id, doc] of ORG_KNOWLEDGE_BASE) {
    const tags = doc.tags.join(", ");
    await memoryStore.appendNamespaceFact(
      DEMO_NAMESPACE,
      `${SEED_FACT_PREFIX} ${id} — ${doc.title} (${tags})`,
    );
  }
  logger.log(
    `[zorya] seeded namespace memory for "${DEMO_NAMESPACE}" with ${ORG_KNOWLEDGE_BASE.size} KB doc facts`,
  );
}

// ---------------------------------------------------------------------------
// Workers — register two mock workers so the dashboard's Workers page has
// something to display. The demo runs every workflow in-process via
// `runner.run` (no actual task dispatch over a queue), so these workers
// don't claim any work; they just heartbeat and show up in the registry.
// To see a real worker join, run `examples/worker.ts` in another terminal —
// it connects via /rpc/* and advertises workflows the in-process map
// doesn't know about (hello-world, fan-out-demo).

const workerRegistry = new InMemoryWorkerRegistry();
const MOCK_WORKERS = [
  {
    workerId: "demo-worker-eu-1",
    capabilities: ["any"],
    concurrency: 4,
    metadata: {
      hostname: "eu-1.demo.local",
      runtime: "bun",
      version: "0.4.2",
      // Custom tags go under `labels` so the dashboard can render them
      // as a distinct category (versus capabilities / workflows / namespaces).
      labels: { region: "eu-west", env: "demo" },
    },
  },
  {
    workerId: "demo-worker-us-2",
    capabilities: ["video", "etl"],
    concurrency: 2,
    metadata: {
      hostname: "us-2.demo.local",
      runtime: "bun",
      version: "0.4.2",
      labels: { region: "us-east", env: "demo" },
    },
  },
] as const;

for (const w of MOCK_WORKERS) {
  await workerRegistry.register({
    workerId: w.workerId,
    capabilities: w.capabilities,
    concurrency: w.concurrency,
    metadata: w.metadata,
  });
}
// Heartbeat each mock worker so detectDead() doesn't tip them into the
// "dead" bucket. 5s cadence stays well below typical 30s timeouts and
// keeps the lastHeartbeat field visibly fresh in the dashboard.
const heartbeatHandle = setInterval(() => {
  for (const w of MOCK_WORKERS) {
    void workerRegistry.heartbeat(w.workerId);
  }
}, 5_000);
process.on("SIGINT", () => clearInterval(heartbeatHandle));
process.on("SIGTERM", () => clearInterval(heartbeatHandle));

// Registry so trigger-by-name works.
// Auto-discover workflows by scanning ./workflows. Every .ts module under
// that directory whose exports include a Workflow is registered under its
// `workflow.name`. Add a new file and restart — no edits here needed.
const scanRoot = path.join(import.meta.dir, "workflows");
const scanResult = await scanWorkflowsFolder(scanRoot, {
  onWorkflow: (name, _wf, src) =>
    logger.log(`[zorya] discovered workflow ${name} (${path.relative(scanRoot, src)})`),
});
for (const w of scanResult.warnings) logger.warn(`[zorya] ${w}`);
const workflowsByName: Record<string, Workflow<unknown, unknown>> = scanResult.workflows;

let idCounter = 0;
function nextId(name: string): string {
  idCounter += 1;
  return `${name}-${Date.now().toString(36)}-${idCounter}`;
}

// ---------------------------------------------------------------------------
// Background: seed a handful of runs on startup so the first page has data.
// Ongoing traffic comes from schedules — pausing a schedule actually stops
// its runs (no hidden random loop).

async function seedInitialRuns(
  seedTrigger: (name: string, input: unknown, opts: { namespace?: string }) => Promise<unknown>,
) {
  // Skip when the persistent DB already has runs — keep accumulated
  // history intact across restarts. Schedules still fire on their own
  // cadence so the dashboard stays animated.
  const existing = await storage.listWorkflows({ limit: 1 });
  if (existing.length > 0) {
    logger.log("[zorya] storage already has runs — skipping initial seed");
    return;
  }
  // Seed every workflow once so the dashboard has data on first load,
  // and rotate through a few namespaces so the sidebar's namespace
  // switcher actually has alternates (it would otherwise only see
  // `default`/null on every row).
  const namespaces = ["tenant-a", "tenant-b", undefined];
  let i = 0;
  for (const name of Object.keys(workflowsByName)) {
    const namespace = namespaces[i++ % namespaces.length];
    await seedTrigger(name, inputFor(name), namespace !== undefined ? { namespace } : {});
  }
}

async function seedDemoNamespaces() {
  for (const ns of [
    { id: "default", displayName: "Default", description: "Default Zorya namespace" },
    { id: "tenant-a", displayName: "Tenant A", description: "Demo tenant A" },
    { id: "tenant-b", displayName: "Tenant B", description: "Demo tenant B" },
  ]) {
    const existing = await namespaceRegistry.get(ns.id);
    if (!existing) await namespaceRegistry.create(ns);
  }
}

// ---------------------------------------------------------------------------
// Boot

await seedDemoNamespaces();
await seedSchedules({ schedulerStorage });
await seedAgents();
await seedNamespaceMemory();
const stopApprovalAutoSignaler = startApprovalAutoSignaler({
  storage,
  runner,
  workflowsByName,
  logger,
});

// Sleep scanner + agent scan loop are now owned by the service classes
// (LocalWorkflows + ZoryaAgents). Their start() methods kick the loops
// once server.listen() runs. SIGINT/SIGTERM go through server.stop()
// which calls each service's stop() in turn.

const uiDir = process.env.ZORYA_UI_DIR ?? path.join(import.meta.dir, "..", "dist", "public");

// WorkflowVersionRegistry — drives the Deployments page and the
// promote/findActive routing. Register the three versions of the
// versioned-greeter demo workflow so the UI has something to promote
// between. Coordinator picks the active version via `findActive` when no
// explicit version is supplied to `runner.run({ name })`.
const { WorkflowVersionRegistry } = await import("@promin/workflow");
const versionRegistry = new WorkflowVersionRegistry();
const { versionedGreeterVersions } = await import("./workflows/versioned-greeter.ts");
for (const v of versionedGreeterVersions) versionRegistry.register(v);
logger.log(
  `[zorya] versioned-greeter: registered ${versionedGreeterVersions
    .map((v) => `v${v.version}`)
    .join(", ")} (no active until promoted)`,
);

// The hybrid: local for in-process workflows, queued fallback for any
// workflow only an external worker advertises (e.g. examples/worker.ts).
const workflows = new LocalWorkflows({
  storage,
  runner,
  definitions: workflowsByName,
  versionRegistry,
  recovery: RecoveryStrategy.builder()
    .failStale({ olderThanMs: 60 * 60 * 1000, error: "Stale run auto-failed on restart" })
    .resumeRecent()
    .build(),
  fallback: new QueuedWorkflows({
    storage,
    workflowStarts,
    advertisements,
    acceptAny: true,
  }),
});

// Tool catalog drives GET /api/agents/_catalog/tools (Designer's tool
// multi-select). Aggregates the host's static tools (the same map
// resolveLocalAgent picks from) + the file-discovered registry. MCP
// tool enumeration is opt-in via the catalog's mcp config — left out
// here since the demo doesn't declare MCP servers.
const agentToolCatalog = new DefaultAgentToolCatalog({
  inProcess: {
    listWorkflows: listWorkflowsTool,
    ...schedulerTools,
  },
  file: toolRegistry.getTools(),
});

const agents = new ZoryaAgents({
  registry: agentRegistry,
  resolve: resolveAgent,
  memory: memoryStore,
  instances: instanceRegistry,
  models: modelCatalog,
  toolCatalog: agentToolCatalog,
  fragments: fragmentRegistry,
  roles: roleRegistry,
  scan: {
    root: agentScanRoot,
    intervalMs: 5_000,
    onTick: (tick) => {
      if (tick.added.length > 0) {
        logger.log(`[zorya] hot-reload: registered new agents: ${tick.added.join(", ")}`);
      }
      for (const w of tick.warnings) logger.warn(`[zorya] agent-scan: ${w}`);
    },
  },
});

// Skills service — owns the SkillRegistry that backs /api/skills CRUD, the
// agent editor's skill picker (/api/agents/_catalog/skills), and the
// resolver's catalog injection (see resolveAgent above). Hot-reloads skill
// manifests (.ts modules + SKILL.md) from ./skills on a tick.
const skillScanRoot = path.join(import.meta.dir, "skills");
const skills = new ZoryaSkills({
  registry: skillRegistry,
  scan: {
    root: skillScanRoot,
    intervalMs: 5_000,
    onTick: (tick) => {
      if (tick.added.length > 0) {
        logger.log(`[zorya] hot-reload: registered new skills: ${tick.added.join(", ")}`);
      }
      for (const w of tick.warnings) logger.warn(`[zorya] skill-scan: ${w}`);
    },
  },
});

// Fragments service — scans examples/fragments/*.md on boot + on a tick;
// the manager UI mutates the same registry via /api/fragments CRUD.
const fragmentScanRoot = path.join(import.meta.dir, "fragments");
const fragments = new ZoryaFragments({
  registry: fragmentRegistry,
  store: fragmentStore,
  scan: {
    root: fragmentScanRoot,
    intervalMs: 5_000,
    onTick: (tick) => {
      if (tick.added.length > 0) {
        logger.log(`[zorya] hot-reload: registered new fragments: ${tick.added.join(", ")}`);
      }
      for (const w of tick.warnings) logger.warn(`[zorya] fragment-scan: ${w}`);
    },
  },
});

const scheduler = new ZoryaScheduler({
  storage: schedulerStorage,
  workflows,
  agents,
  namespaces: "all",
  pollIntervalMs: 1_000,
  dispatchConcurrency: 5,
  sampleInput: inputFor,
});

await seedInitialRuns((name, input, opts) => workflows.trigger(name, input, opts));

// ---------------------------------------------------------------------------
// Agentic DAG demo wiring — register 3 specialist recipes (planner /
// researcher / synthesizer) + the diamond-shape research-synthesis DAG
// the recipes power. Each node references one of these agentIds; the
// dag executor resolves them via the same resolveAgent path the chat
// gateway uses, so credentialRefs / tools / memory all flow through.
// ---------------------------------------------------------------------------

const dagAgentRecipes = [
  {
    id: "dag-planner",
    description: "Splits a topic into 3 research sub-questions.",
    systemPrompt:
      "You are a research planner. Given a topic, emit exactly 3 sub-questions (one per line) " +
      "covering different angles: historical, current, contrarian. Be concise — sub-questions only, no preamble.",
  },
  {
    id: "dag-researcher",
    description: "Produces a focused finding for one sub-question.",
    systemPrompt:
      "You are a research specialist. Given a topic and a focus directive, produce a 4-6 sentence " +
      "finding that's factual and concrete. No filler.",
  },
  {
    id: "dag-synthesizer",
    description: "Joins multiple findings into a single report.",
    systemPrompt:
      "You are a senior research editor. Given a topic and multiple research findings, produce a " +
      "single synthesized 8-12 sentence report. Resolve contradictions explicitly.",
  },
] as const;

for (const r of dagAgentRecipes) {
  await agentRegistry.register({
    id: r.id,
    backend: {
      type: "local",
      // Prefer Sonnet for synthesizer (quality), Haiku for planner / researcher
      // (cheap + fast). When ANTHROPIC_API_KEY is missing, fall back to ollama
      // so the demo still runs.
      model: haveAnthropicKey
        ? r.id === "dag-synthesizer"
          ? { provider: "anthropic", id: "claude-sonnet-4-6" }
          : { provider: "anthropic", id: "claude-haiku-4-5-20251001" }
        : { provider: "ollama", id: OLLAMA_MODEL },
      role: {
        inline: {
          systemPrompt: r.systemPrompt,
          tools: [],
        },
      },
    },
    metadata: {
      description: r.description,
      capabilities: ["chat"],
      tags: ["dag-demo"],
    },
  });
}
await dagRegistry.register(researchSynthesisRecipe);

// DAG resolver: agentId → Agent. Reuses the existing agentRegistry +
// resolveAgent so DAG runs share the same credential / tool / memory
// machinery as direct chat invocations.
const dags = new ZoryaDags({
  registry: dagRegistry,
  runner,
  resolveAgent: async (agentId, version) => {
    const recipe = await agentRegistry.get(agentId, version);
    if (!recipe) throw new Error(`DAG node references unknown agent: ${agentId}`);
    return resolveAgent(recipe);
  },
});

logger.log(
  `[zorya] DAG registered: ${researchSynthesisRecipe.id}@${researchSynthesisRecipe.version ?? "v1"} ` +
    `(${researchSynthesisRecipe.nodes.length} nodes, ${researchSynthesisRecipe.edges.length} edges)`,
);

const server = createZoryaServerBuilder()
  .workflows(workflows)
  .scheduler(scheduler)
  .agents(agents)
  .skills(skills)
  .fragments(fragments)
  .dags(dags)
  .namespaces(namespaceRegistry)
  .secrets(secretsStorage)
  .remoteWorkers()
  .sampleInput(inputFor)
  .uiDir(uiDir)
  .workers(new RegistryBackedWorkersProvider(workerRegistry))
  .versionRegistry(versionRegistry)
  .retention({
    maxAgeDays: Number(process.env.ZORYA_RETENTION_DAYS ?? 1),
    intervalMs: Number(process.env.ZORYA_RETENTION_INTERVAL_MS ?? 60 * 60 * 1000),
    batchSize: 500,
  })
  .logger(logger)
  .build();

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
logger.log(`Zorya demo server on http://${host}:${actualPort}`);
logger.log(`  - Storage:      sqlite (${dbPath})`);
logger.log(`  - Workflows:    ${Object.keys(workflowsByName).length} in-process (LocalWorkflows)`);
logger.log(`                  + queued fallback for any workflow advertised by remote workers`);
logger.log(`  - Agents:       ${agentScan.agents.map((a) => a.id).join(", ") || "(none)"}`);
logger.log(`                  • local backends (echo-bot, support-bot, ollama-bot, …)`);
logger.log(`                  • remote backend (remote-bot — federation proxy)`);
logger.log(`                  • cursor backend (cursor-bot — Cursor CLI 'agent -p')`);
logger.log(`  - Dashboard:    http://${host}:${actualPort}/`);
logger.log(`  - Agents tab:   http://${host}:${actualPort}/#/agents`);
logger.log(``);
logger.log(`  Two more processes round out the demo:`);
logger.log(``);
logger.log(`  1. Remote worker — joins the server, advertises workflows the in-process`);
logger.log(`     map doesn't have (hello-world, fan-out-demo). Triggers from the`);
logger.log(`     dashboard route through the QueuedWorkflows fallback to the worker.`);
logger.log(
  `       ZORYA_URL=http://${host}:${actualPort} \\\n` +
    `         bun --conditions=@promin/source run packages/zorya/examples/worker.ts`,
);
logger.log(``);
logger.log(`  2. User app — runs your own workflow code in-process while writing storage`);
logger.log(`     to this server (Temporal-style). See examples/user-app.ts.`);
logger.log(
  `       ZORYA_URL=http://${host}:${actualPort} \\\n` +
    `         bun --conditions=@promin/source run packages/zorya/examples/user-app.ts`,
);

// Graceful shutdown. Registering ANY `process.on("SIGINT")` handler in Bun
// overrides the default exit-on-Ctrl+C — the heartbeat-cleanup handlers
// above kept the process alive, so the listening socket stayed bound and
// the next boot hit EADDRINUSE. We now stop the server (releases the port
// and tears down the coordinator + scheduler loops) and exit explicitly.
const shutdown = async (signal: string) => {
  logger.log(`\n[zorya] ${signal} received — shutting down`);
  try {
    stopApprovalAutoSignaler();
    clearInterval(heartbeatHandle);
    await server.stop();
  } catch (e) {
    logger.error("[zorya] server.stop failed:", e);
  }
  process.exit(0);
};
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
