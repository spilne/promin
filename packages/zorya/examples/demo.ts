// ---------------------------------------------------------------------------
// Demo server — runs real workflows on an in-memory engine and serves the
// dashboard. Run with:
//   bun run packages/zorya/examples/demo.ts
//
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

import {
  InMemoryStepQueue,
  InMemoryWorkerRegistry,
  createWorkflowRunner,
  createSleepScanner,
  completeSignal,
  isJournaledSuspendStorage,
  type Workflow,
} from "@promin/workflow";
import {
  SqliteWorkflowStorage,
  SqliteSchedulerStorage,
  SqliteAgentRegistry,
  SqliteMemoryStore,
} from "@promin/sqlite";
import {
  anthropic,
  applyDiscoveredAgents,
  resolveLocalAgent,
  tool,
  type AgentTool,
  type LLMChatParams,
  type LLMProvider,
  type LLMResponse,
  type LLMStreamChunk,
} from "@promin/agent";
import { echoLLM } from "@promin/agent/testing";
import { z } from "zod";
import { Database } from "bun:sqlite";
import { ZoryaServer, scanAgentsFolder, scanWorkflowsFolder } from "../src/index.ts";
import path from "node:path";
import { mkdirSync } from "node:fs";

// ---------------------------------------------------------------------------
// Storage + runner
//
// Defaults to a persistent file under ./target so runs, schedules, and
// advertised workflows survive server restarts (and the dev hot-reload
// loop, which restarts the subprocess on .ts changes). Override with:
//   ZORYA_DB=:memory:        bun run zorya     # fresh on every boot
//   ZORYA_DB=./somewhere.db  bun run zorya     # custom path

const dbPath = process.env.ZORYA_DB ?? "./target/zorya.db";
if (dbPath !== ":memory:") {
  // mkdir -p the parent so first-time runs don't crash on a missing dir.
  mkdirSync(path.dirname(dbPath), { recursive: true });
}
const db = new Database(dbPath);
// Turn on WAL + foreign keys for file-backed DBs. No-op for :memory:.
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA foreign_keys = ON");

const storage = SqliteWorkflowStorage.make({ db });
// Persistent scheduler state — `lastFiredAt`, `tickCount`, `nextRun`, and
// leader locks all live on disk in the same db as workflow runs, so a
// server restart resumes schedules from where they left off instead of
// starting fresh on every boot.
const schedulerStorage = SqliteSchedulerStorage.make({ db });
const runner = createWorkflowRunner({ storage });

// Agent registry + memory store share the same db. Persists registered
// recipes, threads, messages, and per-scope memory across restarts so
// chats in the dashboard's Agents tab survive hot-reloads of the demo.
const agentRegistry = SqliteAgentRegistry.make({ db });
const memoryStore = SqliteMemoryStore.make({ db });

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
    console.log(`[zorya] discovered agent ${agent.id} (${path.relative(agentScanRoot, src)})`),
});
for (const w of rawAgentScan.warnings) console.warn(`[zorya] ${w}`);

// Recipes that need a live API key get filtered out when the key is
// missing, so the dashboard only surfaces agents that actually work.
const haveAnthropicKey = !!process.env["ANTHROPIC_API_KEY"];
const liveOnlyAgentIds = new Set<string>(["claude-bot"]);
if (!haveAnthropicKey) {
  console.warn(
    "[zorya] ANTHROPIC_API_KEY not set — skipping live agents: " + [...liveOnlyAgentIds].join(", "),
  );
}
const agentScan = {
  ...rawAgentScan,
  agents: rawAgentScan.agents.filter((a) => !liveOnlyAgentIds.has(a.id) || haveAnthropicKey),
};

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
// Tools — wired into the resolver per agent. Keyed by recipe id; an agent
// without an entry gets an empty tool kit. Each tool's `name` here must
// match a name listed in the recipe's `backend.tools`.

const weatherTool: AgentTool<{ city: string }, { tempF: number; conditions: string }> = tool({
  name: "weather",
  description: "Look up the current weather for a city.",
  parameters: z.object({ city: z.string().min(1) }),
  execute: async ({ city }) => {
    // Stubbed lookup — deterministic values per city so the demo doesn't
    // need a real API.
    const conditions = ["sunny", "cloudy", "rainy", "windy"];
    const idx = Math.abs(hashString(city)) % conditions.length;
    return {
      tempF: 60 + (Math.abs(hashString(city)) % 25),
      conditions: conditions[idx]!,
    };
  },
});

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}

// `currentTime` — exercises a tool with optional input. Uses Intl so any
// IANA timezone string works ("America/New_York", "Europe/Kyiv", etc.).
const currentTimeTool: AgentTool<
  { timezone?: string },
  { iso: string; formatted: string; timezone: string }
> = tool({
  name: "currentTime",
  description:
    "Get the current date and time, optionally in a specific IANA timezone (e.g. 'America/New_York'). Defaults to UTC.",
  parameters: z.object({
    timezone: z
      .string()
      .optional()
      .describe("IANA timezone like 'America/New_York' or 'Europe/Kyiv'. Defaults to UTC."),
  }),
  execute: async ({ timezone }) => {
    const tz = timezone && timezone.length > 0 ? timezone : "UTC";
    const now = new Date();
    let formatted: string;
    try {
      formatted = new Intl.DateTimeFormat("en-US", {
        timeZone: tz,
        dateStyle: "full",
        timeStyle: "long",
      }).format(now);
    } catch {
      // Invalid timezone — fall back to UTC and tell the model.
      formatted = `${now.toISOString()} (invalid timezone ${tz}, used UTC)`;
    }
    return { iso: now.toISOString(), formatted, timezone: tz };
  },
});

// `calculate` — single-op arithmetic. Avoids `eval` / Function() so the
// demo doesn't hand the model a remote-code-execution surface.
const calculateTool: AgentTool<
  { a: number; b: number; op: "add" | "subtract" | "multiply" | "divide" },
  { result: number }
> = tool({
  name: "calculate",
  description:
    "Compute a single arithmetic operation on two numbers. Use multiple calls for compound expressions.",
  parameters: z.object({
    a: z.number(),
    b: z.number(),
    op: z.enum(["add", "subtract", "multiply", "divide"]),
  }),
  execute: async ({ a, b, op }) => {
    switch (op) {
      case "add":
        return { result: a + b };
      case "subtract":
        return { result: a - b };
      case "multiply":
        return { result: a * b };
      case "divide":
        if (b === 0) throw new Error("division by zero");
        return { result: a / b };
    }
  },
});

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

const agentTools: Record<string, Record<string, AgentTool<unknown, unknown>>> = {
  // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
  "weather-bot": { weather: weatherTool as AgentTool<any, any> },
  // Live Claude gets the full kit — exercises a single-input tool
  // (weather), an optional-input tool (currentTime), an enum-typed tool
  // (calculate), and a tool that pokes at real server state
  // (listWorkflows). Together they prove the adapter wires tool
  // definitions, tool_use blocks, and tool_result blocks correctly with
  // a real model.
  "claude-bot": {
    // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
    weather: weatherTool as AgentTool<any, any>,
    // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
    currentTime: currentTimeTool as AgentTool<any, any>,
    // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
    calculate: calculateTool as AgentTool<any, any>,
    // biome-ignore lint/suspicious/noExplicitAny: tool inputs are validated at runtime via Zod
    listWorkflows: listWorkflowsTool as AgentTool<any, any>,
  },
};

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
  // Live LLM — bound only when the key is present. Anthropic's adapter
  // streams natively with real network latency, so we DON'T wrap it in
  // naturalLLM (that would pile fake delays on top of real ones).
  ...(haveAnthropicKey ? { "claude-bot": anthropic("claude-sonnet-4-6") } : {}),
};

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
  // Drop stale live-only rows from a previous boot when their capability
  // (e.g. ANTHROPIC_API_KEY) is no longer present, so the dashboard
  // doesn't surface agents that would fail at invoke time.
  for (const id of liveOnlyAgentIds) {
    if (!haveAnthropicKey) {
      const existing = await agentRegistry.get(id);
      if (existing) {
        await agentRegistry.unregister(id);
        console.log(`[zorya] unregistered stale ${id} (key missing)`);
      }
    }
  }
  if (agentScan.agents.length === 0) {
    console.log(`[zorya] no agents discovered under ${agentScanRoot}`);
    return;
  }
  // Upsert by default — keeps operator-edited rows from boot N when
  // recipe code changes for boot N+1. Pass `sync: true` if you want the
  // filesystem to be authoritative (deletes registry rows not in scan).
  const result = await applyDiscoveredAgents(agentRegistry, agentScan.agents);
  if (result.added.length > 0) {
    console.log(`[zorya] registered new agents: ${result.added.join(", ")}`);
  }
  console.log(`[zorya] upserted ${result.upserted.length} agent recipe(s)`);
}

// ---------------------------------------------------------------------------
// Workers — register two mock workers so the dashboard's Workers page has
// something to display. The demo runs every workflow in-process via
// `runner.run` (no actual task dispatch over a queue), so these workers
// don't claim any work; they just heartbeat and show up in the registry.
// To see real worker behavior (capability claims, run distribution, dead
// detection on stop), run the split example in `examples/split/`.

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
    console.log(`[zorya] discovered workflow ${name} (${path.relative(scanRoot, src)})`),
});
for (const w of scanResult.warnings) console.warn(`[zorya] ${w}`);
const workflowsByName: Record<string, Workflow<unknown, unknown>> = scanResult.workflows;

function inputFor(name: string): unknown {
  switch (name) {
    case "order":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        customer: `cust-${Math.floor(Math.random() * 100)}`,
      };
    case "payment":
      return { amount: Math.floor(Math.random() * 5_000) + 100, currency: "USD" };
    case "video-transcode":
      return {
        videoId: `vid-${Math.floor(Math.random() * 1_000_000)}`,
        url: "https://example.com/video.mp4",
      };
    case "onboarding":
      return { email: `user-${Math.floor(Math.random() * 10_000)}@example.com` };
    case "etl":
      return { source: "events-prod", batch: Math.floor(Math.random() * 100) };
    case "order-fulfillment":
      return {
        orderId: Math.floor(Math.random() * 10_000),
        items: ["sku-a", "sku-b"],
      };
    case "batch-process":
      return {
        batchId: `batch-${Math.floor(Math.random() * 10_000)}`,
        itemCount: 6 + Math.floor(Math.random() * 8),
      };
    case "approval-flow":
      return {
        requestId: Math.floor(Math.random() * 10_000),
        requester: `user-${Math.floor(Math.random() * 100)}`,
      };
    case "research":
      return {
        topic: ["durable-execution", "distributed-systems", "saga-patterns", "event-sourcing"][
          Math.floor(Math.random() * 4)
        ],
        sourceCount: 3 + Math.floor(Math.random() * 3),
      };
    default:
      return {};
  }
}

let idCounter = 0;
function nextId(name: string): string {
  idCounter += 1;
  return `${name}-${Date.now().toString(36)}-${idCounter}`;
}

async function triggerRun(
  name: string,
  input: unknown,
  opts: {
    workflowId?: string;
    namespace?: string;
    metadata?: Record<string, unknown>;
    runSource?: import("@promin/workflow").RunSource;
    runSourceId?: string;
  } = {},
): Promise<{ workflowId: string }> {
  const wf = workflowsByName[name];
  if (!wf) throw new Error(`Unknown workflow: ${name}`);
  const workflowId = opts.workflowId ?? nextId(name);
  // Pre-create the row whenever the caller wants namespace, metadata, OR
  // runSource to stick. `runner.run`'s internal createWorkflow is
  // idempotent — it sees the existing row and resumes instead of
  // overwriting — so pre-creation is the seam where these typed fields
  // land. Without this, scheduler-fired runs would lose their runSource
  // link and the dashboard's "filter by source" would return nothing.
  if (opts.namespace || opts.metadata || opts.runSource) {
    const result = await storage.createWorkflow({
      workflowId,
      workflowName: name,
      input,
      namespace: opts.namespace,
      metadata: opts.metadata,
      runSource: opts.runSource,
      runSourceId: opts.runSourceId,
      version: wf.version,
    });
    // Existing row hit. Two cases:
    //  - Same boot, idempotent retry of the same fire — leave it; runner
    //    will resume against the existing state.
    //  - Cross-boot collision: the deterministic workflowId (e.g.
    //    `${scheduleId}.${tickNumber}`) repeats after the scheduler's
    //    tickCount resets. The previous run's terminal `startedAt` /
    //    `completedAt` would otherwise pollute lag/duration math for THIS
    //    fire. Reset via `startFreshRun` — bumps the run counter, archives
    //    prior steps, clears the timestamps so the fresh run reports its
    //    own latency.
    if (!result.created && isTerminal(result.existing.status)) {
      await storage.startFreshRun(workflowId);
    }
  }
  // Fire-and-forget: we don't await run() so the server responds immediately.
  runner.run({ workflow: wf, workflowId, input }).catch(() => {
    // Failures are stored as workflow.failed state; swallow here so the
    // background loop keeps running.
  });
  return { workflowId };
}

function isTerminal(status: string): boolean {
  return status === "completed" || status === "failed" || status === "tripwire";
}

// ---------------------------------------------------------------------------
// Background: seed a handful of runs on startup so the first page has data.
// Ongoing traffic comes from schedules — pausing a schedule actually stops
// its runs (no hidden random loop).

async function seedInitialRuns() {
  // Skip when the persistent DB already has runs — keep accumulated
  // history intact across restarts. Schedules still fire on their own
  // cadence so the dashboard stays animated.
  const existing = await storage.listWorkflows({ limit: 1 });
  if (existing.length > 0) {
    console.log("[zorya] storage already has runs — skipping initial seed");
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
    await triggerRun(name, inputFor(name), { namespace });
  }
}

// ---------------------------------------------------------------------------
// Schedules — seed + lightweight poll-based firing

async function seedSchedules() {
  // Pin some schedules to specific namespaces so the runs they produce
  // populate tenant-a / tenant-b consistently — gives the namespace
  // switcher in the sidebar live, scheduled traffic to filter on.
  // Schedules carry the namespace BOTH on the top-level field (so the
  // dashboard's tenant switcher filters them via storage.listSchedules)
  // AND in metadata.namespace (so the demo's firer hands it to
  // triggerRun, which pre-creates the resulting workflow row in that
  // namespace). They're conceptually the same axis — the demo just has
  // to write it twice because schedule.namespace is what scheduler
  // storage queries on, while metadata.namespace is the input the
  // demo's firer reads.
  await schedulerStorage.upsertSchedule({
    id: "orders-every-15s",
    name: "Orders every 15s",
    intervalMs: 15_000,
    enabled: true,
    namespace: "tenant-a",
    metadata: { workflowName: "order", namespace: "tenant-a" },
  });
  await schedulerStorage.upsertSchedule({
    id: "payments-every-30s",
    name: "Payments every 30s",
    intervalMs: 30_000,
    enabled: true,
    jitterMs: 2_000,
    namespace: "tenant-a",
    metadata: { workflowName: "payment", namespace: "tenant-a" },
  });
  await schedulerStorage.upsertSchedule({
    id: "video-transcodes-every-45s",
    name: "Video transcodes every 45s",
    intervalMs: 45_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "video-transcode", namespace: "tenant-b" },
  });
  await schedulerStorage.upsertSchedule({
    id: "onboarding-every-60s",
    name: "Onboarding every 60s",
    intervalMs: 60_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "onboarding", namespace: "tenant-b" },
  });
  await schedulerStorage.upsertSchedule({
    id: "etl-hourly",
    name: "ETL hourly",
    cron: "0 * * * *",
    timezone: "UTC",
    enabled: true,
    metadata: { workflowName: "etl" },
  });
  await schedulerStorage.upsertSchedule({
    id: "fulfillment-every-40s",
    name: "Order fulfillment saga every 40s",
    intervalMs: 40_000,
    enabled: true,
    namespace: "tenant-a",
    metadata: { workflowName: "order-fulfillment", namespace: "tenant-a" },
  });
  await schedulerStorage.upsertSchedule({
    id: "batch-every-50s",
    name: "Batch process every 50s",
    intervalMs: 50_000,
    enabled: true,
    metadata: { workflowName: "batch-process" },
  });
  await schedulerStorage.upsertSchedule({
    id: "approvals-every-75s",
    name: "Approval flow every 75s",
    intervalMs: 75_000,
    enabled: true,
    metadata: { workflowName: "approval-flow" },
  });
  await schedulerStorage.upsertSchedule({
    id: "research-every-90s",
    name: "Research (journaled multi-activity) every 90s",
    intervalMs: 90_000,
    enabled: true,
    namespace: "tenant-b",
    metadata: { workflowName: "research", namespace: "tenant-b" },
  });
  await schedulerStorage.upsertSchedule({
    id: "weekly-payment-audit",
    name: "Weekly payment audit (paused)",
    cron: "0 2 * * 1",
    timezone: "UTC",
    enabled: false,
    metadata: { workflowName: "payment", input: { mode: "audit" } },
  });

  // Kickstart schedules that have never fired so the embedded
  // SchedulerLoop picks them up on its first poll. `upsertSchedule` alone
  // doesn't write `nextRun` — DurableScheduler.registerAsync would, but
  // the demo manages schedules through the storage directly.
  //
  // Only kickstart when `lastFired` is null. With persistent storage that
  // means "schedule has never run" — i.e. first boot or a brand-new
  // schedule was added. On subsequent restarts the existing nextRun
  // (committed by the previous boot's last poll) is honored, so schedules
  // resume from where they left off instead of re-firing immediately.
  const all = await schedulerStorage.listSchedules({ limit: 500 });
  const now = new Date();
  for (const s of all) {
    if (s.enabled === false) continue;
    const state = await schedulerStorage.loadScheduleState(s.id);
    if (state?.lastFired) continue;
    await schedulerStorage.setNextRun(s.id, now);
  }
}

/**
 * Auto-delivers the "approval" signal to any approval-flow run that's been
 * waiting for one. Runs every 5s. Half the approvals are approved, half
 * rejected — so the dashboard shows both terminal outcomes.
 */
async function startApprovalAutoSignaler(): Promise<void> {
  const delivered = new Set<string>();
  setInterval(async () => {
    const runs = await storage.listWorkflows({
      name: "approval-flow",
      status: "suspended",
      limit: 50,
    });
    for (const r of runs) {
      if (delivered.has(r.workflowId)) continue;
      // Only deliver to runs whose review step is waiting for a signal.
      const waiting = Object.values(r.steps).some(
        (s) => s.stepName === "review" && s.status === "waiting_for_signal",
      );
      if (!waiting) continue;
      const approved = Math.random() < 0.5;
      const payload = { approved, by: `auto-signaler` };
      // Three steps: store for the Signals tab, complete the pending journal
      // entry, and re-run the workflow so the journaled step picks up the
      // completed entry and continues. The sleep scanner only handles
      // sleep resumption — signal completion needs its own nudge.
      await storage.deliverSignal(r.workflowId, "approval", payload);
      if (isJournaledSuspendStorage(storage)) {
        await completeSignal({
          storage,
          workflowId: r.workflowId,
          stepName: "review",
          signalName: "approval",
          value: payload,
        });
      }
      delivered.add(r.workflowId);
      runner
        .run({
          workflow: workflowsByName["approval-flow"]!,
          workflowId: r.workflowId,
          input: r.input,
        })
        .catch(() => {
          // Suspended errors are expected; failures are recorded in storage.
        });
    }
  }, 5_000);
}

/**
 * Resume runs that were left in `pending` or `running` from a previous
 * server session. The demo drives execution in-process (no coordinator),
 * so when the bun --hot subprocess hot-replaces or the user kills the
 * server, every in-flight `runner.run()` promise dies with it. The rows
 * stay in storage with their last-observed status; without this sweep
 * they sit there forever.
 *
 * Recovery: re-call `runner.run` for each. The runner is idempotent on
 * (workflowId, input) — it loads the existing row, replays journal
 * entries for any journaled steps, and continues from the next pending
 * step. Stale runs from yesterday will resume now and complete with
 * a fresh end-time, which is fine for a demo but obviously the wrong
 * policy for production (you'd want a stale-cutoff + auto-fail).
 */
async function resumeOrphanedRuns() {
  const candidates = await storage.listWorkflows({ status: "pending", limit: 500 });
  const running = await storage.listWorkflows({ status: "running", limit: 500 });
  const all = [...candidates, ...running];
  if (all.length === 0) return;
  console.log(`[zorya] resuming ${all.length} orphaned run(s) from prior session`);
  for (const state of all) {
    const def = workflowsByName[state.workflowName];
    if (!def) continue; // workflow registry may have changed since the row was created
    runner.run({ workflow: def, workflowId: state.workflowId, input: state.input }).catch(() => {
      // Recovery best-effort; failures land in storage as workflow.failed.
    });
  }
}

// ---------------------------------------------------------------------------
// Boot

await seedSchedules();
await seedAgents();
void startApprovalAutoSignaler();
void resumeOrphanedRuns();

// Wake suspended workflows whose sleep has expired or whose signal was
// delivered. Without this, runs that entered ctx.sleep / ctx.signal never
// resume after their wake condition — they just sit in "suspended" forever.
const sleepScanner = createSleepScanner({
  storage,
  runner,
  scanIntervalMs: 2_000,
  resolveWorkflow: (name) => workflowsByName[name],
});
void sleepScanner.start();

await seedInitialRuns();

const uiDir = process.env.ZORYA_UI_DIR ?? path.join(import.meta.dir, "..", "dist", "public");

const server = new ZoryaServer({
  storage,
  scheduler: schedulerStorage,
  workflows: workflowsByName,
  // Agent gateway — exposes /api/agents/* and powers the Agents tab. The
  // resolver materialises a `LocalAgent` per request from the recipe in
  // the registry, sharing the per-agent LLM map so cycle state persists
  // across calls. Tools default to {} for now — no tool catalogue.
  agents: {
    registry: agentRegistry,
    resolve: (recipe) =>
      resolveLocalAgent(recipe, {
        runner,
        memory: memoryStore,
        llm: () => agentLlms[recipe.id] ?? naturalLLM(echoLLM()),
        tools: agentTools[recipe.id] ?? {},
      }),
  },
  // Same store the resolver uses, so the inspector reads the live cascade.
  memoryInspector: { memory: memoryStore },
  // Feeds the "Trigger workflow" form on the Workflows page with plausible
  // defaults so users can tweak fields instead of writing raw JSON.
  sampleInput: (name) => inputFor(name),
  uiDir,
  // Workers page reads from the registry. The demo mocks two entries
  // above; `stepQueue` is structurally required by the workerProtocol
  // type but unused at runtime here because we keep the explicit
  // `trigger` callback below — that path runs workflows in-process via
  // the runner, never actually enqueues to the step queue.
  workerProtocol: { stepQueue: new InMemoryStepQueue(), workerRegistry },
  // Embedded scheduler tick loop — `namespaces: "all"` polls every tenant
  // (the seed mixes "tenant-a", "tenant-b", and the global namespace).
  // `dispatchConcurrency: 5` caps the per-tick fan-out so a wakeup of
  // many simultaneous schedules doesn't slam the trigger.
  scheduling: {
    enabled: true,
    namespaces: "all",
    pollIntervalMs: 1_000,
    dispatchConcurrency: 5,
  },
  // Forward workflowId so the embedded SchedulerLoop's deterministic
  // `${scheduleId}.${tickNumber}` lands on storage — keeps repeat ticks
  // idempotent (createWorkflow is no-op on a known id). Schedules without
  // a `metadata.input` arrive here with `input === undefined`; synthesise
  // one from `inputFor(name)` so SQLite's NOT NULL constraint on the
  // `input` column doesn't reject the row (silent dispatch failure that
  // looks like "the scheduler ticked but no run appeared").
  trigger: (name, input, opts) =>
    triggerRun(name, input === undefined ? inputFor(name) : input, {
      namespace: opts?.namespace,
      workflowId: opts?.workflowId,
      metadata: opts?.metadata,
      // Forward the typed source link (`schedule` + scheduleId, `manual`,
      // …) so the dashboard's "filter by source" works without parsing
      // workflow ids or chasing metadata keys.
      runSource: opts?.runSource,
      runSourceId: opts?.runSourceId,
    }),
  rerun: async (workflowId) => {
    // After startFreshRun the row is reset; we still need to drive the
    // workflow again. Look up the name from storage, find its definition,
    // and call runner.run with the same workflow id.
    const state = await storage.loadWorkflow(workflowId);
    if (!state) return;
    const def = workflowsByName[state.workflowName];
    if (!def) return;
    runner.run({ workflow: def, workflowId, input: state.input }).catch(() => {});
  },
  // No explicit `workers` provider — the server falls through to
  // RegistryBackedWorkersProvider over `workerProtocol.workerRegistry`,
  // which surfaces the two mock workers we registered above.
});

const port = Number(process.env.PORT ?? 4100);
const { port: actualPort, hostname } = server.listen({ port });
const host = hostname === "0.0.0.0" ? "localhost" : hostname;
console.log(`Zorya demo server on http://${host}:${actualPort}`);
console.log(`  - Storage:      sqlite (${dbPath})`);
console.log(`  - Workflows:    ${Object.keys(workflowsByName).length} discovered`);
console.log(`  - Agents:       ${agentScan.agents.map((a) => a.id).join(", ") || "(none)"}`);
console.log(`  - Dashboard:    http://${host}:${actualPort}/`);
console.log(`  - Agents tab:   http://${host}:${actualPort}/#/agents`);
console.log(`  - Traffic comes from schedules — pause one to stop its runs`);

// Graceful shutdown. Registering ANY `process.on("SIGINT")` handler in Bun
// overrides the default exit-on-Ctrl+C — the heartbeat-cleanup handlers
// above kept the process alive, so the listening socket stayed bound and
// the next boot hit EADDRINUSE. We now stop the server (releases the port
// and tears down the coordinator + scheduler loops) and exit explicitly.
const shutdown = (signal: string) => {
  console.log(`\n[zorya] ${signal} received — shutting down`);
  try {
    server.stop();
  } catch (e) {
    console.error("[zorya] server.stop failed:", e);
  }
  process.exit(0);
};
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
