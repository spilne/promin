// ---------------------------------------------------------------------------
// `AgentRegistry` — the "who can this gateway invoke" table.
//
// A registry stores `RegisteredAgent` entries: durable, JSON-serializable
// recipes that the gateway resolves into a live `Agent` per request.
// Static-config knobs (model spec, system prompt, tool names) live here;
// runtime injectables (LLMProvider instances, AgentTool implementations,
// WorkflowRunner, MemoryStore) are supplied by the gateway when the
// recipe is materialised.
//
// Design choices:
//
// 1. Backend is a discriminated union so the registry stays open to ACP
//    spawners, Mastra, HTTP-proxied agents, etc., without breaking
//    existing rows. First version ships only `LocalAgentBackend`.
//
// 2. `version` is part of the primary key, defaulting to `"v1"`. Lets
//    operators run experiments side-by-side and rollback by re-pointing.
//
// 3. No tenant column — agents are tenant-agnostic templates. Tenant
//    binding happens at gateway invocation time via `agent.bind()` or
//    per-call opts (see LocalAgent.bind).
// ---------------------------------------------------------------------------

import type { NetworkRecipe } from "../network/types.ts";

/**
 * A registered agent recipe — JSON-serializable, durable, version-keyed.
 * The runtime resolves this into a live `Agent` by combining it with
 * runtime injectables (LLM provider instances, tool implementations,
 * runner, memory store).
 */
export interface RegisteredAgent {
  readonly id: string;
  readonly version: string;
  readonly backend: AgentBackend;
  readonly metadata: AgentMetadata;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface AgentMetadata {
  readonly description: string | null;
  readonly capabilities: ReadonlyArray<string>;
  readonly tags: ReadonlyArray<string>;
  /**
   * Marks this recipe as a CLONEABLE TEMPLATE. The Designer surfaces
   * templates in a gallery view; the gateway's clone endpoint accepts
   * a `secrets` body for templates and validates that every named
   * `requiredSecrets` key has a value.
   *
   * Clones do NOT inherit the template flag — once cloned, it's an
   * ordinary recipe owned by the cloner.
   */
  readonly template?: boolean;
  /**
   * Names of secrets a cloner must supply when forking this template.
   * The clone endpoint validates that body.secrets covers every name
   * before persisting; missing names return 400 missing_required_secrets.
   *
   * Typically pairs with `model.credentialRef` so the cloner's key is
   * stored under the same name the recipe references.
   */
  readonly requiredSecrets?: ReadonlyArray<string>;
  /**
   * Operator kill-switch. `enabled !== false` (default) means the
   * recipe is invokable; setting `enabled: false` keeps the row in
   * the registry (so threads / history / metadata stay browsable)
   * but rejects new invocations at the gateway with 410 Gone.
   *
   * Lets operators take a recipe out of rotation without losing its
   * threads / runs / memory. Useful for: incident response (LLM
   * provider outage), schema migrations (new tool refs not yet
   * wired), staged rollouts (disable v1 after promoting v2).
   */
  readonly enabled?: boolean;
}

/**
 * Backend-specific recipe. Discriminated by `type`.
 *
 * - `local`  — LocalAgent running in this process (agentAction / agentLoop)
 * - `remote` — thin HTTP proxy forwarding calls to another Zorya deployment
 */
export type AgentBackend = LocalAgentBackend | RemoteAgentBackend | CursorAgentBackend;

export interface LocalAgentBackend {
  readonly type: "local";
  /**
   * Model identifier — runtime resolves to an `LLMProvider`.
   *
   * `credentialRef` is the BYOK hook: when set, the resolver fetches
   * the named secret from `SecretsStorage` (cascade: resource →
   * namespace → global) at request time and passes the value as the
   * LLM factory's `apiKey` argument. This lets a tenant supply their
   * own Anthropic / OpenAI / etc. key — they pay their own bills,
   * stay under their own contract, manage their own rate limits.
   * When unset, the host's pooled key is used (today's behaviour).
   */
  readonly model: {
    readonly provider: string;
    readonly id: string;
    readonly credentialRef?: string;
  };
  /** Inline system prompt. For prompt-registry indirection, use `null` and resolve outside. */
  readonly systemPrompt: string | null;
  /** Tool names to wire in. Runtime supplies the implementations. */
  readonly tools: ReadonlyArray<string>;
  /**
   * Optional MCP servers to attach. The host opens (or reuses) an
   * `McpClient` per server via `McpClientPool` at resolve time, lists
   * each server's tools, and merges them into the agent's tool list
   * keyed as `<serverName>:<toolName>`. The recipe's `tools` field
   * picks which of those to actually expose to the LLM (alongside
   * in-process tools).
   */
  readonly mcpServers?: ReadonlyArray<import("../mcp/types.ts").McpServerConfig>;
  /**
   * Catalog of skills this agent may load on demand. Each ref pins a skill
   * `(id, version?)` from the `SkillRegistry`. At resolve time the host
   * resolves the catalog (see `resolveSkillCatalog`), injects each skill's
   * `description` + `whenToUse` into the system prompt, and auto-attaches a
   * `loadSkill` tool the model calls to pull a full body into context.
   *
   * Pinned per recipe: changing this list (or cutting a new recipe version)
   * is the supported way to change an agent's catalog — the change surfaces
   * on the next turn since the system prompt is rebuilt from live config.
   * Leave unset for an agent with no skills.
   */
  readonly skills?: ReadonlyArray<import("../skills/types.ts").SkillRef>;
  /** Per-turn step cap. Optional, runtime default applies when unset. */
  readonly maxStepsPerTurn?: number;
  /** Max user turns per session before the loop terminates. */
  readonly maxTurns?: number;
  /**
   * Recipe-level auto-compaction config. JSON-serialisable subset of
   * the runtime `AutoCompactConfig` (no `when` predicate — closures
   * don't survive registry persistence). When set, OVERRIDES the host's
   * resolver-supplied default. Set to `false` to explicitly disable
   * for this recipe even if the host enables it globally. Leave
   * unset to inherit the host's setting.
   */
  readonly autoCompact?: AutoCompactRecipe | false;
  /** Recipe-level auto-distillation config. Same merge semantics as `autoCompact`. */
  readonly autoDistill?: AutoDistillRecipe | false;
  /**
   * Recipe-level token budget for `MemoryStore.resolveContext`. When
   * set, overrides the host default. JSON-serialisable subset (the
   * `estimate` / `estimateEpisode` callbacks are runtime-only and stay
   * host-supplied).
   */
  readonly contextBudget?: ContextBudgetRecipe;
  /**
   * Recipe-level opt-in to the agents network. When set, the resolver
   * auto-attaches `findAgent` and `callAgent` tools so this agent can
   * discover and delegate to peers in the same namespace. Leave unset
   * to keep this agent isolated. See `packages/agent/src/lib/network/types.ts`.
   */
  readonly network?: NetworkRecipe;
  /**
   * Environment variable names that must be set at resolve time.
   * `resolveLocalAgent` throws a descriptive error when any are absent;
   * `applyDiscoveredAgents` emits a warning when registering a recipe
   * whose vars aren't in the current environment (but still registers —
   * the recipe is just JSON and can be used once the vars are set).
   */
  readonly requiredEnv?: ReadonlyArray<string>;
  /** Free-form extension knobs the resolver may consume. */
  readonly extra?: Readonly<Record<string, unknown>>;
}

/**
 * Remote backend — forwards all agent calls over HTTP to another Zorya
 * deployment. The local process acts as a pure proxy: it does not run
 * any LLM or memory store for this agent; everything lives on the remote.
 *
 * Use case: cross-org federation where Org A's coordinator delegates to
 * Org B's specialist without either side leaving its own trust boundary.
 */
export interface RemoteAgentBackend {
  readonly type: "remote";
  /** Base URL of the remote Zorya server. Trailing slash is stripped. */
  readonly endpoint: string;
  /** Agent id as registered on the remote server. */
  readonly remoteAgentId: string;
  /**
   * Auth credentials sent with every request. v1 supports bearer tokens
   * only. Pass `undefined` for unauthenticated local dev (same origin).
   */
  readonly auth?: {
    readonly kind: "bearer";
    readonly token: string;
  };
  /** Per-call timeout in ms. No timeout by default. */
  readonly timeoutMs?: number;
}

/**
 * Cursor backend — drives Anthropic's Cursor CLI (`agent -p`) over its
 * NDJSON stream-json output. The runtime spawns one child per
 * invocation; the resolver wires `process.env.CURSOR_API_KEY` (or
 * whatever is named in `requiredEnv`) into the child env.
 *
 * Use case: route a tenant's coding-heavy agent invocations to Cursor
 * while keeping chat / tool-driven agents on `local`.
 */
export interface CursorAgentBackend {
  readonly type: "cursor";
  /** Override the binary name. Default: `"agent"`. */
  readonly command?: string;
  /** Default model. Cursor accepts e.g. `"auto"`, `"composer-2"`, `"sonnet-4.5-thinking"`. */
  readonly model?: string;
  /** Workspace path Cursor operates in (mapped to `--workspace`). */
  readonly workspace?: string;
  /** Spawn inside a fresh git worktree (`--worktree`). */
  readonly worktree?: boolean;
  /** Pass `--trust` (skip Cursor's first-run trust prompt). Default: true. */
  readonly trust?: boolean;
  /** `--sandbox enabled|disabled`. Default: omitted (Cursor's own default). */
  readonly sandbox?: "enabled" | "disabled";
  /** Extra raw args appended to every spawn. Forward-compat for new flags. */
  readonly extraArgs?: ReadonlyArray<string>;
  /**
   * Env var names that must be set at resolve time. Default:
   * `["CURSOR_API_KEY"]`. `resolveCursorAgent` throws with a descriptive
   * error when any are missing; `applyDiscoveredAgents` warns at
   * registration time.
   */
  readonly requiredEnv?: ReadonlyArray<string>;
}

/**
 * Recipe-level subset of `AutoCompactConfig`. Numeric thresholds + mode
 * persist on the recipe; the predicate-based `when` escape hatch is
 * runtime-only (host can layer it via `resolveLocalAgent.deps.autoCompact.when`).
 */
export interface AutoCompactRecipe {
  readonly messageThreshold?: number;
  readonly tokenThreshold?: number;
  readonly contextLimit?: number;
  readonly compressAt?: number;
  readonly keepRecent?: number;
  readonly mode?: "background" | "blocking";
}

/**
 * Recipe-level subset of `AutoDistillConfig`. Numeric thresholds + mode
 * persist on the recipe; the predicate-based `when` escape hatch is
 * runtime-only (host can layer it via `resolveLocalAgent.deps.autoDistill.when`).
 *
 * Multiple thresholds compose with OR semantics — fires when ANY of
 * `messageThreshold`, `tokenThreshold`, `intervalMs` trips.
 */
export interface AutoDistillRecipe {
  readonly messageThreshold?: number;
  readonly tokenThreshold?: number;
  readonly intervalMs?: number;
  readonly force?: boolean;
  readonly mode?: "background" | "blocking";
}

/** Recipe-level subset of `TokenBudget`. */
export interface ContextBudgetRecipe {
  readonly maxMessageTokens: number;
  readonly maxEpisodeTokens?: number;
}

/**
 * Caller-facing input to `register`. The store assigns `createdAt` /
 * `updatedAt` and defaults `version` to `"v1"` when omitted.
 */
export interface RegisterAgentInput {
  readonly id: string;
  /** Defaults to `"v1"` when omitted. */
  readonly version?: string;
  readonly backend: AgentBackend;
  readonly metadata?: Partial<AgentMetadata>;
}

/** Filter / pagination params for `list`. */
export interface ListAgentsParams {
  /** Restrict to agents whose `metadata.capabilities` include this. */
  readonly capability?: string;
  /** Restrict to agents whose `metadata.tags` include this. */
  readonly tag?: string;
  /** Restrict to a specific backend type. */
  readonly backendType?: AgentBackend["type"];
  readonly limit?: number;
  readonly cursor?: string;
  /** Defaults to "createdDesc" (most recent first). */
  readonly order?: "createdAsc" | "createdDesc" | "idAsc";
}

/**
 * Versioned registry of agent recipes. Implementations:
 *   - `InMemoryAgentRegistry` (reference, in this package)
 *   - `SqliteAgentRegistry` (SQLite, in `@promin/sqlite`)
 *
 * Both must pass `agentRegistryTestSuite`.
 */
export interface AgentRegistry {
  /**
   * Register or replace an agent at `(id, version)`. When the row exists,
   * `backend` and `metadata` are replaced; `createdAt` is preserved and
   * `updatedAt` advances.
   */
  register(input: RegisterAgentInput): Promise<RegisteredAgent>;

  /**
   * Look up by id. When `version` is omitted, returns the most recently
   * registered version of that id (sorted by `updatedAt` desc).
   */
  get(id: string, version?: string): Promise<RegisteredAgent | null>;

  /** List, optionally filtered + paginated. */
  list(params?: ListAgentsParams): Promise<RegisteredAgent[]>;

  /** All versions of one id, in ascending `createdAt` order. */
  versions(id: string): Promise<RegisteredAgent[]>;

  /**
   * Remove a row. When `version` is omitted, removes ALL versions of `id`.
   * No-op when nothing matches.
   */
  unregister(id: string, version?: string): Promise<void>;
}

/** Default version assigned to new registrations when caller doesn't specify. */
export const DEFAULT_AGENT_VERSION = "v1";

/** Default `AgentMetadata` shape for new registrations. */
export const DEFAULT_AGENT_METADATA: AgentMetadata = {
  description: null,
  capabilities: [],
  tags: [],
};
