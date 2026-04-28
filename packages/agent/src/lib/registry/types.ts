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
}

/**
 * Backend-specific recipe. Discriminated by `type`.
 *
 * Today: only `local` (LocalAgent over agentAction / agentLoop).
 * Future: `acp` (Claude Code / Codex / OpenCode / OpenClaw via ACP),
 *         `mastra` (wraps @mastra/core Agent), `http` (proxied).
 */
export type AgentBackend = LocalAgentBackend;

export interface LocalAgentBackend {
  readonly type: "local";
  /** Model identifier — runtime resolves to an `LLMProvider`. */
  readonly model: { readonly provider: string; readonly id: string };
  /** Inline system prompt. For prompt-registry indirection, use `null` and resolve outside. */
  readonly systemPrompt: string | null;
  /** Tool names to wire in. Runtime supplies the implementations. */
  readonly tools: ReadonlyArray<string>;
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
  /** Free-form extension knobs the resolver may consume. */
  readonly extra?: Readonly<Record<string, unknown>>;
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

/** Recipe-level subset of `AutoDistillConfig`. */
export interface AutoDistillRecipe {
  readonly messageThreshold?: number;
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
