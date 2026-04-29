// ---------------------------------------------------------------------------
// Three-scope memory model for multi-tenant agent runtimes.
//
// Two orthogonal axes:
//
//   1. SCOPE     — who owns the data:    namespace → resource → thread
//   2. TIER      — how distilled it is:  raw → episodic → semantic → procedural
//
// The matrix below shows where each tier lives in each scope. Brain
// analogues (Atkinson–Shiffrin / Tulving) are noted for orientation —
// the model uses them as a sanity check, not a literal mapping.
//
//   ┌──────────────┬──────────────────┬────────────────────┬────────────────┬──────────────────┐
//   │              │ Raw (L0/L1)      │ Episodic (L2)      │ Semantic (L3)  │ Procedural (L4)  │
//   │              │ working / recent │ summaries +        │ distilled      │ reusable rules / │
//   │              │ buffer           │ salience + embed.  │ key/value      │ strategies       │
//   ├──────────────┼──────────────────┼────────────────────┼────────────────┼──────────────────┤
//   │ namespace    │ —                │ episodes           │ facts          │ staticRules      │
//   │ resource     │ —                │ episodes           │ facts          │ staticRules      │
//   │ thread       │ messages +       │ episodes           │ facts          │ —                │
//   │              │ workingMemory    │ (in-thread rollup) │                │                  │
//   └──────────────┴──────────────────┴────────────────────┴────────────────┴──────────────────┘
//
// Slot semantics:
//
//   - staticRules    — markdown, human-authored. CLAUDE.md / Project
//                      instructions / GPT system prompt analog. (Threads
//                      do not carry static rules — only namespace and
//                      resource do.)
//   - facts          — L3. Numbered, timestamped, plaintext list,
//                      inspectable at every layer. Written by the model
//                      via the `setMemory` tool or by user UI /
//                      privileged tools.
//   - workingMemory  — markdown scratchpad the agent self-edits.
//                      Most-mutated at the thread layer.
//   - messages       — L0/L1. Append-only transcript, THREAD ONLY.
//   - episodes       — L2. Distilled "what happened" entries with
//                      salience + optional embedding. Per-thread
//                      rollups support compaction; per-resource
//                      episodes power cross-thread recall.
//
// Inheritance: each scope row carries `inheritFromParent: boolean`
// (default `true`). Setting it to `false` cuts inheritance both ways:
//   - on resource: namespace contributions skipped at prompt build
//   - on thread:   both namespace AND resource contributions skipped
//
// This is the most important multi-tenant safety primitive (modeled on
// ChatGPT's project-only-memory mode shipped Aug 2025).
//
// `MemoryStore` is the canonical durable agent state. It is distinct
// from `MemoryIndex` (the flat semantic-recall index over `MemoryEntry`
// rows in this same package) — `MemoryIndex` is what an optional
// `SemanticRecall` capability sits over.
// ---------------------------------------------------------------------------

import type { Message } from "../message.ts";

/** A user-resource scope inside a tenant. */
export interface ScopedKey {
  readonly namespaceId: string;
  readonly resourceId: string;
}

/**
 * A thread scope. `resourceId` is optional — a thread can exist without
 * a user attached to it (e.g. an organization-wide chat). When present,
 * the resource layer participates in the cascade.
 */
export interface ThreadKey {
  readonly namespaceId: string;
  readonly resourceId?: string;
  readonly threadId: string;
}

/**
 * One numbered, timestamped fact.
 *
 * Facts are the "Saved Memories" tier — small, durable plaintext entries
 * that survive across conversations. Render path = read path = user
 * inspection path (no embeddings required for the base tier).
 */
export interface Fact {
  readonly id: string;
  readonly text: string;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/**
 * Stored thread message — the runtime `Message` shape with storage
 * identity attached. Append paths accept plain `Message`; reads return
 * `StoredMessage` so callers always have a sequence number + creation
 * timestamp to work with.
 */
export type StoredMessage = Message & {
  readonly seq: number;
  readonly createdAt: number;
};

/**
 * Episodic record — L2 memory tier. A distilled "what happened" entry
 * for one chunk of past activity (a thread, a session, a resolved task).
 * Sits between raw `messages` and crystallised `Fact`s:
 *
 *   raw transcript  →  EpisodicRecord (summary + salience + embedding)
 *                                     →  Fact (one-line, indefinite)
 *
 * Storage scope is decided by the writer. Per-thread episodes are
 * useful for in-thread compaction (free message tokens, keep gist).
 * Per-resource episodes power cross-thread recall ("what did this user
 * and I work on last week?"). Per-namespace episodes are rare but
 * supported for org-wide events.
 *
 * The base store treats episodes as plain rows. `SemanticRecall`
 * adapters index `summary` + `embedding` for similarity retrieval;
 * the base store sorts by `salience` or `createdAt` only.
 */
export interface EpisodicRecord {
  readonly id: string;
  /** 3-line markdown summary — what happened, in compressed form. */
  readonly summary: string;
  /** Optional one-line outcome / resolution. */
  readonly outcome: string | null;
  /**
   * 0..1 importance score. Used for retrieval ranking and forgetting
   * curves. Stored as-given; no auto-decay in the base store.
   */
  readonly salience: number;
  /** Optional embedding vector — populated by indexer or model. */
  readonly embedding: ReadonlyArray<number> | null;
  /** Source thread this episode was distilled from, if any. */
  readonly sourceThreadId: string | null;
  /** Inclusive seq range on the source thread, if applicable. */
  readonly sourceMessageRange: { readonly fromSeq: number; readonly toSeq: number } | null;
  /** When the underlying events occurred (may precede `createdAt`). */
  readonly occurredAt: number;
  readonly createdAt: number;
  readonly metadata: Readonly<Record<string, unknown>>;
}

/** Caller-facing input to `appendXEpisode`. The store assigns `id` + `createdAt`. */
export interface EpisodeInput {
  readonly summary: string;
  readonly outcome?: string | null;
  /** Defaults to `0.5` when omitted. Clamped to [0, 1]. */
  readonly salience?: number;
  readonly embedding?: ReadonlyArray<number> | null;
  readonly sourceThreadId?: string | null;
  readonly sourceMessageRange?: { readonly fromSeq: number; readonly toSeq: number } | null;
  /** Defaults to `now()` when omitted. */
  readonly occurredAt?: number;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** Filter / order params for `listXEpisodes`. */
export interface EpisodeListParams {
  readonly limit?: number;
  /** Drop episodes below this salience threshold. */
  readonly minSalience?: number;
  /** Defaults to "salienceDesc". */
  readonly order?: "salienceDesc" | "createdDesc" | "occurredDesc";
}

/** Common slot shape — namespace and resource carry these four. */
interface NamespaceLikeRow {
  readonly staticRules: string | null;
  readonly workingMemory: string | null;
  readonly inheritFromParent: boolean;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/** Per-namespace state. */
export interface NamespaceRow extends NamespaceLikeRow {
  readonly namespaceId: string;
  readonly metadata: Readonly<Record<string, unknown>>;
}

/** Per-(namespace, resource) state. */
export interface ResourceRow extends NamespaceLikeRow {
  readonly namespaceId: string;
  readonly resourceId: string;
  readonly metadata: Readonly<Record<string, unknown>>;
}

/**
 * Per-thread state. Threads do not carry `staticRules` — the static
 * content is owned by namespace/resource and inherited via the cascade.
 */
export interface ThreadRow {
  readonly namespaceId: string;
  readonly resourceId: string | null;
  readonly threadId: string;
  /**
   * Display title for the thread. Optional; UIs fall back to `threadId`
   * when null. First-class column (rather than a `metadata.title` key)
   * so backends can index / search on it and `setMetadata` can't
   * accidentally clobber it.
   */
  readonly title: string | null;
  readonly workingMemory: string | null;
  readonly inheritFromParent: boolean;
  readonly metadata: Readonly<Record<string, unknown>>;
  /**
   * When the thread was archived. Null means active.
   * First-class column so the agent model can't accidentally clobber it
   * through `setMetadata`.
   */
  readonly archivedAt: number | null;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/** Compact summary returned by `listThreads`. */
export interface ThreadSummary {
  readonly namespaceId: string;
  readonly resourceId: string | null;
  readonly threadId: string;
  /** See `ThreadRow.title`. UIs fall back to `threadId` when null. */
  readonly title: string | null;
  readonly metadata: Readonly<Record<string, unknown>>;
  /** When the thread was archived. Null means active. */
  readonly archivedAt: number | null;
  readonly messageCount: number;
  readonly lastActiveAt: number;
  readonly createdAt: number;
}

/** Patch shapes accepted by `upsert*` calls. Caller sets only the slots they care about. */
export interface NamespacePatch {
  readonly staticRules?: string | null;
  readonly workingMemory?: string | null;
  readonly inheritFromParent?: boolean;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ResourcePatch extends NamespacePatch {}

export interface ThreadInit {
  readonly resourceId?: string;
  readonly title?: string | null;
  readonly workingMemory?: string | null;
  readonly inheritFromParent?: boolean;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly archivedAt?: number | null;
}

/** Filter / pagination for `listThreads`. */
export interface ListThreadsParams {
  readonly namespaceId: string;
  /** When set, restrict to threads owned by this resource. */
  readonly resourceId?: string;
  /**
   * Subset match against thread metadata. Each entry must be present
   * on the thread's metadata with an equal value.
   */
  readonly metadataFilter?: Readonly<Record<string, unknown>>;
  /**
   * Case-insensitive substring match on `threadId`. Implementations
   * SHOULD push this to the storage layer (SQL LIKE, FTS, etc.) rather
   * than filtering after the fact, so dropdown searches scale beyond
   * the page-size hint.
   */
  readonly q?: string;
  readonly limit?: number;
  /** Opaque continuation token returned from a prior page. */
  readonly cursor?: string;
  /** Defaults to "lastActiveDesc". */
  readonly order?: "lastActiveDesc" | "createdAsc" | "createdDesc";
  /**
   * When `false` (default): exclude archived threads (archivedAt IS NOT NULL).
   * When `true`: include only archived threads.
   * When `undefined`: return all threads regardless of archive status.
   */
  readonly archived?: boolean;
}

/** Range for `getMessages`. Defaults to "last `limit` messages, oldest first". */
export interface MessageRange {
  /** Inclusive lower bound (sequence number). */
  readonly fromSeq?: number;
  /** Inclusive upper bound (sequence number). */
  readonly toSeq?: number;
  /** Max messages returned. */
  readonly limit?: number;
  /** "asc" = oldest-first (default), "desc" = newest-first. */
  readonly order?: "asc" | "desc";
}

/**
 * Token budget governing `resolveContext` trimming. The cascade always
 * keeps `staticRules` and `facts` for every layer (small, durable);
 * only the message tail is trimmed when over budget.
 */
export interface TokenBudget {
  /** Hard upper bound on the number of message tokens to include. */
  readonly maxMessageTokens: number;
  /**
   * Hard upper bound on tokens spent on injected episodic recall.
   * Default `0` — episodes are opt-in. When > 0, `resolveContext` pulls
   * top-salience resource-scoped episodes up to this budget.
   */
  readonly maxEpisodeTokens?: number;
  /**
   * Caller-provided message estimator. Must return a stable, non-negative
   * integer. Defaults to chars/4 when unset.
   */
  readonly estimate?: (m: StoredMessage) => number;
  /**
   * Caller-provided episode estimator. Must return a stable, non-negative
   * integer. Defaults to chars/4 over `summary + outcome` when unset.
   */
  readonly estimateEpisode?: (e: EpisodicRecord) => number;
}

/**
 * Resolved cascade — the prompt-ready view returned by `resolveContext`.
 *
 * `systemPrompt` carries the deterministic merge of:
 *   - namespace.staticRules + namespace.facts + namespace.workingMemory
 *   - cache boundary marker, kept stable so providers that respect
 *     prompt caching can split cleanly
 *   - resource.staticRules + resource.facts + resource.workingMemory
 *   - thread.workingMemory
 *
 * `messages` is the trimmed thread tail (oldest-first within budget).
 *
 * `recallHits` is reserved for an optional `SemanticRecall` capability;
 * the base store always returns `undefined`.
 */
export interface ResolvedContext {
  readonly systemPrompt: string;
  readonly messages: StoredMessage[];
  readonly recallHits?: ReadonlyArray<RecallHit>;
}

/** Optional semantic recall result. Reserved; not produced by the base store. */
export interface RecallHit {
  readonly source: "fact" | "message" | "episode";
  readonly scope: "namespace" | "resource" | "thread";
  readonly id: string;
  readonly text: string;
  readonly score: number;
  readonly createdAt: number;
}

/**
 * Three-scope memory store with four cognitive tiers per scope.
 *
 * Tier ↔ scope matrix:
 *
 * | scope     | raw (L0/L1)       | episodic (L2) | semantic (L3) | procedural (L4) |
 * |-----------|-------------------|---------------|---------------|-----------------|
 * | namespace | —                 | episodes      | facts         | staticRules     |
 * | resource  | —                 | episodes      | facts         | staticRules     |
 * | thread    | messages, working | episodes      | facts         | —               |
 *
 * Implementations: `InMemoryMemoryStore` (reference) and `PgMemoryStore`
 * (Postgres, in `@promin/postgres`). Both must pass `memoryStoreTestSuite`.
 */
export interface MemoryStore {
  // --- Namespace --------------------------------------------------------
  getNamespace(namespaceId: string): Promise<NamespaceRow | null>;
  upsertNamespace(namespaceId: string, patch: NamespacePatch): Promise<NamespaceRow>;
  appendNamespaceFact(namespaceId: string, text: string): Promise<Fact>;
  listNamespaceFacts(namespaceId: string): Promise<Fact[]>;
  deleteNamespaceFact(namespaceId: string, factId: string): Promise<void>;
  appendNamespaceEpisode(namespaceId: string, input: EpisodeInput): Promise<EpisodicRecord>;
  listNamespaceEpisodes(namespaceId: string, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteNamespaceEpisode(namespaceId: string, episodeId: string): Promise<void>;

  // --- Resource (user / persona) ---------------------------------------
  getResource(key: ScopedKey): Promise<ResourceRow | null>;
  upsertResource(key: ScopedKey, patch: ResourcePatch): Promise<ResourceRow>;
  appendResourceFact(key: ScopedKey, text: string): Promise<Fact>;
  listResourceFacts(key: ScopedKey): Promise<Fact[]>;
  deleteResourceFact(key: ScopedKey, factId: string): Promise<void>;
  appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord>;
  listResourceEpisodes(key: ScopedKey, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteResourceEpisode(key: ScopedKey, episodeId: string): Promise<void>;

  // --- Thread ----------------------------------------------------------
  createThread(key: ThreadKey, init?: ThreadInit): Promise<ThreadRow>;
  getThread(key: ThreadKey): Promise<ThreadRow | null>;
  listThreads(params: ListThreadsParams): Promise<ThreadSummary[]>;
  setThreadWorking(key: ThreadKey, content: string | null): Promise<void>;
  /** Set the thread's display title. `null` clears it (UI falls back to threadId). */
  setThreadTitle(key: ThreadKey, title: string | null): Promise<void>;
  setThreadMetadata(key: ThreadKey, metadata: Readonly<Record<string, unknown>>): Promise<void>;
  setThreadInheritFromParent(key: ThreadKey, inherit: boolean): Promise<void>;
  /** Archive or restore a thread. Pass `null` to restore (clear archivedAt). */
  setThreadArchived(key: ThreadKey, archivedAt: number | null): Promise<void>;
  appendThreadFact(key: ThreadKey, text: string): Promise<Fact>;
  listThreadFacts(key: ThreadKey): Promise<Fact[]>;
  deleteThreadFact(key: ThreadKey, factId: string): Promise<void>;
  appendMessages(key: ThreadKey, msgs: ReadonlyArray<Message>): Promise<StoredMessage[]>;
  getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]>;
  appendThreadEpisode(key: ThreadKey, input: EpisodeInput): Promise<EpisodicRecord>;
  listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteThreadEpisode(key: ThreadKey, episodeId: string): Promise<void>;
  deleteThread(key: ThreadKey): Promise<void>;

  // --- Cascade & assembly ----------------------------------------------
  /**
   * Build the prompt-ready view for a thread by merging namespace +
   * resource + thread layers per the documented cascade. Honors
   * `inheritFromParent` severance and trims messages to fit `budget`.
   */
  resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext>;
}

/**
 * Optional semantic recall capability. A `MemoryStore` MAY also
 * implement this — adapters that index facts and messages can be
 * layered on top of any base store. Use `isSemanticRecall` to detect.
 */
export interface SemanticRecall {
  readonly hasSemanticRecall: true;
  recall(
    key: ThreadKey,
    query: string,
    opts?: { limit?: number; sources?: ReadonlyArray<"fact" | "message" | "episode"> },
  ): Promise<RecallHit[]>;
}

/** Type guard. */
export function isSemanticRecall(s: unknown): s is SemanticRecall {
  return (
    typeof s === "object" &&
    s !== null &&
    (s as { hasSemanticRecall?: unknown }).hasSemanticRecall === true
  );
}

/**
 * Marker placed in `ResolvedContext.systemPrompt` between stable layers
 * (namespace) and volatile layers (resource + thread). Providers that
 * implement prompt caching can split on this exact marker. Plaintext —
 * not a control character — so the prompt stays human-inspectable.
 */
export const PROMPT_CACHE_BOUNDARY = "<!-- promin:cache-boundary -->";
