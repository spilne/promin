// ---------------------------------------------------------------------------
// Three-scope memory model for agent runtimes with multi-user / multi-team
// scoping. Today's outermost axis is `namespaceId` (think team / workspace
// within a single trusted operator); a future `tenantId` axis would sit
// above it for org-level isolation.
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
//   │              │ buffer           │ salience           │ key/value      │ strategies       │
//   ├──────────────┼──────────────────┼────────────────────┼────────────────┼──────────────────┤
//   │ namespace    │ —                │ episodes           │ facts          │ staticRules      │
//   │ resource     │ —                │ episodes           │ facts          │ staticRules      │
//   │ thread       │ messages +       │ episodes           │ facts          │ —                │
//   │              │ workingMemory    │ (in-thread rollup) │                │                  │
//   └──────────────┴──────────────────┴────────────────────┴────────────────┴──────────────────┘
//
// Slot semantics:
//
//   - staticRules    — markdown, operator-authored. CLAUDE.md / Project
//                      instructions / GPT system prompt analog. (Threads
//                      do not carry static rules — only namespace and
//                      resource do.) The only memory slot rendered as
//                      instructions; everything else is reference data.
//   - facts          — L3. Numbered, timestamped, plaintext list,
//                      inspectable at every layer. Written by the model
//                      via the `memory` tool, by the consolidator, or by
//                      user UI / privileged tools; each carries its
//                      provenance (`FactSource`).
//   - workingMemory  — markdown scratchpad the agent self-edits.
//                      Most-mutated at the thread layer.
//   - messages       — L0/L1. Append-only transcript, THREAD ONLY.
//   - episodes       — L2. Distilled "what happened" entries with
//                      salience. A thread's latest compaction episode
//                      stands in for the messages it folded;
//                      per-resource episodes power cross-thread recall.
//
// Inheritance: each scope row carries `inheritFromParent: boolean`
// (default `true`). Setting it to `false` cuts inheritance both ways:
//   - on resource: namespace contributions skipped at prompt build
//   - on thread:   both namespace AND resource contributions skipped
//
// This is the most important cross-scope safety primitive (modeled on
// ChatGPT's project-only-memory mode shipped Aug 2025).
//
// `MemoryStore` is the canonical durable agent state.
// ---------------------------------------------------------------------------

import type { Message } from "../message.ts";

/** A user-resource scope inside a namespace. */
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
  /** Who wrote the fact. See `FactSource`. */
  readonly source: FactSource;
  readonly createdAt: number;
  readonly updatedAt: number;
}

/**
 * Provenance of a fact:
 *   - `operator` — written by an operator (admin API / dashboard, seed data)
 *   - `user`     — entered directly by the end user through the host app
 *   - `model`    — written by the agent (memory tool, tool code) or
 *                  distilled from a transcript by the consolidator
 *
 * Every fact renders as untrusted reference data whatever its source; the
 * provenance is shown beside it and decides retention — automatic fact
 * eviction only ever drops `model` facts.
 */
export type FactSource = "operator" | "user" | "model";

/** Caller-facing input to `append*Fact`. The store assigns `id` + timestamps. */
export interface FactInput {
  readonly text: string;
  readonly source: FactSource;
}

/** Longest fact the agent's `memory` tool and the consolidator write, in characters. */
export const MAX_FACT_CHARS = 500;

/** Longest working-memory scratchpad the agent's `memory` tool writes, in characters. */
export const MAX_WORKING_MEMORY_CHARS = 4_000;

/**
 * Longest `staticRules` rendered into the prompt, per scope, in characters.
 * Longer rules are truncated by `resolveContext`; write paths reject them.
 */
export const MAX_STATIC_RULES_CHARS = 20_000;

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
 *   raw transcript  →  EpisodicRecord (summary + salience)
 *                                     →  Fact (one-line, indefinite)
 *
 * Storage scope is decided by the writer. Per-thread episodes carry
 * compaction: the latest one with `metadata.kind === "compact"` is
 * rendered by `resolveContext` in place of the messages it covers.
 * Per-resource episodes power cross-thread recall ("what did this user
 * and I work on last week?"). Per-namespace episodes are rare but
 * supported for org-wide events.
 *
 * The base store treats episodes as plain rows and sorts them by
 * `salience`, `createdAt` or `occurredAt` only.
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
  /**
   * Defaults to "salienceDesc". `sourceRangeDesc` puts the episode covering
   * the newest messages first (highest `sourceMessageRange.toSeq`; episodes
   * without a range last). Ties go to the later insert.
   */
  readonly order?: "salienceDesc" | "createdDesc" | "occurredDesc" | "sourceRangeDesc";
  /** Only episodes whose `metadata.kind` equals this (e.g. `"compact"`, `"distill"`). */
  readonly kind?: string;
  /** Only episodes distilled from this thread (`sourceThreadId`). */
  readonly sourceThreadId?: string;
}

/** Episode `metadata.kind` of a thread compaction (`Consolidator.compactThread`). */
export const COMPACT_EPISODE_KIND = "compact";
/** Episode `metadata.kind` of a thread distillation (`Consolidator.distillThread`). */
export const DISTILL_EPISODE_KIND = "distill";

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
   * on the thread's metadata with an equal JSON value (same type: `true`
   * does not match `1`). Entries whose value is `undefined` are ignored.
   */
  readonly metadataFilter?: Readonly<Record<string, unknown>>;
  /**
   * Case-insensitive substring match on `threadId`. Implementations
   * SHOULD push this to the storage layer (SQL LIKE, FTS, etc.) rather
   * than filtering after the fact, so dropdown searches scale beyond
   * the page-size hint.
   */
  readonly q?: string;
  /** Page size. Default `DEFAULT_THREAD_PAGE_SIZE`. */
  readonly limit?: number;
  /**
   * Continuation token: `nextThreadCursor({ thread, order })` of the last
   * thread of the previous page (same order and filters). The next page
   * starts right after that thread.
   */
  readonly cursor?: string;
  /** Defaults to "lastActiveDesc". */
  readonly order?: "lastActiveDesc" | "createdAsc" | "createdDesc";
  /**
   * `false`: only active threads (archivedAt null). `true`: only archived
   * threads. Omitted: every thread, archived or not.
   */
  readonly archived?: boolean;
}

/** Default `ListThreadsParams.limit`. */
export const DEFAULT_THREAD_PAGE_SIZE = 50;

/** Options of `MemoryStore.appendMessages`. */
export interface AppendMessagesOptions {
  /**
   * Makes the append idempotent per thread: the first append with this key
   * stores the messages; any later one returns them instead of appending
   * again. Thread turns pass one key per turn phase, so a turn re-run after
   * a crash never duplicates history.
   */
  readonly idempotencyKey?: string;
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
 * The cascade is split by trust:
 *
 *   - `instructions` — operator-authored `staticRules` (namespace, then
 *     resource), each capped at `MAX_STATIC_RULES_CHARS`. Authoritative:
 *     runtimes send it as a system block right after the agent persona.
 *   - `reference` — facts, working memory and episodes at every scope,
 *     rendered as ONE fenced reference-data block (see
 *     `fenceReferenceData`). This text is written by users, tools and the
 *     model, so runtimes send it as data (a user-role message), never as
 *     a system message.
 *
 * Both are `""` when empty. `messages` is the trimmed thread tail
 * (oldest-first within budget).
 */
export interface ResolvedContext {
  readonly instructions: string;
  readonly reference: string;
  readonly messages: StoredMessage[];
}

/**
 * A request to record one consolidation run (an LLM distill) against the
 * sliding-window caps of `MemoryStore.reserveConsolidation`.
 */
export interface ConsolidationReservationRequest {
  readonly namespaceId: string;
  readonly resourceId: string;
  /** Operation being counted. Caps count reservations of the same kind. */
  readonly kind: string;
  /** Length of the trailing window, in ms. */
  readonly windowMs: number;
  /** Most reservations per `(namespaceId, resourceId)` inside the window. */
  readonly maxPerResource: number;
  /** Most reservations per `namespaceId` (all resources) inside the window. */
  readonly maxPerNamespace?: number;
}

/** Outcome of `MemoryStore.reserveConsolidation`. */
export type ConsolidationReservation =
  | { readonly granted: true; readonly reservationId: string }
  | {
      readonly granted: false;
      /** Which cap is full. */
      readonly scope: "resource" | "namespace";
      /** Reservations inside the window for that scope. */
      readonly seen: number;
      readonly max: number;
      /** Wait until the oldest reservation of that scope leaves the window. */
      readonly retryAfterMs: number;
    };

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
  appendNamespaceFact(namespaceId: string, fact: FactInput): Promise<Fact>;
  listNamespaceFacts(namespaceId: string): Promise<Fact[]>;
  deleteNamespaceFact(namespaceId: string, factId: string): Promise<void>;
  appendNamespaceEpisode(namespaceId: string, input: EpisodeInput): Promise<EpisodicRecord>;
  listNamespaceEpisodes(namespaceId: string, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteNamespaceEpisode(namespaceId: string, episodeId: string): Promise<void>;

  // --- Resource (user / persona) ---------------------------------------
  getResource(key: ScopedKey): Promise<ResourceRow | null>;
  upsertResource(key: ScopedKey, patch: ResourcePatch): Promise<ResourceRow>;
  appendResourceFact(key: ScopedKey, fact: FactInput): Promise<Fact>;
  listResourceFacts(key: ScopedKey): Promise<Fact[]>;
  deleteResourceFact(key: ScopedKey, factId: string): Promise<void>;
  appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord>;
  listResourceEpisodes(key: ScopedKey, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteResourceEpisode(key: ScopedKey, episodeId: string): Promise<void>;

  // --- Thread ----------------------------------------------------------
  createThread(key: ThreadKey, init?: ThreadInit): Promise<ThreadRow>;
  getThread(key: ThreadKey): Promise<ThreadRow | null>;
  /**
   * One page of the namespace's threads, filtered and ordered by the store
   * (`limit` default `DEFAULT_THREAD_PAGE_SIZE`). `messageCount` and
   * `lastActiveAt` come from the store's own bookkeeping, not a scan.
   */
  listThreads(params: ListThreadsParams): Promise<ThreadSummary[]>;
  setThreadWorking(key: ThreadKey, content: string | null): Promise<void>;
  /** Set the thread's display title. `null` clears it (UI falls back to threadId). */
  setThreadTitle(key: ThreadKey, title: string | null): Promise<void>;
  setThreadMetadata(key: ThreadKey, metadata: Readonly<Record<string, unknown>>): Promise<void>;
  setThreadInheritFromParent(key: ThreadKey, inherit: boolean): Promise<void>;
  /** Archive or restore a thread. Pass `null` to restore (clear archivedAt). */
  setThreadArchived(key: ThreadKey, archivedAt: number | null): Promise<void>;
  appendThreadFact(key: ThreadKey, fact: FactInput): Promise<Fact>;
  listThreadFacts(key: ThreadKey): Promise<Fact[]>;
  deleteThreadFact(key: ThreadKey, factId: string): Promise<void>;
  /**
   * Append messages to the thread's transcript (creating the thread row
   * when missing). With `opts.idempotencyKey`, the append happens at most
   * once per thread: a later call with the same key appends nothing and
   * returns the messages the first call stored.
   */
  appendMessages(
    key: ThreadKey,
    msgs: ReadonlyArray<Message>,
    opts?: AppendMessagesOptions,
  ): Promise<StoredMessage[]>;
  /**
   * Messages of the thread inside `range`, ordered by `range.order`
   * (default oldest-first). Without `limit` every message in range is
   * returned; readers of a live thread pass a limit.
   */
  getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]>;
  appendThreadEpisode(key: ThreadKey, input: EpisodeInput): Promise<EpisodicRecord>;
  listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]>;
  deleteThreadEpisode(key: ThreadKey, episodeId: string): Promise<void>;
  deleteThread(key: ThreadKey): Promise<void>;

  // --- Consolidation rate limit ---------------------------------------
  /**
   * Atomically count the `kind` reservations inside the trailing
   * `windowMs` for the resource and (when `maxPerNamespace` is set) the
   * namespace, and record a new one only when both are under their caps.
   * Two concurrent callers never both take the last slot. Reservations
   * older than the window are dropped on the way.
   */
  reserveConsolidation(request: ConsolidationReservationRequest): Promise<ConsolidationReservation>;
  /** Give back a reservation whose run did not happen (it failed or was a no-op). */
  releaseConsolidation(params: { namespaceId: string; reservationId: string }): Promise<void>;

  // --- Cascade & assembly ----------------------------------------------
  /**
   * Build the prompt-ready view for a thread by merging namespace +
   * resource + thread layers per the documented cascade. Honors
   * `inheritFromParent` severance, renders the thread's latest
   * compaction in place of the messages it folded, and reads only the
   * message tail that fits `budget` (see `loadResolvedContext`). A thread
   * without a row resolves as an empty thread under the namespace and
   * resource layers of `key`.
   */
  resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext>;
}
