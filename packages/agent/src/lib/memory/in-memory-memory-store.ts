// ---------------------------------------------------------------------------
// `InMemoryMemoryStore` — reference implementation of the layered
// `MemoryStore` interface. Three `Map`s plus a per-thread message array.
//
// Auto-creates parent rows on `appendXFact` / `setThreadX` so callers
// don't have to upsert first. `createThread` is strict — throws if the
// thread already exists. Use `getThread` first when uncertain.
//
// Pluggable `WallClock` (defaults to `SystemWallClock`) so tests can drive
// `createdAt` / `updatedAt` deterministically.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type { Message } from "../message.ts";
import { ThreadExistsError } from "../agent/errors.ts";
import { resolveFactConfidence } from "./fact-confidence.ts";
import { relationKey } from "./relations.ts";
import { appendStaticRule, normalizeRuleText } from "./rule-proposals.ts";
import { newEpisode, newFact } from "./record-factories.ts";
import { loadResolvedContext } from "./load-context.ts";
import { decayedSalience } from "./scoring.ts";
import { isAfterThreadCursor, parseThreadCursor } from "./thread-cursor.ts";
import {
  DEFAULT_THREAD_PAGE_SIZE,
  type AppendMessagesOptions,
  type ConsolidationReservation,
  type ConsolidationReservationRequest,
  type EpisodeInput,
  type EpisodeListParams,
  type EpisodicRecord,
  type Fact,
  type FactInput,
  type FactListParams,
  type ListThreadsParams,
  MemoryLimitError,
  type MemoryStore,
  type MessageRange,
  type NamespacePatch,
  type NamespaceRow,
  type Relation,
  type RelationInput,
  type ResolvedContext,
  type ResourcePatch,
  type ResourceRow,
  type RuleProposal,
  type RuleProposalListParams,
  type ScopedKey,
  type StoredMessage,
  type ThreadInit,
  type ThreadKey,
  type ThreadRow,
  type ThreadSummary,
  type TokenBudget,
} from "./types.ts";

export interface InMemoryMemoryStoreConfig {
  /** Time source. Default: `SystemWallClock`. Tests pass a `FakeWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryMemoryStore implements MemoryStore {
  private readonly clock: WallClock;
  private readonly namespaces = new Map<string, NamespaceRow>();
  private readonly resources = new Map<string, ResourceRow>();
  private readonly threads = new Map<string, ThreadRow>();
  private readonly namespaceFacts = new Map<string, Fact[]>();
  private readonly resourceFacts = new Map<string, Fact[]>();
  private readonly resourceRelations = new Map<string, Relation[]>();
  private readonly resourceProposals = new Map<string, RuleProposal[]>();
  private readonly threadFacts = new Map<string, Fact[]>();
  private readonly messages = new Map<string, StoredMessage[]>();
  private readonly seqByThread = new Map<string, number>();
  /** Idempotent appends per thread: idempotency key → the messages stored. */
  private readonly appendsByThread = new Map<string, Map<string, StoredMessage[]>>();
  private readonly lastActiveByThread = new Map<string, number>();
  private readonly namespaceEpisodes = new Map<string, EpisodicRecord[]>();
  private readonly resourceEpisodes = new Map<string, EpisodicRecord[]>();
  private readonly threadEpisodes = new Map<string, EpisodicRecord[]>();
  private reservations: Array<{
    readonly id: string;
    readonly namespaceId: string;
    readonly resourceId: string;
    readonly kind: string;
    readonly at: number;
  }> = [];

  constructor(config: InMemoryMemoryStoreConfig = {}) {
    this.clock = config.clock ?? SystemWallClock;
  }

  private now(): number {
    return this.clock.currentTimeMs();
  }

  private resourceK(key: ScopedKey): string {
    return `${key.namespaceId}|${key.resourceId}`;
  }

  private threadK(key: ThreadKey): string {
    return `${key.namespaceId}|${key.threadId}`;
  }

  // --- Namespace --------------------------------------------------------

  async getNamespace(namespaceId: string): Promise<NamespaceRow | null> {
    return this.namespaces.get(namespaceId) ?? null;
  }

  async upsertNamespace(namespaceId: string, patch: NamespacePatch): Promise<NamespaceRow> {
    const existing = this.namespaces.get(namespaceId);
    const now = this.now();
    const next: NamespaceRow = existing
      ? {
          ...existing,
          staticRules: patch.staticRules !== undefined ? patch.staticRules : existing.staticRules,
          workingMemory:
            patch.workingMemory !== undefined ? patch.workingMemory : existing.workingMemory,
          inheritFromParent: patch.inheritFromParent ?? existing.inheritFromParent,
          metadata: patch.metadata ?? existing.metadata,
          updatedAt: now,
        }
      : {
          namespaceId,
          staticRules: patch.staticRules ?? null,
          workingMemory: patch.workingMemory ?? null,
          inheritFromParent: patch.inheritFromParent ?? true,
          metadata: patch.metadata ?? {},
          createdAt: now,
          updatedAt: now,
        };
    this.namespaces.set(namespaceId, next);
    return next;
  }

  async appendNamespaceFact(namespaceId: string, input: FactInput): Promise<Fact> {
    if (!this.namespaces.has(namespaceId)) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = newFact({ input, now: this.now() });
    const list = this.namespaceFacts.get(namespaceId) ?? [];
    this.retireSuperseded({ list, input, successor: f });
    list.push(f);
    this.namespaceFacts.set(namespaceId, list);
    return f;
  }

  async listNamespaceFacts(namespaceId: string, params?: FactListParams): Promise<Fact[]> {
    return listFacts({ facts: this.namespaceFacts.get(namespaceId) ?? [], params });
  }

  async deleteNamespaceFact(namespaceId: string, factId: string): Promise<void> {
    const list = this.namespaceFacts.get(namespaceId);
    if (!list) return;
    this.namespaceFacts.set(
      namespaceId,
      list.filter((f) => f.id !== factId),
    );
  }

  async appendNamespaceEpisode(namespaceId: string, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!this.namespaces.has(namespaceId)) {
      await this.upsertNamespace(namespaceId, {});
    }
    const ep = newEpisode({ input, now: this.now() });
    const list = this.namespaceEpisodes.get(namespaceId) ?? [];
    list.push(ep);
    this.namespaceEpisodes.set(namespaceId, list);
    return ep;
  }

  async listNamespaceEpisodes(
    namespaceId: string,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(this.namespaceEpisodes.get(namespaceId) ?? [], params);
  }

  async deleteNamespaceEpisode(namespaceId: string, episodeId: string): Promise<void> {
    const list = this.namespaceEpisodes.get(namespaceId);
    if (!list) return;
    this.namespaceEpisodes.set(
      namespaceId,
      list.filter((e) => e.id !== episodeId),
    );
  }

  // --- Resource ---------------------------------------------------------

  async getResource(key: ScopedKey): Promise<ResourceRow | null> {
    return this.resources.get(this.resourceK(key)) ?? null;
  }

  async upsertResource(key: ScopedKey, patch: ResourcePatch): Promise<ResourceRow> {
    return this.setResource(key, patch);
  }

  private setResource(key: ScopedKey, patch: ResourcePatch): ResourceRow {
    const k = this.resourceK(key);
    const existing = this.resources.get(k);
    const now = this.now();
    const next: ResourceRow = existing
      ? {
          ...existing,
          staticRules: patch.staticRules !== undefined ? patch.staticRules : existing.staticRules,
          workingMemory:
            patch.workingMemory !== undefined ? patch.workingMemory : existing.workingMemory,
          inheritFromParent: patch.inheritFromParent ?? existing.inheritFromParent,
          metadata: patch.metadata ?? existing.metadata,
          updatedAt: now,
        }
      : {
          namespaceId: key.namespaceId,
          resourceId: key.resourceId,
          staticRules: patch.staticRules ?? null,
          workingMemory: patch.workingMemory ?? null,
          inheritFromParent: patch.inheritFromParent ?? true,
          metadata: patch.metadata ?? {},
          createdAt: now,
          updatedAt: now,
        };
    this.resources.set(k, next);
    return next;
  }

  async appendResourceStaticRule(
    key: ScopedKey,
    params: { readonly text: string; readonly maxChars: number },
  ): Promise<ResourceRow> {
    return this.appendStaticRuleNow({ key, ...params });
  }

  private appendStaticRuleNow(params: {
    readonly key: ScopedKey;
    readonly text: string;
    readonly maxChars: number;
  }): ResourceRow {
    const existing = this.resources.get(this.resourceK(params.key));
    const staticRules = appendStaticRule({
      existing: existing?.staticRules ?? null,
      text: params.text,
      maxChars: params.maxChars,
    });
    return this.setResource(params.key, { staticRules });
  }

  async appendResourceFact(key: ScopedKey, input: FactInput): Promise<Fact> {
    if (!this.resources.has(this.resourceK(key))) {
      await this.upsertResource(key, {});
    }
    const f = newFact({ input, now: this.now() });
    const list = this.resourceFacts.get(this.resourceK(key)) ?? [];
    this.retireSuperseded({ list, input, successor: f });
    list.push(f);
    this.resourceFacts.set(this.resourceK(key), list);
    return f;
  }

  async appendResourceRelation(
    key: ScopedKey,
    input: RelationInput,
    options?: { readonly maxRelations?: number },
  ): Promise<Relation> {
    if (!this.resources.has(this.resourceK(key))) {
      await this.upsertResource(key, {});
    }
    const k = this.resourceK(key);
    const list = this.resourceRelations.get(k) ?? [];
    const identity = relationKey(input);
    const existing = list.find((r) => relationKey(r) === identity);
    if (existing) return { ...existing };
    if (options?.maxRelations !== undefined && list.length >= options.maxRelations) {
      throw new MemoryLimitError(`Resource already has ${options.maxRelations} relations.`);
    }
    const relation: Relation = {
      id: randomUUID(),
      subject: input.subject.trim(),
      predicate: input.predicate.trim(),
      object: input.object.trim(),
      source: input.source,
      confidence: resolveFactConfidence(input),
      createdAt: this.now(),
    };
    list.push(relation);
    this.resourceRelations.set(k, list);
    return { ...relation };
  }

  async recordResourceRuleProposal(
    key: ScopedKey,
    input: { readonly text: string; readonly threadId: string },
    options?: { readonly maxPending?: number },
  ): Promise<RuleProposal> {
    if (!this.resources.has(this.resourceK(key))) {
      await this.upsertResource(key, {});
    }
    const k = this.resourceK(key);
    const list = this.resourceProposals.get(k) ?? [];
    const identity = normalizeRuleText(input.text);
    const now = this.now();
    const at = list.findIndex((p) => normalizeRuleText(p.text) === identity);
    if (at >= 0) {
      const p = list[at]!;
      const evidence = p.evidenceThreadIds.includes(input.threadId)
        ? p.evidenceThreadIds
        : [...p.evidenceThreadIds, input.threadId];
      const next = {
        ...p,
        evidenceThreadIds: evidence,
        occurrences: evidence.length,
        lastSeenAt: now,
      };
      list[at] = next;
      return copyProposal(next);
    }
    if (
      options?.maxPending !== undefined &&
      list.filter((p) => p.status === "pending").length >= options.maxPending
    ) {
      throw new MemoryLimitError(`Resource already has ${options.maxPending} pending proposals.`);
    }
    const proposal: RuleProposal = {
      id: randomUUID(),
      text: input.text.trim(),
      status: "pending",
      evidenceThreadIds: [input.threadId],
      occurrences: 1,
      firstSeenAt: now,
      lastSeenAt: now,
    };
    list.push(proposal);
    this.resourceProposals.set(k, list);
    return copyProposal(proposal);
  }

  async listResourceRuleProposals(
    key: ScopedKey,
    params?: RuleProposalListParams,
  ): Promise<RuleProposal[]> {
    return (this.resourceProposals.get(this.resourceK(key)) ?? [])
      .filter(
        (p) =>
          (params?.status === undefined || p.status === params.status) &&
          (params?.minOccurrences === undefined || p.occurrences >= params.minOccurrences),
      )
      .map(copyProposal);
  }

  async decideResourceRuleProposal(
    key: ScopedKey,
    input: { readonly id: string; readonly status: "approved" | "rejected" },
  ): Promise<RuleProposal | null> {
    return this.decideNow({ key, ...input });
  }

  private decideNow(params: {
    readonly key: ScopedKey;
    readonly id: string;
    readonly status: "approved" | "rejected";
  }): RuleProposal | null {
    const list = this.resourceProposals.get(this.resourceK(params.key)) ?? [];
    const at = list.findIndex((p) => p.id === params.id && p.status === "pending");
    if (at < 0) return null;
    const next = { ...list[at]!, status: params.status };
    list[at] = next;
    return copyProposal(next);
  }

  async approveResourceRuleProposal(
    key: ScopedKey,
    params: { readonly id: string; readonly maxChars: number },
  ): Promise<RuleProposal | null> {
    // No await below: the check and both writes run as one step.
    const pending = (this.resourceProposals.get(this.resourceK(key)) ?? []).find(
      (p) => p.id === params.id && p.status === "pending",
    );
    if (!pending) return null;
    this.appendStaticRuleNow({ key, text: pending.text, maxChars: params.maxChars });
    return this.decideNow({ key, id: params.id, status: "approved" });
  }

  async listResourceRelations(key: ScopedKey): Promise<Relation[]> {
    return [...(this.resourceRelations.get(this.resourceK(key)) ?? [])];
  }

  async deleteResourceRelation(key: ScopedKey, relationId: string): Promise<void> {
    const k = this.resourceK(key);
    const list = this.resourceRelations.get(k);
    if (!list) return;
    this.resourceRelations.set(
      k,
      list.filter((r) => r.id !== relationId),
    );
  }

  async listResourceFacts(key: ScopedKey, params?: FactListParams): Promise<Fact[]> {
    return listFacts({ facts: this.resourceFacts.get(this.resourceK(key)) ?? [], params });
  }

  async deleteResourceFact(key: ScopedKey, factId: string): Promise<void> {
    const k = this.resourceK(key);
    const list = this.resourceFacts.get(k);
    if (!list) return;
    this.resourceFacts.set(
      k,
      list.filter((f) => f.id !== factId),
    );
  }

  async appendResourceEpisode(key: ScopedKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!this.resources.has(this.resourceK(key))) {
      await this.upsertResource(key, {});
    }
    const ep = newEpisode({ input, now: this.now() });
    const list = this.resourceEpisodes.get(this.resourceK(key)) ?? [];
    list.push(ep);
    this.resourceEpisodes.set(this.resourceK(key), list);
    return ep;
  }

  async listResourceEpisodes(
    key: ScopedKey,
    params?: EpisodeListParams,
  ): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(this.resourceEpisodes.get(this.resourceK(key)) ?? [], params);
  }

  async deleteResourceEpisode(key: ScopedKey, episodeId: string): Promise<void> {
    const k = this.resourceK(key);
    const list = this.resourceEpisodes.get(k);
    if (!list) return;
    this.resourceEpisodes.set(
      k,
      list.filter((e) => e.id !== episodeId),
    );
  }

  // --- Thread -----------------------------------------------------------

  async createThread(key: ThreadKey, init: ThreadInit = {}): Promise<ThreadRow> {
    const k = this.threadK(key);
    if (this.threads.has(k)) {
      throw new ThreadExistsError(key);
    }
    const now = this.now();
    const row: ThreadRow = {
      namespaceId: key.namespaceId,
      resourceId: key.resourceId ?? init.resourceId ?? null,
      threadId: key.threadId,
      title: init.title ?? null,
      workingMemory: init.workingMemory ?? null,
      inheritFromParent: init.inheritFromParent ?? true,
      metadata: init.metadata ?? {},
      archivedAt: init.archivedAt ?? null,
      createdAt: now,
      updatedAt: now,
    };
    this.threads.set(k, row);
    this.lastActiveByThread.set(k, now);
    return row;
  }

  async getThread(key: ThreadKey): Promise<ThreadRow | null> {
    return this.threads.get(this.threadK(key)) ?? null;
  }

  async listThreads(params: ListThreadsParams): Promise<ThreadSummary[]> {
    const all = Array.from(this.threads.values()).filter(
      (t) => t.namespaceId === params.namespaceId,
    );
    const q = params.q?.trim().toLowerCase();
    const filtered = all.filter((t) => {
      if (params.resourceId !== undefined && t.resourceId !== params.resourceId) return false;
      if (q && !t.threadId.toLowerCase().includes(q)) return false;
      if (params.metadataFilter) {
        for (const [k, v] of Object.entries(params.metadataFilter)) {
          if (v === undefined) continue;
          if (!(k in t.metadata) || !isDeepStrictEqual(t.metadata[k], v)) return false;
        }
      }
      if (params.archived === true && t.archivedAt === null) return false;
      if (params.archived === false && t.archivedAt !== null) return false;
      return true;
    });

    const summaries: ThreadSummary[] = filtered.map((t) => {
      const k = this.threadK({
        namespaceId: t.namespaceId,
        threadId: t.threadId,
      });
      return {
        namespaceId: t.namespaceId,
        resourceId: t.resourceId,
        threadId: t.threadId,
        title: t.title,
        metadata: t.metadata,
        archivedAt: t.archivedAt,
        messageCount: this.messages.get(k)?.length ?? 0,
        lastActiveAt: this.lastActiveByThread.get(k) ?? t.updatedAt,
        createdAt: t.createdAt,
      };
    });

    const order = params.order ?? "lastActiveDesc";
    summaries.sort((a, b) => {
      const byOrder =
        order === "lastActiveDesc"
          ? b.lastActiveAt - a.lastActiveAt
          : order === "createdAsc"
            ? a.createdAt - b.createdAt
            : b.createdAt - a.createdAt;
      // Thread id breaks ties, so pages never overlap or skip a thread.
      return byOrder !== 0
        ? byOrder
        : a.threadId < b.threadId
          ? -1
          : a.threadId > b.threadId
            ? 1
            : 0;
    });

    const position = parseThreadCursor(params.cursor);
    const page = position
      ? summaries.filter((s) =>
          isAfterThreadCursor({
            position,
            key: order === "lastActiveDesc" ? s.lastActiveAt : s.createdAt,
            threadId: s.threadId,
            order,
          }),
        )
      : summaries;
    return page.slice(0, params.limit ?? DEFAULT_THREAD_PAGE_SIZE);
  }

  async setThreadWorking(key: ThreadKey, content: string | null): Promise<void> {
    const row = await this.requireThread(key);
    this.threads.set(this.threadK(key), {
      ...row,
      workingMemory: content,
      updatedAt: this.now(),
    });
  }

  async setThreadTitle(key: ThreadKey, title: string | null): Promise<void> {
    const row = await this.requireThread(key);
    this.threads.set(this.threadK(key), {
      ...row,
      title,
      updatedAt: this.now(),
    });
  }

  async setThreadMetadata(
    key: ThreadKey,
    metadata: Readonly<Record<string, unknown>>,
  ): Promise<void> {
    const row = await this.requireThread(key);
    this.threads.set(this.threadK(key), {
      ...row,
      metadata,
      updatedAt: this.now(),
    });
  }

  async setThreadInheritFromParent(key: ThreadKey, inherit: boolean): Promise<void> {
    const row = await this.requireThread(key);
    this.threads.set(this.threadK(key), {
      ...row,
      inheritFromParent: inherit,
      updatedAt: this.now(),
    });
  }

  async setThreadArchived(key: ThreadKey, archivedAt: number | null): Promise<void> {
    const row = await this.requireThread(key);
    this.threads.set(this.threadK(key), {
      ...row,
      archivedAt,
      updatedAt: this.now(),
    });
  }

  async appendThreadFact(key: ThreadKey, input: FactInput): Promise<Fact> {
    if (!this.threads.has(this.threadK(key))) {
      await this.createThread(key);
    }
    const f = newFact({ input, now: this.now() });
    const list = this.threadFacts.get(this.threadK(key)) ?? [];
    this.retireSuperseded({ list, input, successor: f });
    list.push(f);
    this.threadFacts.set(this.threadK(key), list);
    return f;
  }

  async listThreadFacts(key: ThreadKey, params?: FactListParams): Promise<Fact[]> {
    return listFacts({ facts: this.threadFacts.get(this.threadK(key)) ?? [], params });
  }

  async deleteThreadFact(key: ThreadKey, factId: string): Promise<void> {
    const k = this.threadK(key);
    const list = this.threadFacts.get(k);
    if (!list) return;
    this.threadFacts.set(
      k,
      list.filter((f) => f.id !== factId),
    );
  }

  async appendMessages(
    key: ThreadKey,
    msgs: ReadonlyArray<Message>,
    opts?: AppendMessagesOptions,
  ): Promise<StoredMessage[]> {
    if (!this.threads.has(this.threadK(key))) {
      await this.createThread(key);
    }
    const k = this.threadK(key);
    const idempotencyKey = opts?.idempotencyKey;
    if (idempotencyKey !== undefined) {
      const done = this.appendsByThread.get(k)?.get(idempotencyKey);
      if (done) return done;
    }
    const now = this.now();
    let seq = this.seqByThread.get(k) ?? 0;
    const stored: StoredMessage[] = msgs.map((m) => {
      seq += 1;
      return { ...m, seq, createdAt: now } as StoredMessage;
    });
    const list = this.messages.get(k) ?? [];
    list.push(...stored);
    this.messages.set(k, list);
    this.seqByThread.set(k, seq);
    this.lastActiveByThread.set(k, now);
    if (idempotencyKey !== undefined) {
      const appends = this.appendsByThread.get(k) ?? new Map<string, StoredMessage[]>();
      appends.set(idempotencyKey, stored);
      this.appendsByThread.set(k, appends);
    }
    return stored;
  }

  async getMessages(key: ThreadKey, range?: MessageRange): Promise<StoredMessage[]> {
    const list = this.messages.get(this.threadK(key)) ?? [];
    let filtered = list;
    if (range?.fromSeq !== undefined) {
      filtered = filtered.filter((m) => m.seq >= range.fromSeq!);
    }
    if (range?.toSeq !== undefined) {
      filtered = filtered.filter((m) => m.seq <= range.toSeq!);
    }
    const order = range?.order ?? "asc";
    if (order === "desc") {
      filtered = filtered.slice().reverse();
    } else {
      filtered = filtered.slice();
    }
    if (range?.limit !== undefined) {
      filtered = filtered.slice(0, range.limit);
    }
    return filtered;
  }

  async appendThreadEpisode(key: ThreadKey, input: EpisodeInput): Promise<EpisodicRecord> {
    if (!this.threads.has(this.threadK(key))) {
      await this.createThread(key);
    }
    const ep = newEpisode({ input, now: this.now() });
    const list = this.threadEpisodes.get(this.threadK(key)) ?? [];
    list.push(ep);
    this.threadEpisodes.set(this.threadK(key), list);
    return ep;
  }

  async listThreadEpisodes(key: ThreadKey, params?: EpisodeListParams): Promise<EpisodicRecord[]> {
    return this.queryEpisodes(this.threadEpisodes.get(this.threadK(key)) ?? [], params);
  }

  async deleteThreadEpisode(key: ThreadKey, episodeId: string): Promise<void> {
    const k = this.threadK(key);
    const list = this.threadEpisodes.get(k);
    if (!list) return;
    this.threadEpisodes.set(
      k,
      list.filter((e) => e.id !== episodeId),
    );
  }

  async deleteThread(key: ThreadKey): Promise<void> {
    const k = this.threadK(key);
    this.threads.delete(k);
    this.threadFacts.delete(k);
    this.threadEpisodes.delete(k);
    this.messages.delete(k);
    this.seqByThread.delete(k);
    this.appendsByThread.delete(k);
    this.lastActiveByThread.delete(k);
  }

  // --- Consolidation rate limit ----------------------------------------

  async reserveConsolidation(
    request: ConsolidationReservationRequest,
  ): Promise<ConsolidationReservation> {
    // Synchronous from here on: no other call interleaves between the
    // count and the insert.
    const now = this.now();
    const since = now - request.windowMs;
    this.reservations = this.reservations.filter(
      (r) => !(r.namespaceId === request.namespaceId && r.kind === request.kind && r.at <= since),
    );
    const inNamespace = this.reservations.filter(
      (r) => r.namespaceId === request.namespaceId && r.kind === request.kind,
    );
    const inResource = inNamespace.filter((r) => r.resourceId === request.resourceId);
    const denied = (scope: "resource" | "namespace", rows: typeof inNamespace, max: number) => ({
      granted: false as const,
      scope,
      seen: rows.length,
      max,
      retryAfterMs: Math.max(0, Math.min(...rows.map((r) => r.at)) + request.windowMs - now),
    });
    if (inResource.length >= request.maxPerResource) {
      return denied("resource", inResource, request.maxPerResource);
    }
    if (request.maxPerNamespace !== undefined && inNamespace.length >= request.maxPerNamespace) {
      return denied("namespace", inNamespace, request.maxPerNamespace);
    }
    const id = randomUUID();
    this.reservations.push({
      id,
      namespaceId: request.namespaceId,
      resourceId: request.resourceId,
      kind: request.kind,
      at: now,
    });
    return { granted: true, reservationId: id };
  }

  async releaseConsolidation(params: {
    namespaceId: string;
    reservationId: string;
  }): Promise<void> {
    this.reservations = this.reservations.filter(
      (r) => !(r.namespaceId === params.namespaceId && r.id === params.reservationId),
    );
  }

  // --- Cascade ----------------------------------------------------------

  async resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext> {
    return loadResolvedContext({ store: this, key, budget });
  }

  // --- helpers ----------------------------------------------------------

  private async requireThread(key: ThreadKey): Promise<ThreadRow> {
    const row = await this.getThread(key);
    if (!row) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    return row;
  }

  /** Retire the current fact `input.supersedes` names in `list`, in place. */
  private retireSuperseded(params: {
    readonly list: Fact[];
    readonly input: FactInput;
    readonly successor: Fact;
  }): void {
    const { list, input, successor } = params;
    if (input.supersedes === undefined) return;
    const at = list.findIndex((f) => f.id === input.supersedes && f.retiredAt === null);
    if (at < 0) return;
    list[at] = { ...list[at]!, retiredAt: successor.createdAt, supersededBy: successor.id };
  }

  private queryEpisodes(
    list: ReadonlyArray<EpisodicRecord>,
    params?: EpisodeListParams,
  ): EpisodicRecord[] {
    let out = list.slice();
    if (params?.kind !== undefined) {
      const kind = params.kind;
      out = out.filter((e) => e.metadata["kind"] === kind);
    }
    if (params?.sourceThreadId !== undefined) {
      const source = params.sourceThreadId;
      out = out.filter((e) => e.sourceThreadId === source);
    }
    if (params?.minSalience !== undefined) {
      const threshold = params.minSalience;
      out = out.filter((e) => e.salience >= threshold);
    }
    if (params?.anyTerms && params.anyTerms.length > 0) {
      const terms = params.anyTerms.map((t) => t.toLowerCase());
      out = out.filter((e) => {
        const text = `${e.summary}\n${e.outcome ?? ""}`.toLowerCase();
        return terms.some((t) => text.includes(t));
      });
    }
    const order = params?.order ?? "salienceDesc";
    const now = params?.now ?? this.clock.currentTimeMs();
    const key = (e: EpisodicRecord): number =>
      order === "salienceDesc"
        ? e.salience
        : order === "decayedSalienceDesc"
          ? decayedSalience({
              salience: e.salience,
              occurredAt: e.occurredAt,
              now,
              halfLifeMs: params?.halfLifeMs,
            })
          : order === "createdDesc"
            ? e.createdAt
            : order === "occurredDesc"
              ? e.occurredAt
              : (e.sourceMessageRange?.toSeq ?? -1);
    // Ties go to the later insert, like the SQL stores' insertion sequence.
    const ranked = out.map((e, index) => ({ e, index }));
    ranked.sort((a, b) => key(b.e) - key(a.e) || b.index - a.index);
    out = ranked.map((r) => r.e);
    if (params?.limit !== undefined) out = out.slice(0, params.limit);
    return out;
  }
}

/** A proposal the caller can hold without sharing the store's evidence array. */
function copyProposal(p: RuleProposal): RuleProposal {
  return { ...p, evidenceThreadIds: [...p.evidenceThreadIds] };
}

/** Current facts in insertion order; with `includeRetired`, retired ones follow. */
function listFacts(params: {
  readonly facts: ReadonlyArray<Fact>;
  readonly params: FactListParams | undefined;
}): Fact[] {
  const current = params.facts.filter((f) => f.retiredAt === null);
  if (!params.params?.includeRetired) return current;
  return [...current, ...params.facts.filter((f) => f.retiredAt !== null)];
}
