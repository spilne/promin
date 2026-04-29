// ---------------------------------------------------------------------------
// `InMemoryMemoryStore` — reference implementation of the layered
// `MemoryStore` interface. Three `Map`s plus a per-thread message array.
//
// Auto-creates parent rows on `appendXFact` / `setThreadX` so callers
// don't have to upsert first. `createThread` is strict — throws if the
// thread already exists. Use `getThread` first when uncertain.
//
// Pluggable `Clock` (defaults to `SystemClock`) so tests can drive
// `createdAt` / `updatedAt` deterministically.
// ---------------------------------------------------------------------------

import { randomUUID } from "node:crypto";
import { SystemClock, type Clock } from "@promin/core";
import type { Message } from "../message.ts";
import { resolveContext } from "./resolve-context.ts";
import type {
  EpisodeInput,
  EpisodeListParams,
  EpisodicRecord,
  Fact,
  ListThreadsParams,
  MemoryStore,
  MessageRange,
  NamespacePatch,
  NamespaceRow,
  ResolvedContext,
  ResourcePatch,
  ResourceRow,
  ScopedKey,
  StoredMessage,
  ThreadInit,
  ThreadKey,
  ThreadRow,
  ThreadSummary,
  TokenBudget,
} from "./types.ts";

export interface InMemoryMemoryStoreConfig {
  /** Time source. Default: `SystemClock`. Tests pass a `FakeClock`. */
  readonly clock?: Clock;
}

export class InMemoryMemoryStore implements MemoryStore {
  private readonly clock: Clock;
  private readonly namespaces = new Map<string, NamespaceRow>();
  private readonly resources = new Map<string, ResourceRow>();
  private readonly threads = new Map<string, ThreadRow>();
  private readonly namespaceFacts = new Map<string, Fact[]>();
  private readonly resourceFacts = new Map<string, Fact[]>();
  private readonly threadFacts = new Map<string, Fact[]>();
  private readonly messages = new Map<string, StoredMessage[]>();
  private readonly seqByThread = new Map<string, number>();
  private readonly lastActiveByThread = new Map<string, number>();
  private readonly namespaceEpisodes = new Map<string, EpisodicRecord[]>();
  private readonly resourceEpisodes = new Map<string, EpisodicRecord[]>();
  private readonly threadEpisodes = new Map<string, EpisodicRecord[]>();

  constructor(config: InMemoryMemoryStoreConfig = {}) {
    this.clock = config.clock ?? SystemClock;
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

  async appendNamespaceFact(namespaceId: string, text: string): Promise<Fact> {
    if (!this.namespaces.has(namespaceId)) {
      await this.upsertNamespace(namespaceId, {});
    }
    const f = this.makeFact(text);
    const list = this.namespaceFacts.get(namespaceId) ?? [];
    list.push(f);
    this.namespaceFacts.set(namespaceId, list);
    return f;
  }

  async listNamespaceFacts(namespaceId: string): Promise<Fact[]> {
    return (this.namespaceFacts.get(namespaceId) ?? []).slice();
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
    const ep = this.makeEpisode(input);
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

  async appendResourceFact(key: ScopedKey, text: string): Promise<Fact> {
    if (!this.resources.has(this.resourceK(key))) {
      await this.upsertResource(key, {});
    }
    const f = this.makeFact(text);
    const list = this.resourceFacts.get(this.resourceK(key)) ?? [];
    list.push(f);
    this.resourceFacts.set(this.resourceK(key), list);
    return f;
  }

  async listResourceFacts(key: ScopedKey): Promise<Fact[]> {
    return (this.resourceFacts.get(this.resourceK(key)) ?? []).slice();
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
    const ep = this.makeEpisode(input);
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
      throw new Error(`Thread already exists: ${key.namespaceId}/${key.threadId}`);
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
    const row = this.threads.get(this.threadK(key));
    if (!row) return null;
    // Same legacy fallback as listThreads: surface `metadata.title` as
    // the typed `title` when the column itself is null.
    if (row.title == null && typeof row.metadata.title === "string") {
      return { ...row, title: row.metadata.title };
    }
    return row;
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
          if (t.metadata[k] !== v) return false;
        }
      }
      if (params.archived === true) {
        if (t.archivedAt == null) return false;
      } else if (params.archived === false || params.archived === undefined) {
        if (t.archivedAt != null) return false;
      }
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
        // Read-side fallback: legacy rows wrote `metadata.title` before
        // the column existed. Surface that as `title` so the UI doesn't
        // lose names that were set under the old contract. Newly written
        // titles always go through the typed column.
        title: t.title ?? (typeof t.metadata.title === "string" ? t.metadata.title : null),
        metadata: t.metadata,
        archivedAt: t.archivedAt,
        messageCount: this.messages.get(k)?.length ?? 0,
        lastActiveAt: this.lastActiveByThread.get(k) ?? t.updatedAt,
        createdAt: t.createdAt,
      };
    });

    const order = params.order ?? "lastActiveDesc";
    summaries.sort((a, b) => {
      switch (order) {
        case "lastActiveDesc":
          return b.lastActiveAt - a.lastActiveAt;
        case "createdAsc":
          return a.createdAt - b.createdAt;
        case "createdDesc":
          return b.createdAt - a.createdAt;
      }
    });

    const offset = params.cursor ? parseInt(params.cursor, 10) || 0 : 0;
    const limit = params.limit ?? summaries.length;
    return summaries.slice(offset, offset + limit);
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

  async appendThreadFact(key: ThreadKey, text: string): Promise<Fact> {
    if (!this.threads.has(this.threadK(key))) {
      await this.createThread(key);
    }
    const f = this.makeFact(text);
    const list = this.threadFacts.get(this.threadK(key)) ?? [];
    list.push(f);
    this.threadFacts.set(this.threadK(key), list);
    return f;
  }

  async listThreadFacts(key: ThreadKey): Promise<Fact[]> {
    return (this.threadFacts.get(this.threadK(key)) ?? []).slice();
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

  async appendMessages(key: ThreadKey, msgs: ReadonlyArray<Message>): Promise<StoredMessage[]> {
    if (!this.threads.has(this.threadK(key))) {
      await this.createThread(key);
    }
    const k = this.threadK(key);
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
    const ep = this.makeEpisode(input);
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
    this.lastActiveByThread.delete(k);
  }

  // --- Cascade ----------------------------------------------------------

  async resolveContext(key: ThreadKey, budget: TokenBudget): Promise<ResolvedContext> {
    const thread = await this.getThread(key);
    if (!thread) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    const namespace = await this.getNamespace(key.namespaceId);
    const namespaceFacts = await this.listNamespaceFacts(key.namespaceId);

    const resourceId = thread.resourceId ?? key.resourceId;
    const resource = resourceId
      ? await this.getResource({ namespaceId: key.namespaceId, resourceId })
      : null;
    const resourceFacts = resourceId
      ? await this.listResourceFacts({ namespaceId: key.namespaceId, resourceId })
      : [];

    // Episodes only loaded when the budget opts in. Pulled from the
    // resource scope (cross-thread recall for THIS user); per-thread
    // and per-namespace episodes are storage-only by default.
    const resourceEpisodes =
      resourceId && (budget.maxEpisodeTokens ?? 0) > 0
        ? await this.listResourceEpisodes(
            { namespaceId: key.namespaceId, resourceId },
            { order: "salienceDesc" },
          )
        : [];

    const threadFacts = await this.listThreadFacts(key);
    const messages = await this.getMessages(key);

    return resolveContext({
      namespace,
      namespaceFacts,
      resource,
      resourceFacts,
      resourceEpisodes,
      thread,
      threadFacts,
      messages,
      budget,
    });
  }

  // --- helpers ----------------------------------------------------------

  private async requireThread(key: ThreadKey): Promise<ThreadRow> {
    const row = await this.getThread(key);
    if (!row) {
      throw new Error(`Thread not found: ${key.namespaceId}/${key.threadId}`);
    }
    return row;
  }

  private makeFact(text: string): Fact {
    const now = this.now();
    return { id: randomUUID(), text, createdAt: now, updatedAt: now };
  }

  private makeEpisode(input: EpisodeInput): EpisodicRecord {
    const now = this.now();
    const salience = clamp01(input.salience ?? 0.5);
    return {
      id: randomUUID(),
      summary: input.summary,
      outcome: input.outcome ?? null,
      salience,
      embedding: input.embedding ?? null,
      sourceThreadId: input.sourceThreadId ?? null,
      sourceMessageRange: input.sourceMessageRange ?? null,
      occurredAt: input.occurredAt ?? now,
      createdAt: now,
      metadata: input.metadata ?? {},
    };
  }

  private queryEpisodes(
    list: ReadonlyArray<EpisodicRecord>,
    params?: EpisodeListParams,
  ): EpisodicRecord[] {
    let out = list.slice();
    if (params?.minSalience !== undefined) {
      const threshold = params.minSalience;
      out = out.filter((e) => e.salience >= threshold);
    }
    const order = params?.order ?? "salienceDesc";
    out.sort((a, b) => {
      switch (order) {
        case "salienceDesc":
          return b.salience - a.salience;
        case "createdDesc":
          return b.createdAt - a.createdAt;
        case "occurredDesc":
          return b.occurredAt - a.occurredAt;
      }
    });
    if (params?.limit !== undefined) out = out.slice(0, params.limit);
    return out;
  }
}

function clamp01(n: number): number {
  if (Number.isNaN(n)) return 0.5;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}
