// ---------------------------------------------------------------------------
// RetrieverRegistry — the host's table of live `Retriever` instances, keyed
// by the id recipes reference in `backend.knowledge[].id`.
//
// A row is either global (no `namespaceId`: a host-wired retriever every
// namespace may bind) or belongs to one namespace (a managed knowledge
// base). An agent running in a namespace sees the global rows plus that
// namespace's; a namespace row shadows a global row with the same id.
//
// It holds runtime objects, not durable knowledge-base definitions: a host
// builds it from config, a pgvector table, or managed knowledge bases, and
// passes it to `resolveLocalAgent({ retrievers })`. The zorya agent editor
// lists its metadata (never the retriever object) to populate its picker.
// ---------------------------------------------------------------------------

import type { Retriever } from "./types.ts";

export interface RegisteredRetriever {
  readonly id: string;
  /** Namespace the row belongs to; absent for a global row. */
  readonly namespaceId?: string;
  readonly retriever: Retriever;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface RegisterRetrieverInput {
  readonly id: string;
  /** Register in this namespace only. Omit for a global row. */
  readonly namespaceId?: string;
  readonly retriever: Retriever;
  readonly description?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

/** A retriever id as seen from a namespace (or globally, without one). */
export interface RetrieverKey {
  readonly id: string;
  readonly namespaceId?: string;
}

export interface ListRetrieversParams {
  /** Include this namespace's rows next to the global ones. Omit: global rows only. */
  readonly namespaceId?: string;
  /** Keep only retrievers carrying this tag. */
  readonly tag?: string;
}

export interface RetrieverRegistry {
  /** Add a retriever, replacing any row registered under the same id and namespace. */
  register(input: RegisterRetrieverInput): RegisteredRetriever;
  /**
   * The row an agent in `key.namespaceId` binds as `key.id`: the
   * namespace's row, else the global one. Without a namespace: the global
   * row only.
   */
  get(key: RetrieverKey): RegisteredRetriever | null;
  /**
   * `get` for a resolve: a registry backed by a shared store (managed
   * knowledge bases) re-reads the store when it has no row of the key's
   * namespace, so a retriever created by another process resolves here
   * too. A plain registry answers like `get`.
   */
  resolve(key: RetrieverKey): Promise<RegisteredRetriever | null>;
  /** Rows visible from `params.namespaceId` (see `get`), ordered by id. */
  list(params?: ListRetrieversParams): RegisteredRetriever[];
  /** Remove the row registered under exactly this id and namespace. Absent: no-op. */
  unregister(key: RetrieverKey): void;
}

export class InMemoryRetrieverRegistry implements RetrieverRegistry {
  private readonly rows = new Map<string, RegisteredRetriever>();

  constructor(inputs: ReadonlyArray<RegisterRetrieverInput> = []) {
    for (const input of inputs) {
      this.register(input);
    }
  }

  register(input: RegisterRetrieverInput): RegisteredRetriever {
    if (!input.id.trim()) throw new Error("InMemoryRetrieverRegistry: id must not be empty");
    if (input.namespaceId !== undefined && !input.namespaceId.trim()) {
      throw new Error("InMemoryRetrieverRegistry: namespaceId must not be empty");
    }
    const row: RegisteredRetriever = {
      id: input.id,
      ...(input.namespaceId !== undefined && { namespaceId: input.namespaceId }),
      retriever: input.retriever,
      ...(input.description !== undefined && { description: input.description }),
      tags: [...(input.tags ?? [])],
      metadata: { ...input.metadata },
    };
    this.rows.set(rowKey(input), row);
    return row;
  }

  get(key: RetrieverKey): RegisteredRetriever | null {
    if (key.namespaceId !== undefined) {
      const scoped = this.rows.get(rowKey(key));
      if (scoped) return scoped;
    }
    return this.rows.get(rowKey({ id: key.id })) ?? null;
  }

  async resolve(key: RetrieverKey): Promise<RegisteredRetriever | null> {
    return this.get(key);
  }

  list(params: ListRetrieversParams = {}): RegisteredRetriever[] {
    const visible = new Map<string, RegisteredRetriever>();
    for (const row of this.rows.values()) {
      if (row.namespaceId === undefined) {
        if (!visible.has(row.id)) visible.set(row.id, row);
      } else if (row.namespaceId === params.namespaceId) {
        visible.set(row.id, row);
      }
    }
    const rows = [...visible.values()].sort((a, b) => a.id.localeCompare(b.id));
    const { tag } = params;
    if (tag === undefined) return rows;
    return rows.filter((row) => row.tags.includes(tag));
  }

  unregister(key: RetrieverKey): void {
    this.rows.delete(rowKey(key));
  }
}

function rowKey(key: RetrieverKey): string {
  return JSON.stringify([key.namespaceId ?? null, key.id]);
}
