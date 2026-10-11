// ---------------------------------------------------------------------------
// RetrieverRegistry — the host's table of live `Retriever` instances, keyed
// by the id recipes reference in `backend.knowledge[].id`.
//
// It holds runtime objects, not durable knowledge-base definitions: a host
// builds it from config, a pgvector table, or managed knowledge bases, and
// passes it to `resolveLocalAgent({ retrievers })`. The zorya agent editor
// lists its metadata (never the retriever object) to populate its picker.
// ---------------------------------------------------------------------------

import type { Retriever } from "./types.ts";

export interface RegisteredRetriever {
  readonly id: string;
  readonly retriever: Retriever;
  readonly description?: string;
  readonly tags: ReadonlyArray<string>;
  readonly metadata: Readonly<Record<string, unknown>>;
}

export interface RegisterRetrieverInput {
  readonly id: string;
  readonly retriever: Retriever;
  readonly description?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ListRetrieversParams {
  /** Keep only retrievers carrying this tag. */
  readonly tag?: string;
}

export interface RetrieverRegistry {
  /** Add a retriever, replacing any row already registered under `input.id`. */
  register(input: RegisterRetrieverInput): RegisteredRetriever;
  get(id: string): RegisteredRetriever | null;
  /** Rows ordered by id. */
  list(params?: ListRetrieversParams): RegisteredRetriever[];
  /** Remove a row. Removing an absent id is a no-op. */
  unregister(id: string): void;
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
    const row: RegisteredRetriever = {
      id: input.id,
      retriever: input.retriever,
      ...(input.description !== undefined && { description: input.description }),
      tags: [...(input.tags ?? [])],
      metadata: { ...input.metadata },
    };
    this.rows.set(input.id, row);
    return row;
  }

  get(id: string): RegisteredRetriever | null {
    return this.rows.get(id) ?? null;
  }

  list(params: ListRetrieversParams = {}): RegisteredRetriever[] {
    const rows = [...this.rows.values()].sort((a, b) => a.id.localeCompare(b.id));
    const { tag } = params;
    if (tag === undefined) return rows;
    return rows.filter((row) => row.tags.includes(tag));
  }

  unregister(id: string): void {
    this.rows.delete(id);
  }
}
