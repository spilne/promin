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
  readonly tag?: string;
}

/**
 * Runtime registry for concrete Retriever implementations.
 *
 * This stores live objects, not durable knowledge-base metadata. Hosts like
 * Zorya can build it from config, Postgres/pgvector adapters, or UI-managed
 * knowledge bases, then pass it to `resolveLocalAgent`.
 */
export interface RetrieverRegistry {
  register(input: RegisterRetrieverInput): RegisteredRetriever;
  get(id: string): RegisteredRetriever | null;
  list(params?: ListRetrieversParams): RegisteredRetriever[];
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
    const row: RegisteredRetriever = {
      id: input.id,
      retriever: input.retriever,
      description: input.description,
      tags: [...(input.tags ?? [])],
      metadata: { ...(input.metadata ?? {}) },
    };
    this.rows.set(input.id, row);
    return row;
  }

  get(id: string): RegisteredRetriever | null {
    return this.rows.get(id) ?? null;
  }

  list(params: ListRetrieversParams = {}): RegisteredRetriever[] {
    const rows = [...this.rows.values()].sort((a, b) => a.id.localeCompare(b.id));
    if (params.tag === undefined) return rows;
    return rows.filter((row) => row.tags.includes(params.tag!));
  }

  unregister(id: string): void {
    this.rows.delete(id);
  }
}

export function createInMemoryRetrieverRegistry(
  inputs: ReadonlyArray<RegisterRetrieverInput> = [],
): InMemoryRetrieverRegistry {
  return new InMemoryRetrieverRegistry(inputs);
}

export function isRetrieverRegistry(value: unknown): value is RetrieverRegistry {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as RetrieverRegistry).get === "function" &&
    typeof (value as RetrieverRegistry).list === "function" &&
    typeof (value as RetrieverRegistry).register === "function"
  );
}
