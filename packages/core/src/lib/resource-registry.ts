/**
 * Small synchronous registry primitive for host-owned resources.
 *
 * Domain registries should add their own validation, persistence, versioning,
 * and async lifecycle on top. This intentionally only standardizes identity,
 * replacement, lookup, listing, and removal.
 */
export interface ResourceRegistry<T, K = string> {
  get(key: K): T | null;
  list(): T[];
  set(value: T): T;
  delete(key: K): boolean;
}

export interface InMemoryResourceRegistryConfig<T, K> {
  readonly keyOf: (value: T) => K;
  readonly compare?: (a: T, b: T) => number;
}

export class InMemoryResourceRegistry<T, K = string> implements ResourceRegistry<T, K> {
  private readonly rows = new Map<K, T>();
  private readonly keyOf: (value: T) => K;
  private readonly compare?: (a: T, b: T) => number;

  constructor(config: InMemoryResourceRegistryConfig<T, K>) {
    this.keyOf = config.keyOf;
    this.compare = config.compare;
  }

  get(key: K): T | null {
    return this.rows.get(key) ?? null;
  }

  list(): T[] {
    const values = [...this.rows.values()];
    return this.compare ? values.sort(this.compare) : values;
  }

  set(value: T): T {
    this.rows.set(this.keyOf(value), value);
    return value;
  }

  delete(key: K): boolean {
    return this.rows.delete(key);
  }
}
