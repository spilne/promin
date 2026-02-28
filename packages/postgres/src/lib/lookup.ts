// ---------------------------------------------------------------------------
// defineLookup — maps core string enums to integer IDs for DB compaction
// ---------------------------------------------------------------------------

export interface LookupEntry<TName extends string = string> {
  readonly id: number;
  readonly name: TName;
}

/**
 * A frozen, type-safe lookup mapping string enum values to integer IDs.
 *
 * @example
 * ```ts
 * import type { WorkflowStatus } from "@ts-backend/core";
 *
 * const WorkflowStatusIds = defineLookup<WorkflowStatus>({
 *   running: 1, completed: 2, failed: 3, suspended: 4,
 * });
 *
 * WorkflowStatusIds.toId("running")    // 1
 * WorkflowStatusIds.toName(2)          // "completed"
 * WorkflowStatusIds.id.running         // 1
 * ```
 */
export interface Lookup<T extends string> {
  /** Map string name to integer ID. */
  readonly toId: (name: T) => number;
  /** Map integer ID back to string name. */
  readonly toName: (id: number) => T;
  /** Direct access: id.running → 1 */
  readonly id: Readonly<Record<T, number>>;
  /** All entries as [name, id] pairs. */
  readonly entries: readonly LookupEntry<T>[];
}

/**
 * Define a lookup mapping from a core string enum to integer IDs.
 * The mapping must cover all values in the enum.
 *
 * @example
 * ```ts
 * export const WorkflowStatusIds = defineLookup<WorkflowStatus>({
 *   running: 1, completed: 2, failed: 3, suspended: 4,
 * });
 * ```
 */
export function defineLookup<T extends string>(mapping: Record<T, number>): Lookup<T> {
  const entries: LookupEntry<T>[] = (Object.entries(mapping) as [T, number][]).map(
    ([name, id]) => ({ id, name }),
  );

  // Validate no duplicate IDs
  const seenIds = new Set<number>();
  for (const { name, id } of entries) {
    if (seenIds.has(id)) {
      throw new Error(`Duplicate ID ${id} for "${name}"`);
    }
    seenIds.add(id);
  }

  const nameToId = new Map<string, number>(entries.map((e) => [e.name, e.id]));
  const idToName = new Map<number, string>(entries.map((e) => [e.id, e.name]));

  return Object.freeze({
    toId: (name: T): number => {
      const id = nameToId.get(name);
      if (id === undefined) throw new Error(`Unknown lookup name: "${name}"`);
      return id;
    },
    toName: (id: number): T => {
      const name = idToName.get(id);
      if (name === undefined) throw new Error(`Unknown lookup id: ${id}`);
      return name as T;
    },
    id: Object.freeze({ ...mapping }),
    entries: Object.freeze(entries),
  });
}
