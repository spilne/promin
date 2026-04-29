// ---------------------------------------------------------------------------
// Metadata filter helpers for `SchedulerStorage.listSchedules({ metadata })`.
//
// Semantics mirror Postgres jsonb `@>` containment: a row matches when
// every LEAF path in the filter exists in the row's metadata with a
// deep-equal value. Extra keys in the row's metadata don't fail the match.
//
// Why a separate helper from `workflowMetadataMatches`
// ----------------------------------------------------
// `workflowMetadataMatches` does strict deep-equality on the top-level
// filter keys — useful when callers want exact subtree equality, but
// wrong for the scheduler's common case `{ target: { type: "agent" } }`,
// where the actual schedule's `target` carries extra fields (agentId,
// threadId, ...) and would never satisfy strict equality. Containment
// is what the dashboard, the per-thread drawer, and the SQL backends
// all want.
//
// SQL-friendly walk
// -----------------
// `flattenLeafPaths` produces `[(["target", "type"], "agent"), ...]`
// pairs that translate cleanly to `json_extract(metadata, '$.target.type')
// = 'agent'` in SQLite or `metadata->'target'->>'type' = 'agent'` in
// Postgres. Backends call it once per query to build their WHERE clause.
// ---------------------------------------------------------------------------

/**
 * Postgres jsonb `@>` containment. `actual` matches the `filter` when:
 *   - every key in `filter` exists in `actual`
 *   - leaves (primitives, arrays, nulls) compare via deep-equal
 *   - nested objects recurse: each filter leaf must be present in actual
 *   - extra keys in `actual` are allowed
 */
export function scheduleMetadataContains(
  actual: Record<string, unknown> | undefined | null,
  filter: Record<string, unknown>,
): boolean {
  if (Object.keys(filter).length === 0) return true;
  if (!actual) return false;
  return contains(actual, filter);
}

function contains(actual: unknown, filter: unknown): boolean {
  if (filter === null) return actual === null;
  if (typeof filter !== "object") return actual === filter;
  if (Array.isArray(filter)) return arraysEqual(actual, filter);
  // filter is a non-null, non-array object — recurse per key.
  if (actual === null || typeof actual !== "object" || Array.isArray(actual)) return false;
  const a = actual as Record<string, unknown>;
  const f = filter as Record<string, unknown>;
  for (const k of Object.keys(f)) {
    if (!contains(a[k], f[k])) return false;
  }
  return true;
}

function arraysEqual(actual: unknown, filter: ReadonlyArray<unknown>): boolean {
  if (!Array.isArray(actual) || actual.length !== filter.length) return false;
  for (let i = 0; i < filter.length; i++) {
    if (!contains(actual[i], filter[i])) return false;
  }
  return true;
}

/**
 * Walk a nested filter object and yield `(path, leafValue)` pairs for
 * every primitive / array / null leaf. Nested objects are flattened —
 * `{ target: { type: "agent" } }` becomes `[(["target", "type"], "agent")]`.
 *
 * SQL backends translate each pair into `json_extract(metadata, '$.<path>')
 * = ?` (or the equivalent jsonb path operator in Postgres).
 */
export function flattenLeafPaths(
  filter: Record<string, unknown>,
): Array<{ path: string[]; value: unknown }> {
  const out: Array<{ path: string[]; value: unknown }> = [];
  walk(filter, [], out);
  return out;
}

function walk(node: unknown, path: string[], out: Array<{ path: string[]; value: unknown }>): void {
  if (node === null || typeof node !== "object" || Array.isArray(node)) {
    out.push({ path, value: node });
    return;
  }
  for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
    walk(v, [...path, k], out);
  }
}
