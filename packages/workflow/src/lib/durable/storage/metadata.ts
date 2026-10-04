// ---------------------------------------------------------------------------
// Run metadata helpers shared by the storage backends.
// ---------------------------------------------------------------------------

/**
 * Deep-equality predicate matching Postgres jsonb `@>` containment for the
 * `listWorkflows({ metadata })` filter. Returns true when, for every key/
 * value pair in `filter`, `actual?.[key]` deep-equals the filter value.
 * Missing or undefined `actual` matches an empty filter only.
 */
export function workflowMetadataMatches(
  actual: Record<string, unknown> | undefined | null,
  filter: Record<string, unknown>,
): boolean {
  const keys = Object.keys(filter);
  if (keys.length === 0) return true;
  if (!actual) return false;
  for (const k of keys) {
    if (!deepEqual(actual[k], filter[k])) return false;
  }
  return true;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return false;
  if (typeof a !== "object") return false;
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false;
    }
    return true;
  }
  if (Array.isArray(b)) return false;
  const ar = a as Record<string, unknown>;
  const br = b as Record<string, unknown>;
  const ak = Object.keys(ar);
  const bk = Object.keys(br);
  if (ak.length !== bk.length) return false;
  for (const k of ak) {
    if (!deepEqual(ar[k], br[k])) return false;
  }
  return true;
}

/**
 * `setWorkflowMetadata` semantics: a shallow merge of `patch` over
 * `current` in which a `null` value removes the key. Returns a new object;
 * neither argument is changed.
 */
export function applyMetadataPatch(params: {
  readonly current: Record<string, unknown> | null | undefined;
  readonly patch: Record<string, unknown>;
}): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...params.current };
  for (const [k, v] of Object.entries(params.patch)) {
    if (v === null) delete merged[k];
    else merged[k] = v;
  }
  return merged;
}
