// ---------------------------------------------------------------------------
// sameStoredValue — equality of two JSON-shaped values as a store sees them.
//
// Idempotent writes compare what a repeated call asks to store with what
// the first call stored. Backends round-trip values through JSON (jsonb
// reorders object keys), so the comparison is on a canonical form: object
// keys sorted, `undefined` members dropped, Dates as ISO strings.
// ---------------------------------------------------------------------------

/**
 * True when an existing signal token is the one a repeated
 * `createSignalToken` call asks for.
 */
export function sameSignalTokenCreate(params: {
  readonly existing: {
    readonly workflowId: string;
    readonly signalName: string;
    readonly bearer: string;
  };
  readonly requested: {
    readonly workflowId: string;
    readonly signalName: string;
    readonly bearer: string;
  };
}): boolean {
  const { existing, requested } = params;
  return (
    existing.workflowId === requested.workflowId &&
    existing.signalName === requested.signalName &&
    existing.bearer === requested.bearer
  );
}

/** True when `requested` stores as the same JSON value as `stored`. */
export function sameStoredValue(params: { stored: unknown; requested: unknown }): boolean {
  return canonicalJson(params.stored) === canonicalJson(params.requested);
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonical(value)) ?? "undefined";
}

function canonical(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const member = (value as Record<string, unknown>)[key];
      if (member !== undefined) out[key] = canonical(member);
    }
    return out;
  }
  return value;
}
