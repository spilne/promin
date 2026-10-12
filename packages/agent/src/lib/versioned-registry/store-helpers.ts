// ---------------------------------------------------------------------------
// Helpers every `VersionedRegistry` backend shares: splitting a register
// input into options and body, content hashing, version labels, facet
// matching. Kept here so the in-memory, SQLite and Postgres stores decide
// identically.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import {
  RegistryInputError,
  VERSION_PATTERN,
  type FacetQuery,
  type RegisterOptions,
  type RegisterVersionInput,
  type RegistryKind,
  type Versioned,
} from "./types.ts";

/** A register input split into its options and its normalised, validated body. */
export interface PreparedRegister<TBody> {
  readonly options: RegisterOptions;
  readonly body: TBody;
  /** Hash of the body's canonical JSON — equal bodies hash equal. */
  readonly hash: string;
  readonly facets: ReadonlyArray<string>;
}

const OPTION_KEYS = [
  "id",
  "version",
  "promote",
  "expectedActive",
  "source",
  "trust",
  "enabled",
] as const;

/**
 * Split, normalise and validate a register input. Throws `RegistryInputError`
 * (or the kind's own validation error, e.g. `DagValidationError`).
 */
export function prepareRegister<TBody, TFilter, TInput>(params: {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  readonly input: RegisterVersionInput<TInput>;
}): PreparedRegister<TBody> {
  const { kind, input } = params;
  const record = input as Readonly<Record<string, unknown>>;
  const rest: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) {
    if (!(OPTION_KEYS as ReadonlyArray<string>).includes(key)) rest[key] = value;
  }
  const options: RegisterOptions = {
    id: input.id,
    ...(input.version !== undefined && { version: input.version }),
    ...(input.promote !== undefined && { promote: input.promote }),
    ...(input.expectedActive !== undefined && { expectedActive: input.expectedActive }),
    ...(input.source !== undefined && { source: input.source }),
    ...(input.trust !== undefined && { trust: input.trust }),
    ...(input.enabled !== undefined && { enabled: input.enabled }),
  };
  if (typeof options.id !== "string" || options.id.length === 0) {
    throw new RegistryInputError(`${kind.name}: id must be a non-empty string`);
  }
  if (options.version !== undefined) assertVersionLabel(options.version);
  // The rest is the kind's input body: every key except the options.
  const body = kind.prepare({ id: options.id, input: rest as TInput });
  return {
    options,
    body,
    hash: contentHash(body),
    facets: [...new Set(kind.facets({ id: options.id, body }))].sort(),
  };
}

/** Throw `RegistryInputError` unless `version` is a valid label. */
export function assertVersionLabel(version: string): void {
  if (!VERSION_PATTERN.test(version)) {
    throw new RegistryInputError(
      `version "${version}" must start with a letter or digit and contain only letters, digits, ".", "_" and "-" (max 64)`,
    );
  }
}

/** Throw `RegistryInputError` unless a `list` page size is a positive integer. */
export function assertListLimit(limit: number | undefined): void {
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RegistryInputError(`list limit must be a positive integer, got ${limit}`);
  }
}

/**
 * The version to activate after the active one is removed: `promote` when
 * given, else the most recent earlier active version still live, else the
 * latest live version, else none.
 */
export function nextActiveVersion(params: {
  readonly promote?: string;
  /** Earlier active versions, oldest first. */
  readonly history: ReadonlyArray<string>;
  /** Live versions in registration order. */
  readonly live: ReadonlyArray<string>;
}): string | null {
  if (params.promote !== undefined) return params.promote;
  for (let i = params.history.length - 1; i >= 0; i--) {
    if (params.live.includes(params.history[i]!)) return params.history[i]!;
  }
  return params.live.length > 0 ? params.live[params.live.length - 1]! : null;
}

/** Activation history after `next` replaces `previous` as the active version. */
export function pushHistory(params: {
  readonly history: ReadonlyArray<string>;
  readonly previous: string | null;
  readonly next: string;
}): string[] {
  const out = params.history.filter((v) => v !== params.next && v !== params.previous);
  if (params.previous !== null && params.previous !== params.next) out.push(params.previous);
  return out;
}

/** The label `register` mints for the `seq`-th version: `v<seq>`, skipping taken labels. */
export function autoVersionLabel(params: {
  readonly seq: number;
  readonly taken: (version: string) => boolean;
}): string {
  let n = params.seq;
  while (params.taken(`v${n}`)) n += 1;
  return `v${n}`;
}

/** sha256 over the canonical JSON of `value` (object keys sorted, `undefined` dropped). */
export function contentHash(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

/** JSON with object keys sorted, so equal values serialise equal. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) => {
    if (v === null || typeof v !== "object" || Array.isArray(v)) return v;
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(v).sort()) sorted[k] = (v as Record<string, unknown>)[k];
    return sorted;
  });
}

const VERSION_INFO_KEYS = new Set([
  "id",
  "version",
  "createdAt",
  "active",
  "enabled",
  "trust",
  "source",
]);

/** The stored body of an entry: the entry minus its `VersionInfo` fields. */
export function bodyOf<TBody>(entry: Versioned<TBody>): TBody {
  const body: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(entry as Readonly<Record<string, unknown>>)) {
    if (!VERSION_INFO_KEYS.has(key)) body[key] = value;
  }
  return body as TBody;
}

/** Whether `input` would store the same body `entry` holds. */
export function sameBody<TBody, TFilter, TInput>(params: {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  readonly entry: Versioned<TBody>;
  readonly input: RegisterVersionInput<TInput>;
}): boolean {
  return (
    prepareRegister({ kind: params.kind, input: params.input }).hash ===
    contentHash(bodyOf(params.entry))
  );
}

/** Whether an entry with `facets` matches `query`. */
export function matchesFacets(params: {
  readonly facets: ReadonlyArray<string>;
  readonly query: FacetQuery;
}): boolean {
  const { facets, query } = params;
  return (
    query.include.every((f) => facets.includes(f)) && !query.exclude.some((f) => facets.includes(f))
  );
}

/** The facet query of an optional filter (`undefined` matches everything). */
export function facetQuery<TBody, TFilter, TInput>(params: {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  readonly filter: TFilter | undefined;
}): FacetQuery {
  return params.filter === undefined
    ? { include: [], exclude: [] }
    : params.kind.query(params.filter);
}
